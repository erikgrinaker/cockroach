// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
)

var (
	CircuitBreakerEnabled = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"kv.dist_sender.circuit_breaker.enabled",
		"enable circuit breakers for failing or stalled replicas",
		false, // TODO(erikgrinaker): enable by default?
	)

	CircuitBreakerProbeThreshold = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"kv.dist_sender.circuit_breaker.probe.threshold",
		"duration of errors or stalls after which a replica will be probed",
		3*time.Second,
	)

	CircuitBreakerProbeInterval = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"kv.dist_sender.circuit_breaker.probe.interval",
		"interval between replica probes",
		3*time.Second,
	)

	CircuitBreakerProbeTimeout = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"kv.dist_sender.circuit_breaker.probe.timeout",
		"timeout for replica probes",
		3*time.Second,
	)

	CircuitBreakerCancelInflight = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"kv.dist_sender.circuit_breaker.cancel.enabled",
		"when enabled, in-flight requests will be cancelled when the circuit breaker trips",
		false, // TODO(erikgrinaker): enable by default?
	)
)

// cbGCThreshold is the threshold after which an idle replica's circuit breaker
// will be garbage collected.
const cbGCThreshold = 10 * time.Minute

// cbKey is a key in the DistSender replica circuit breakers map.
type cbKey struct {
	rangeID   roachpb.RangeID
	replicaID roachpb.ReplicaID
}

// DistSenderCircuitBreakers manages circuit breakers for replicas. Their
// primary purpose is to prevent the DistSender getting stuck on non-functional
// replicas. In particular, the DistSender relies on receiving a NLHE from the
// replica to update its range cache and try other replicas, otherwise it will
// keep sending requests to the same broken replica which will continue to get
// stuck, giving the appearance of an unavailable range. This can happen if:
//
//   - The replica stalls, e.g. with a disk stall or mutex deadlock.
//
//   - Clients time out before the replica lease acquisition attempt times out,
//     e.g. if the replica is partitioned away from the leader.
//
// Each replica has its own circuit breaker. The circuit breaker will probe the
// replica if:
//
// - It has only returned errors in the past probe threshold.
//   - Checked after each error.
//   - Send/network errors are ignored, and handled by RPC circuit breakers.
//   - NLHE with a known lease is not considered an error.
//   - Client timeouts and context cancellations count as errors. Consider e.g.
//     a stalled replica which continually causes client timeouts.
//
// - It has potentially stalled, with no responses in the past probeThreshold.
//   - Checked via an asynchronous loop.
//   - Any response from the replica resets the timer (even br.Error).
//   - Only if there are still in-flight requests.
//
// The probe sends a LeaseInfo request every probe interval, and expects either
// a successful response (if it is the leaseholder) or a NLHE (if it knows a
// leaseholder or leader exists elsewhere) before the probe timeout. Otherwise,
// it will trip the circuit breaker. In particular, this will fail if the
// replica is unable to acquire or detect a lease, e.g. because it is
// partitioned away from the leader.
//
// Stale circuit breakers are removed if they haven't seen any traffic for the
// past GC threshold (and aren't tripped).
//
// TODO(erikgrinaker): we can extend this to also manage range-level circuit
// breakers, but for now we focus exclusively on replica-level circuit breakers.
// This avoids the overhead of maintaining and accessing a multi-level
// structure.
//
// TODO(erikgrinaker): this needs comprehensive testing.
type DistSenderCircuitBreakers struct {
	stopper          *stop.Stopper
	settings         *cluster.Settings
	transportFactory TransportFactory
	metrics          DistSenderMetrics

	mu struct {
		syncutil.RWMutex
		replicas map[cbKey]*ReplicaCircuitBreaker
	}
}

// NewDistSenderCircuitBreakers creates new DistSender circuit breakers.
func NewDistSenderCircuitBreakers(
	stopper *stop.Stopper,
	settings *cluster.Settings,
	transportFactory TransportFactory,
	metrics DistSenderMetrics,
) *DistSenderCircuitBreakers {
	d := &DistSenderCircuitBreakers{
		stopper:          stopper,
		settings:         settings,
		transportFactory: transportFactory,
		metrics:          metrics,
	}
	d.mu.replicas = map[cbKey]*ReplicaCircuitBreaker{}
	return d
}

// Start starts the circuit breaker manager, and runs it until the stopper
// stops. It only returns an error if the server is already stopping.
func (d *DistSenderCircuitBreakers) Start() error {
	ctx := context.Background()
	return d.stopper.RunAsyncTask(ctx, "distsender-circuit-breakers", func(ctx context.Context) {
		// Reuse slices across scans.
		var cbs []*ReplicaCircuitBreaker

		// Periodically iterate over replica circuit breakers to check for stalled
		// replicas and garbage collect stale replicas.
		//
		// We use the probe interval as the scan interval, since we can sort of
		// consider this to be probing the replicas for a stall.
		timer := timeutil.NewTimer()
		defer timer.Stop()
		timer.Reset(CircuitBreakerProbeInterval.Get(&d.settings.SV))

		for {
			select {
			case <-timer.C:
				timer.Read = true
				// Eagerly reset the timer, to avoid skewing the interval.
				timer.Reset(CircuitBreakerProbeInterval.Get(&d.settings.SV))
			case <-d.stopper.ShouldQuiesce():
				return
			}

			// Don't do anything if circuit breakers have been disabled and they have
			// all been garbage collected.
			if !CircuitBreakerEnabled.Get(&d.settings.SV) {
				d.mu.RLock()
				l := len(d.mu.replicas) // nolint:deferunlockcheck
				d.mu.RUnlock()
				if l == 0 {
					continue
				}
			}

			// Copy the circuit breaker references to avoid holding a long-lived lock.
			cbs = cbs[:0]
			d.mu.RLock()
			for _, cb := range d.mu.replicas { // nolint:deferunlockcheck
				cb := cb              // pin loop variable
				cbs = append(cbs, cb) // nolint:deferunlockcheck
			}
			d.mu.RUnlock()

			nowNanos := timeutil.Now().UnixNano()
			probeThreshold := CircuitBreakerProbeThreshold.Get(&d.settings.SV).Nanoseconds()
			gcBelow := nowNanos - cbGCThreshold.Nanoseconds()

			// Probe the replica for a stall if we haven't seen a response from it in
			// the past probe threshold.
			for _, cb := range cbs {
				if s := cb.stallSince.Load(); s > 0 && nowNanos-s >= probeThreshold {
					cb.breaker.Probe()
				}
			}

			// Garbage collect stale replicas that haven't seen traffic for some time.
			//
			// We use this simple scheme both to avoid tracking replicas that aren't
			// being used, and also to clean up after replicas that no longer exist.
			// This is much simpler and less error-prone than eagerly removing them in
			// response to errors and synchronizing with range descriptor updates,
			// which would also risk significant churn to create and destroy circuit
			// breakers if the DistSender keeps sending requests to them for some
			// reason.
			var gced int
			for _, cb := range cbs {
				if lastRequest := cb.lastRequest.Load(); lastRequest > 0 && lastRequest < gcBelow {
					// NB: we don't want to remove a tripped circuit breaker. But this
					// also implicitly relies on the probe shutting down and untripping
					// the breaker if the replica no longer exists. There is a risk that
					// we'll "leak" probes if we don't properly detect this.
					if cb.inflightReqs.Load() == 0 && cb.Err() == nil {
						key := cbKey{rangeID: cb.rangeID, replicaID: cb.desc.ReplicaID}
						// Check under lock that the replica still exists in the map. They
						// should only be removed by this loop, but better safe than sorry.
						d.mu.Lock()
						_, ok := d.mu.replicas[key]
						delete(d.mu.replicas, key) // nolint:deferunlockcheck
						d.mu.Unlock()
						// Close the circuit breaker's closedC channel to abort any probes.
						// We use the map as a synchronization point above, so we know this
						// will only be closed once.
						if ok {
							close(cb.closedC)
						}
						// Avoid delaying the probe loop for too long, so yield after 100
						// replicas. We could also try to batch removals under lock, but
						// that could hold the lock for too long, or we could split this out
						// to a separate loop. For now, we do the simple thing.
						gced++
						if gced >= 100 {
							break
						}
					}
				}
			}
		}
	})
}

// ForReplica returns a circuit breaker for a given replica.
func (d *DistSenderCircuitBreakers) ForReplica(
	rangeDesc *roachpb.RangeDescriptor, replDesc *roachpb.ReplicaDescriptor,
) *ReplicaCircuitBreaker {
	// If circuit breakers are disabled, return a nil breaker.
	if !CircuitBreakerEnabled.Get(&d.settings.SV) {
		return nil
	}

	key := cbKey{rangeID: rangeDesc.RangeID, replicaID: replDesc.ReplicaID}

	// Fast path: use existing circuit breaker.
	d.mu.RLock()
	cb, ok := d.mu.replicas[key]
	d.mu.RUnlock()
	if ok {
		return cb
	}

	// Slow path: construct a new replica circuit breaker and insert it. We
	// construct it under lock to avoid a pileup in case lock acquisition is slow.
	cb = func() *ReplicaCircuitBreaker {
		d.mu.Lock()
		defer d.mu.Unlock()

		if c, ok := d.mu.replicas[key]; ok {
			return c // we raced with a concurrent insert
		}

		c := newReplicaCircuitBreaker(d, rangeDesc, replDesc)
		d.mu.replicas[key] = c
		return c
	}()

	return cb
}

// ReplicaCircuitBreaker is a circuit breaker for an individual replica.
type ReplicaCircuitBreaker struct {
	d        *DistSenderCircuitBreakers
	rangeID  roachpb.RangeID
	startKey roachpb.Key
	desc     roachpb.ReplicaDescriptor
	breaker  *circuit.Breaker

	// inflightReqs tracks the number of in-flight requests.
	inflightReqs atomic.Int32

	// errorSince is the timestamp when the current streak of errors began. Set on
	// an initial error, and cleared on successful responses.
	errorSince atomic.Int64

	// stallSince is the timestamp when the current potential stall began. Set on
	// the first in-flight request, moved forward on each response from the
	// replica (even errors), and cleared when there are no in-flight requests.
	stallSince atomic.Int64

	// lastRequest contains the last request timestamp, for garbage collection.
	lastRequest atomic.Int64

	mu struct {
		syncutil.Mutex

		// cancelFns contains context cancellation functions for all in-flight
		// requests. Only tracked if cancellation is enabled.
		cancelFns map[*kvpb.BatchRequest]func()
	}

	// closedC is closed when the replica circuit breaker is closed and removed
	// from DistSenderCircuitBreakers. It will cause any circuit breaker probes to
	// shut down.
	closedC chan struct{}
}

// newReplicaCircuitBreaker creates a new DistSender replica circuit breaker.
//
// TODO(erikgrinaker): consider pooling these.
func newReplicaCircuitBreaker(
	d *DistSenderCircuitBreakers,
	rangeDesc *roachpb.RangeDescriptor,
	replDesc *roachpb.ReplicaDescriptor,
) *ReplicaCircuitBreaker {
	r := &ReplicaCircuitBreaker{
		d:        d,
		rangeID:  rangeDesc.RangeID,
		startKey: rangeDesc.StartKey.AsRawKey(), // immutable
		desc:     *replDesc,
		closedC:  make(chan struct{}),
	}
	// We never update the replica descriptor, and we don't care about the type,
	// so set the type to VOTER_FULL which omits it in the string representation.
	r.desc.Type = roachpb.VOTER_FULL
	r.breaker = circuit.NewBreaker(circuit.Options{
		Name:       r.id(),
		AsyncProbe: r.launchProbe,
	})
	r.mu.cancelFns = map[*kvpb.BatchRequest]func(){}
	return r
}

// replicaCircuitBreakerToken carries request-scoped state between Track() and
// done().
type replicaCircuitBreakerToken struct {
	r      *ReplicaCircuitBreaker // nil if circuit breakers were disabled
	ctx    context.Context
	ba     *kvpb.BatchRequest
	cancel func()
}

// Done records the result of the request and untracks it.
func (t replicaCircuitBreakerToken) Done(br *kvpb.BatchResponse, err error, nowNanos int64) {
	t.r.done(t.ctx, t.ba, br, err, nowNanos, t.cancel)
}

// id returns a string identifier for the replica.
func (r *ReplicaCircuitBreaker) id() redact.RedactableString {
	return redact.Sprintf("r%d/%s", r.rangeID, r.desc)
}

// Err returns the circuit breaker error if it is tripped.
func (r *ReplicaCircuitBreaker) Err() error {
	if r == nil {
		return nil // circuit breakers disabled
	}
	// TODO(erikgrinaker): this is a bit more expensive than necessary, consider
	// optimizing it.
	return r.breaker.Signal().Err()
}

// Track attempts to start tracking a request with the circuit breaker. If the
// breaker is tripped, returns an error. Otherwise, returns the context to use
// for the send and a token which the caller must call Done() on with the result
// of the request.
func (r *ReplicaCircuitBreaker) Track(
	ctx context.Context, ba *kvpb.BatchRequest, nowNanos int64,
) (context.Context, replicaCircuitBreakerToken, error) {
	if r == nil {
		return ctx, replicaCircuitBreakerToken{}, nil // circuit breakers disabled
	}

	// Record the request timestamp.
	r.lastRequest.Store(nowNanos)

	// Check if the breaker is tripped.
	if err := r.Err(); err != nil {
		return nil, replicaCircuitBreakerToken{}, err
	}

	// Set up the request token.
	token := replicaCircuitBreakerToken{
		r:   r,
		ctx: ctx,
		ba:  ba,
	}

	// Record in-flight requests. If this is the only request, tentatively start
	// tracking a stall.
	if inflightReqs := r.inflightReqs.Add(1); inflightReqs == 1 {
		r.stallSince.Store(nowNanos)
	} else if inflightReqs < 0 {
		log.Fatalf(ctx, "inflightReqs %d < 0", inflightReqs) // overflow
	}

	// If enabled, create a send context that can be used to cancel in-flight
	// requests if the breaker trips.
	//
	// TODO(erikgrinaker): this needs optimization. We should try to make the map
	// lock-free. Alternatively, if we have to use a mutex, we can drop the
	// atomics and update everything under lock. WithCancel() also allocates.
	// Ideally, it should be possible to propagate cancellation of a single
	// replica-scoped context onto all request contexts, but this requires messing
	// with Go internals.
	sendCtx := ctx
	if CircuitBreakerCancelInflight.Get(&r.d.settings.SV) {
		sendCtx, token.cancel = context.WithCancel(ctx)
		r.mu.Lock()
		r.mu.cancelFns[ba] = token.cancel
		r.mu.Unlock()
	}

	return sendCtx, token, nil
}

// done records the result of a tracked request and untracks it. It is called
// via replicaCircuitBreakerToken.Done().
func (r *ReplicaCircuitBreaker) done(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	br *kvpb.BatchResponse,
	err error,
	nowNanos int64,
	cancel func(),
) {
	if r == nil {
		return // circuit breakers disabled when we began tracking the request
	}

	// Untrack the request.
	stallSince := r.stallSince.Load()
	if inflightReqs := r.inflightReqs.Add(-1); inflightReqs < 0 {
		log.Fatalf(ctx, "inflightReqs %d < 0", inflightReqs)
	} else if inflightReqs == 0 {
		// If this was the last inflight request, stop tracking a stall. If the
		// replica has in fact stalled but we keep hitting client timeouts, then the
		// error probes will deal with it instead.
		//
		// We have to be careful not to clobber a value written by a concurrent
		// incoming request after we read inflightReqs == 0, so we use a stallSince
		// CAS and re-check inflightReqs on failure.
		for !r.stallSince.CompareAndSwap(stallSince, 0) && r.inflightReqs.Load() == 0 {
			stallSince = r.stallSince.Load()
		}
	} else if err == nil {
		// If we got a response from the replica (even a br.Error), the replica
		// isn't stalled. Bump the stall timestamp to the current response
		// timestamp, in case a concurrent request has stalled.
		//
		// We have to be careful not to set this to non-zero if we raced with the
		// last inflight request completing, so we use a stallSince CAS and re-check
		// inflightReqs on failure.
		for nowNanos > stallSince && !r.stallSince.CompareAndSwap(stallSince, nowNanos) &&
			r.inflightReqs.Load() > 0 {
			stallSince = r.stallSince.Load()
		}
	}

	// Clean up the cancel function, if cancellation is enabled.
	if cancel != nil {
		r.mu.Lock()
		delete(r.mu.cancelFns, ba) // nolint:deferunlockcheck
		r.mu.Unlock()
		cancel()
	}

	// If this was a local send error, i.e. err != nil, we rely on RPC circuit
	// breakers to fail fast. There is no need for us to launch a probe as well.
	// This includes the case where either the remote or local node has been
	// decommissioned.
	//
	// However, if the sender's context is cancelled, pessimistically assume this
	// is a timeout and fall through to the error handling below to potentially
	// launch a probe. Even though this may simply be the client going away, we
	// can't know if this was because of a client timeout or not, so we assume
	// there may be a problem with the replica. We will typically see recent
	// successful responses too if that isn't the case.
	if err != nil && ctx.Err() == nil {
		return
	}

	// Handle error responses. To record the response as an error, err is set
	// non-nil. Otherwise, the response is recorded as a success.
	if err == nil && br.Error != nil {
		switch tErr := br.Error.GetDetail().(type) {
		case *kvpb.NotLeaseHolderError:
			// Consider NLHE a success if it contains a lease record, as the replica
			// appears functional. If there is no lease record, the replica was unable
			// to acquire a lease and has no idea who the leaseholder might be, likely
			// because it is disconnected from the leader or there is no quorum.
			if tErr.Lease == nil || *tErr.Lease == (roachpb.Lease{}) {
				err = br.Error.GoError()
			}
		case *kvpb.RangeNotFoundError, *kvpb.RangeKeyMismatchError, *kvpb.StoreNotFoundError:
			// If the replica no longer exists, we don't need to probe. The DistSender
			// will stop using the replica soon enough.
		default:
			// Record all other errors.
			//
			// NB: this pessimistically assumes that any other error response may
			// indicate a replica problem. That's generally not true for most errors.
			// However, we will generally also see successful responses. If we only
			// see errors, it seems worthwhile to probe the replica and check, rather
			// than explicitly listing error types and possibly missing some. In the
			// worst case, this means launcing a goroutine and sending a cheap probe
			// every few seconds for each failing replica (which could be bad enough
			// across a large number of replicas).
			err = br.Error.GoError()
		}
	}

	// Track errors.
	errorSince := r.errorSince.Load()
	if err == nil {
		// On success, reset the error tracking.
		if errorSince > 0 {
			r.errorSince.CompareAndSwap(errorSince, 0)
		}
	} else if errorSince == 0 {
		// If this is the first error we've seen, record it. We'll launch a probe on
		// a later error if necessary.
		r.errorSince.CompareAndSwap(0, nowNanos)
	} else if nowNanos-errorSince >= CircuitBreakerProbeThreshold.Get(&r.d.settings.SV).Nanoseconds() {
		// The replica has been failing for the past probe threshold, probe it.
		r.breaker.Probe()
	}
}

// launchProbe spawns an async replica probe that periodically sends LeaseInfo
// requests to the replica.
//
// TODO(erikgrinaker): instead of spawning a goroutine for each replica, we
// should use a shared worker pool, and batch LeaseInfo requests for many/all
// replicas on the same node or store.
func (r *ReplicaCircuitBreaker) launchProbe(report func(error), done func()) {
	// TODO(erikgrinaker): use an annotated context here and elsewhere.
	ctx := context.Background()
	log.Eventf(ctx, "launching probe for %s", r.id())

	name := fmt.Sprintf("distsender-replica-probe-%s", r.id())
	err := r.d.stopper.RunAsyncTask(ctx, name, func(ctx context.Context) {
		defer func() {
			// Always reset the breaker when returning, to avoid launching another
			// probe immediately. We use a stallSince CAS to avoid clobbering any
			// in-flight requests that may have stalled, such that we'll launch
			// another probe once the threshold is reached again.
			nowNanos := timeutil.Now().UnixNano()
			for {
				stallSince := r.stallSince.Load()
				if stallSince == 0 || nowNanos < stallSince {
					break
				}
				stallSinceNew := nowNanos
				if r.inflightReqs.Load() == 0 {
					stallSinceNew = 0
				}
				if r.stallSince.CompareAndSwap(stallSince, stallSinceNew) {
					break
				}
			}
			r.errorSince.Store(0)
			report(nil)
			done()
		}()

		ctx, cancel := r.d.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		// Prepare the probe transport, using SystemClass to avoid RPC latency.
		//
		// We construct a bare replica slice without any locality information, since
		// we're only going to contact this replica.
		replicas := ReplicaSlice{{ReplicaDescriptor: r.desc}}
		opts := SendOptions{
			class:                  rpc.SystemClass,
			metrics:                &r.d.metrics,
			dontConsiderConnHealth: true,
		}
		transport, err := r.d.transportFactory(opts, replicas)
		if err != nil {
			log.Errorf(ctx, "failed to launch probe: %s", err)
			return
		}
		defer transport.Release()

		// Continually probe the replica until it succeeds. We probe immediately
		// since we only trip the breaker on probe failure.
		timer := timeutil.NewTimer()
		defer timer.Stop()
		for {
			// Bail out if circuit breakers are disabled.
			if !CircuitBreakerEnabled.Get(&r.d.settings.SV) {
				return
			}

			// Probe the replica.
			if err := r.sendProbe(ctx, transport); err != nil {
				report(err)
			} else {
				return // defer will untrip breaker
			}

			// Cancel in-flight requests on failure. We do this on every failure, and
			// also remove the cancel functions from the map (even though done() will
			// also clean them up), in case another request makes it in after the
			// breaker trips. There should typically never be any contention here.
			func() {
				r.mu.Lock()
				defer r.mu.Unlock()
				for ba, cancel := range r.mu.cancelFns {
					delete(r.mu.cancelFns, ba)
					cancel()
				}
			}()

			timer.Reset(CircuitBreakerProbeInterval.Get(&r.d.settings.SV))
			select {
			case <-timer.C:
				timer.Read = true
			case <-r.d.stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			case <-r.closedC:
				return
			}
		}
	})
	if err != nil {
		done()
	}
}

// sendProbe probes the replica by sending a LeaseInfo request. It returns an
// error if the circuit breaker should trip, or nil if it should untrip and
// stop probing.
//
// Note that this may return nil even though the request itself fails. The
// typical example is a NLHE, which indicates that the replica is functional but
// not the leaseholder, but there are other cases too. See below.
//
// We use a LeaseInfo request as a makeshift health check because:
//
//   - It is cheap (only reads in-memory state).
//   - It does not take out any latches.
//   - It requires a lease, so it will either attempt to acquire a lease or
//     return NLHE if it knows about a potential leaseholder elsewhere. This is
//     important, because if the replica is not connected to a quorum it will wait
//     for lease acquisition, and clients with low timeouts may cancel their
//     requests before a NLHE is returned, causing the DistSender to get stuck on
//     these replicas.
func (r *ReplicaCircuitBreaker) sendProbe(ctx context.Context, transport Transport) error {
	// We don't use timeutil.RunWithTimeout() because we need to be able to
	// differentiate which context failed.
	timeout := CircuitBreakerProbeTimeout.Get(&r.d.settings.SV)
	sendCtx, cancel := context.WithTimeout(ctx, timeout) // nolint:context
	defer cancel()

	ba := &kvpb.BatchRequest{}
	ba.RangeID = r.rangeID
	ba.Replica = transport.NextReplica()
	ba.Add(&kvpb.LeaseInfoRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: r.startKey,
		},
	})

	transport.Reset()

	log.VEventf(ctx, 2, "sending probe: %s", ba)
	br, err := transport.SendNext(sendCtx, ba)
	log.VEventf(ctx, 2, "probe result: br=%v err=%v", br, err)

	// Handle local send errors.
	if err != nil {
		// If the given context was cancelled, we're shutting down. Stop probing.
		if ctx.Err() != nil {
			return nil
		}

		// If the send context timed out, fail.
		if ctxErr := sendCtx.Err(); ctxErr != nil {
			return ctxErr
		}

		// Any other local error is likely a networking/gRPC issue. This includes if
		// either the remote node or the local node has been decommissioned. We
		// rely on RPC circuit breakers to fail fast for these, so there's no point
		// in us probing individual replicas. Stop probing.
		return nil // nolint:returnerrcheck
	}

	// Handle error responses.
	//
	// TODO(erikgrinaker): handle ReplicaUnavailableError here and in done() once
	// it implements ErrorDetailInterface.
	if br.Error != nil {
		switch tErr := br.Error.GetDetail().(type) {
		case *kvpb.NotLeaseHolderError:
			// If we get a NLHE back with a lease record, the replica is healthy
			// enough to know who the leaseholder is. Otherwise, we have to trip the
			// breaker such that the DistSender will try other replicas and discover
			// the leaseholder -- this may otherwise never happen if clients time out
			// before the replica returns the NLHE.
			if tErr.Lease == nil || *tErr.Lease == (roachpb.Lease{}) {
				return br.Error.GoError()
			}
		case *kvpb.RangeNotFoundError, *kvpb.RangeKeyMismatchError, *kvpb.StoreNotFoundError:
			// If the replica no longer exists, stop probing.
		default:
			// On any other error, trip the breaker.
			return br.Error.GoError()
		}
	}

	// Successful probe.
	return nil
}
