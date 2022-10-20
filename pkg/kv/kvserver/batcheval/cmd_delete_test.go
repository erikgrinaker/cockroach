// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

// TestDeleteFoundKeyWriteTooOld tests that a Delete request evaluated below an
// existing key is correctly retried above it, pushing both the read and write
// timestamps of the transaction and returning FoundKey=true.
func TestDeleteFoundKeyWriteTooOld(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tr := tracing.NewTracer()
	recCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "test")
	defer getRecAndFinish()

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{Tracer: tr})
	defer s.Stopper().Stop(ctx)

	// Txn writes a, which fixes the txn read/write timestamps.
	txn := db.NewTxn(ctx, "test")
	require.NoError(t, txn.Put(ctx, "a", "a"))

	// Someone else writes b and c at a later timestamp.
	require.NoError(t, db.Put(ctx, "b", "b"))
	require.NoError(t, db.Put(ctx, "c", "c"))

	// Txn deletes c. The txn write timestamp is pushed to after the c write,
	// returning FoundKey=true. If DeleteRequest did not have the isRead flag,
	// this would not push the read timestamp, in which case the "c" read below
	// would return nil (at the old read timestamp) rather than "c" (at the new
	// timestamp). This would be an inconsistent read, because FoundKey=true was
	// evaluated above b's timestamp, but Get(c) was evaluated below it.
	var ba roachpb.BatchRequest
	ba.Add(&roachpb.DeleteRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: roachpb.Key("c"),
		},
	})
	br, pErr := txn.Send(recCtx, ba)
	require.NoError(t, pErr.GoError())
	require.True(t, br.Responses[0].GetDelete().FoundKey)

	// Txn reads b. To be consistent with FoundKey=true, this must return "b"
	// rather than nil. In other words, the Delete must have pushed not only the
	// write timestamp but also the read timestamp to after the b,c writes.
	// This requires the isRead flag to be set on DeleteRequest.
	kv, err := txn.Get(recCtx, "b")
	require.NoError(t, err)
	require.NotNil(t, kv.Value)
	v, err := kv.Value.GetBytes()
	require.NoError(t, err)
	require.Equal(t, "b", string(v))

	// Commit the txn.
	require.NoError(t, txn.Commit(ctx))

	fmt.Println(getRecAndFinish())
}
