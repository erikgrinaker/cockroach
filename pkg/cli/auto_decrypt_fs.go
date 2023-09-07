// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble/vfs"
)

// autoDecryptFS is a filesystem which automatically detects paths that are
// registered as encrypted stores and uses the appropriate encryptedFS.
type autoDecryptFS struct {
	encryptedDirs map[string]*encryptedDir
	resolveFn     resolveEncryptedDirFn
}

var _ vfs.FS = (*autoDecryptFS)(nil)

type encryptedDir struct {
	once       sync.Once
	fs         vfs.FS
	resolveErr error
}

type resolveEncryptedDirFn func(dir string) (vfs.FS, error)

// Init sets up the given paths as encrypted and provides a callback that can
// resolve an encrypted directory path into an FS.
//
// Any FS operations inside encrypted paths will use the corresponding resolved FS.
//
// resolveFN is lazily called at most once for each encrypted dir.
func (afs *autoDecryptFS) Init(encryptedDirs []string, resolveFn resolveEncryptedDirFn) {
	afs.resolveFn = resolveFn

	afs.encryptedDirs = make(map[string]*encryptedDir)
	for _, dir := range encryptedDirs {
		if absDir, err := filepath.Abs(dir); err == nil {
			dir = absDir
		}
		afs.encryptedDirs[dir] = &encryptedDir{}
	}
}

func (afs *autoDecryptFS) Create(name string) (vfs.File, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return nil, err
	}
	return fs.Create(name)
}

func (afs *autoDecryptFS) Link(oldname, newname string) error {
	oldname, err := filepath.Abs(oldname)
	if err != nil {
		return err
	}
	newname, err = filepath.Abs(newname)
	if err != nil {
		return err
	}
	fs, err := afs.maybeSwitchFS(oldname)
	if err != nil {
		return err
	}
	return fs.Link(oldname, newname)
}

func (afs *autoDecryptFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return nil, err
	}
	return fs.Open(name, opts...)
}

func (afs *autoDecryptFS) OpenDir(name string) (vfs.File, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return nil, err
	}
	return fs.OpenDir(name)
}

func (afs *autoDecryptFS) Remove(name string) error {
	name, err := filepath.Abs(name)
	if err != nil {
		return err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return err
	}
	return fs.Remove(name)
}

func (afs *autoDecryptFS) RemoveAll(name string) error {
	name, err := filepath.Abs(name)
	if err != nil {
		return err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return err
	}
	return fs.RemoveAll(name)
}

func (afs *autoDecryptFS) Rename(oldname, newname string) error {
	fs, err := afs.maybeSwitchFS(oldname)
	if err != nil {
		return err
	}
	return fs.Rename(oldname, newname)
}

func (afs *autoDecryptFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	oldname, err := filepath.Abs(oldname)
	if err != nil {
		return nil, err
	}
	newname, err = filepath.Abs(newname)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(oldname)
	if err != nil {
		return nil, err
	}
	return fs.ReuseForWrite(oldname, newname)
}

func (afs *autoDecryptFS) MkdirAll(dir string, perm os.FileMode) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	fs, err := afs.maybeSwitchFS(dir)
	if err != nil {
		return err
	}
	return fs.MkdirAll(dir, perm)
}

func (afs *autoDecryptFS) Lock(name string) (io.Closer, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return nil, err
	}
	return fs.Lock(name)
}

func (afs *autoDecryptFS) List(dir string) ([]string, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(dir)
	if err != nil {
		return nil, err
	}
	return fs.List(dir)
}

func (afs *autoDecryptFS) Stat(name string) (os.FileInfo, error) {
	name, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	fs, err := afs.maybeSwitchFS(name)
	if err != nil {
		return nil, err
	}
	return fs.Stat(name)
}

func (afs *autoDecryptFS) PathBase(path string) string {
	return filepath.Base(path)
}

func (afs *autoDecryptFS) PathJoin(elem ...string) string {
	return filepath.Join(elem...)
}

func (afs *autoDecryptFS) PathDir(path string) string {
	return filepath.Dir(path)
}

func (afs *autoDecryptFS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return vfs.DiskUsage{}, err
	}
	fs, err := afs.maybeSwitchFS(path)
	if err != nil {
		return vfs.DiskUsage{}, err
	}
	return fs.GetDiskUsage(path)
}

// maybeSwitchFS finds the first ancestor of path that is registered as an
// encrypted FS; if there is such a path, returns the decrypted FS for that
// path. Otherwise, returns the default FS.
//
// Assumes that path is absolute and clean (i.e. filepath.Abs was run on it);
// the returned FS also assumes that any paths used are absolute and clean. This
// is needed when using encryptedFS, since the PebbleFileRegistry used in that
// context attempts to convert function input paths to relative paths using the
// DBDir. Both the DBDir and function input paths in a CockroachDB node are
// absolute paths, but when using the Pebble tool, the function input paths are
// based on what the cli user passed to the pebble command. We do not wish for a
// user using the cli to remember to pass an absolute path to the various pebble
// tool commands that accept paths. Note that the pebble tool commands taking a
// path parameter are quite varied: ranging from "pebble db" to "pebble lsm", so
// it is simplest to intercept the function input paths here.
func (afs *autoDecryptFS) maybeSwitchFS(path string) (vfs.FS, error) {
	for {
		if e := afs.encryptedDirs[path]; e != nil {
			e.once.Do(func() {
				e.fs, e.resolveErr = afs.resolveFn(path)
			})
			return e.fs, e.resolveErr
		}
		parent := filepath.Dir(path)
		if path == parent {
			break
		}
		path = parent
	}
	return vfs.Default, nil
}
