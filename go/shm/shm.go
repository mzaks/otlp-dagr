//go:build unix

// Package shm is the metrics region shared by an application and a collector on one host
// (Dagr spec/38 §7.1): a file in a directory both processes map, holding the generated
// `MetricsRegion` SharedBuffer under a seqlock. The application's exporter writes it
// (package shmexporter); the collector's `dagrshm` receiver reads it (package
// shmreceiver). Sidecar and DaemonSet deployments differ only in how the directory is
// shared — an `emptyDir{medium: Memory}` volume or a `hostPath` onto a tmpfs (D4).
//
// The region carries no fingerprint of its layout (Dagr spec/20 §8), so a reader checks
// Magic and Version before trusting a byte, and a layout change is a Version bump.
package shm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	mr "otlpgen/metricsregion"
)

// Magic and Version are `MetricsRegionMagic` / `MetricsRegionVersion` in schema.py.
const (
	Magic   = 0x524D4744 // "DGMR" little-endian
	Version = 1
)

var (
	// ErrBusy: every attempt found a write in progress, or a torn copy. A writer that died
	// mid-write leaves the region in this state for good.
	ErrBusy = errors.New("shm: region is being written")
	// ErrNotRegion: the magic does not match — not a metrics region, or not stamped yet.
	ErrNotRegion = errors.New("shm: not a metrics region")
	// ErrVersion: a metrics region of another layout version.
	ErrVersion = errors.New("shm: unsupported metrics region version")
)

// Region is one mapped region file.
type Region struct {
	f    *os.File
	mem  []byte
	lock mr.Seqlock
}

// Create makes the region file at path — replacing one a previous run left behind — and
// maps it. The file appears under its name only once it is sized and stamped (a rename),
// so a reader never maps a short or unstamped file under the final name.
func Create(path, service string) (*Region, error) {
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	r, err := mapFile(f)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return nil, err
	}
	w := r.lock.BeginWrite()
	w.SetMagic(Magic)
	w.SetVersion(Version)
	if len(service) > 64 {
		service = service[:64]
	}
	w.SetService(service)
	r.lock.EndWrite()
	if err := os.Rename(tmp, path); err != nil {
		r.Close()
		os.Remove(tmp)
		return nil, err
	}
	return r, nil
}

// Open maps an existing region file for reading. The mapping is writable because the
// seqlock's confirming read is a read-modify-write on the counter; nothing else is
// written.
func Open(path string) (*Region, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() < mr.ByteSize {
		f.Close()
		return nil, fmt.Errorf("%w: %s is %d bytes, a region is %d", ErrNotRegion, path, st.Size(), mr.ByteSize)
	}
	return mapFile(f)
}

func mapFile(f *os.File) (*Region, error) {
	if err := f.Truncate(mr.ByteSize); err != nil {
		return nil, err
	}
	mem, err := syscall.Mmap(int(f.Fd()), 0, mr.ByteSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &Region{f: f, mem: mem, lock: mr.SeqlockFromRaw(mem)}, nil
}

// File is the mapped file, for identity checks (a replaced file is a new inode).
func (r *Region) File() *os.File { return r.f }

// BeginWrite / EndWrite bracket one publication. There must be one writer: the
// application's exporter serialises its exports.
func (r *Region) BeginWrite() mr.State { return r.lock.BeginWrite() }
func (r *Region) EndWrite()             { r.lock.EndWrite() }

// Snapshot copies a consistent region into dst (at least mr.ByteSize bytes) and checks
// that it is a metrics region of this version. It gives up after attempts tries.
func (r *Region) Snapshot(dst []byte, attempts int) (mr.State, error) {
	s, ok := r.lock.TrySnapshot(dst, attempts)
	if !ok {
		return mr.State{}, ErrBusy
	}
	if s.Magic() != Magic {
		return mr.State{}, ErrNotRegion
	}
	if v := s.Version(); v != Version {
		return mr.State{}, fmt.Errorf("%w: %d (this reader: %d)", ErrVersion, v, Version)
	}
	return s, nil
}

// Close unmaps the region and closes the file. The file stays: the writer removes it
// (Remove) when it shuts down, a reader never does.
func (r *Region) Close() error {
	err := syscall.Munmap(r.mem)
	if cerr := r.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// Remove closes the region and deletes its file — a writer's shutdown.
func (r *Region) Remove() error {
	name := r.f.Name()
	err := r.Close()
	if rerr := os.Remove(finalName(name)); err == nil && !os.IsNotExist(rerr) {
		err = rerr
	}
	return err
}

// finalName undoes the temporary name Create opened the file under.
func finalName(tmp string) string {
	dir, base := filepath.Split(tmp)
	if len(base) > 5 && base[0] == '.' && filepath.Ext(base) == ".tmp" {
		return filepath.Join(dir, base[1:len(base)-4])
	}
	return tmp
}
