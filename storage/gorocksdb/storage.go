package gorocksdb

import (
	"os"
	"path/filepath"

	"github.com/dtynn/winston/pkg/storage"
	"github.com/tecbot/gorocksdb"
)

// Open return a gorocksdb storage
func Open(path string, opts ...Option) (*Storage, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	s := &Storage{
		dir:   dir,
		path:  path,
		dbopt: gorocksdb.NewDefaultOptions(),
	}

	s.dbopt.SetCompression(gorocksdb.NoCompression)
	s.dbopt.SetCreateIfMissing(true)
	s.dbopt.SetPurgeRedundantKvsWhileFlush(true)

	s.db, err = gorocksdb.OpenDb(s.dbopt, s.path)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// Storage storage implementation
type Storage struct {
	dir   string
	path  string
	dbopt *gorocksdb.Options
	db    *gorocksdb.DB
}

// Get return the value
func (s *Storage) Get(key []byte) ([]byte, error) {
	opts := gorocksdb.NewDefaultReadOptions()
	defer opts.Destroy()

	return s.db.GetBytes(opts, key)
}

// MGet return multiple values error
func (s *Storage) MGet(keys ...[]byte) ([][]byte, error) {
	snap := s.db.NewSnapshot()
	opts := gorocksdb.NewDefaultReadOptions()
	opts.SetSnapshot(snap)

	defer func() {
		s.db.ReleaseSnapshot(snap)
		opts.Destroy()
	}()

	vals := make([][]byte, len(keys))
	for i, key := range keys {
		b, err := s.db.GetBytes(opts, key)
		if err != nil {
			return nil, err
		}

		vals[i] = b
	}

	return vals, nil
}

// Put update a key
func (s *Storage) Put(key, val []byte) error {
	opts := gorocksdb.NewDefaultWriteOptions()

	defer opts.Destroy()

	return s.db.Put(opts, key, val)
}

// Del delete a key
func (s *Storage) Del(key []byte) error {
	opts := gorocksdb.NewDefaultWriteOptions()

	defer opts.Destroy()

	return s.db.Delete(opts, key)
}

// PrefixIterator return a iterator with prefix
func (s *Storage) PrefixIterator(prefix []byte) (storage.Iterator, error) {
	iter, err := s.iterator()
	if err != nil {
		return nil, err
	}

	if prefix != nil {
		return storage.PrefixIterator(prefix, iter), nil
	}

	return iter, nil
}

// RangeIterator return a iterator within the range
func (s *Storage) RangeIterator(start, end []byte) (storage.Iterator, error) {
	iter, err := s.iterator()
	if err != nil {
		return nil, err
	}

	if start != nil || end != nil {
		return storage.RangeIterator(start, end, iter), nil
	}

	return iter, nil
}

func (s *Storage) iterator() (*Iterator, error) {
	snap := s.db.NewSnapshot()
	opts := gorocksdb.NewDefaultReadOptions()
	opts.SetSnapshot(snap)
	iter := s.db.NewIterator(opts)

	return &Iterator{
		s:    s,
		snap: snap,
		opts: opts,
		iter: iter,
	}, nil
}

// Batch return a batch
func (s *Storage) Batch() (storage.Batch, error) {
	return &Batch{
		s:     s,
		opts:  gorocksdb.NewDefaultWriteOptions(),
		batch: gorocksdb.NewWriteBatch(),
	}, nil
}

// GC db.Compact
func (s *Storage) GC() error {
	s.db.CompactRange(gorocksdb.Range{})
	return nil
}

// Close close the storage
func (s *Storage) Close() error {
	s.db.Close()
	s.dbopt.Destroy()
	return nil
}

func (s *Storage) cleanup() error {
	return os.RemoveAll(s.dir)
}
