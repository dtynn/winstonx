package gorocksdb

import (
	"github.com/tecbot/gorocksdb"
)

// Iterator iterator
type Iterator struct {
	s    *Storage
	snap *gorocksdb.Snapshot
	opts *gorocksdb.ReadOptions
	iter *gorocksdb.Iterator

	moved bool
	valid bool
}

// First move to first key
func (i *Iterator) First() {
	i.moved = true
	i.iter.SeekToFirst()
	i.valid = i.iter.Valid()
}

// Seek move to key greater than or equal seek
func (i *Iterator) Seek(seek []byte) {
	i.moved = true
	i.iter.Seek(seek)
	i.valid = i.iter.Valid()
}

// Next move to next key
func (i *Iterator) Next() bool {
	if !i.moved {
		i.First()
	} else {
		i.iter.Next()
	}

	i.valid = i.iter.Valid()
	return i.valid
}

// Key return current key
func (i *Iterator) Key() []byte {
	if !i.valid {
		return nil
	}

	s := i.iter.Key()
	if s == nil {
		return nil
	}

	b := s.Data()
	s.Free()
	return b
}

// Value return current value
func (i *Iterator) Value() []byte {
	if !i.valid {
		return nil
	}

	s := i.iter.Value()
	if s == nil {
		return nil
	}

	b := s.Data()
	s.Free()
	return b
}

// Valid return current valid
func (i *Iterator) Valid() bool {
	return i.valid
}

// UpdateValid update valid
func (i *Iterator) UpdateValid(valid bool) {
	i.valid = valid
}

// Err return iter error
func (i *Iterator) Err() error {
	return i.iter.Err()
}

// Close close the iterator
func (i *Iterator) Close() error {
	i.iter.Close()
	i.s.db.ReleaseSnapshot(i.snap)
	if i.opts != nil {
		i.opts.Destroy()
	}

	return nil
}
