package gorocksdb

import (
	"github.com/dtynn/winston/pkg/storage"
	"github.com/tecbot/gorocksdb"
)

// Iterator iterator
type Iterator struct {
	s    *Storage
	snap *gorocksdb.Snapshot
	opts *gorocksdb.ReadOptions
	iter *gorocksdb.Iterator

	start []byte

	moved bool
	valid bool
}

// First move to first key
func (i *Iterator) First() {
	i.moved = true

	if i.start == nil {
		i.iter.SeekToFirst()
		i.valid = i.iter.Valid()
		return
	}

	i.Seek(i.start)
	i.valid = i.iter.Valid()
}

// Last move to the last key
func (i *Iterator) Last() {
	i.moved = true

	i.iter.SeekToLast()
	i.valid = i.iter.Valid() && storage.KeyInRange(i.key(), i.start, nil)
}

// Seek move to key greater than or equal seek
func (i *Iterator) Seek(seek []byte) {
	i.moved = true

	if !storage.KeyInRange(seek, i.start, nil) {
		seek = i.start
	}

	i.iter.Seek(seek)
	i.valid = i.iter.Valid()
}

// Next move to next key
func (i *Iterator) Next() bool {
	if !i.moved {
		i.First()
		return i.valid
	}

	i.iter.Next()
	i.valid = i.iter.Valid()
	return i.valid
}

// Prev move the the previous key
func (i *Iterator) Prev() bool {
	i.iter.Prev()
	i.valid = i.iter.Valid() && storage.KeyInRange(i.key(), i.start, nil)
	return i.valid
}

// Key return current key
func (i *Iterator) Key() []byte {
	if !i.valid {
		return nil
	}

	return i.key()
}

func (i *Iterator) key() []byte {
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
