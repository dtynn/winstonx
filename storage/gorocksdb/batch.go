package gorocksdb

import (
	"github.com/tecbot/gorocksdb"
)

// Batch batch operations
type Batch struct {
	s     *Storage
	opts  *gorocksdb.WriteOptions
	batch *gorocksdb.WriteBatch
}

// Put update a key
func (b *Batch) Put(key, val []byte) error {
	b.batch.Put(key, val)
	return nil
}

// Del del a key
func (b *Batch) Del(key []byte) error {
	b.batch.Delete(key)
	return nil
}

// Commit commit the batch operations
func (b *Batch) Commit() error {
	return b.s.db.Write(b.opts, b.batch)
}

// Close close the batch
func (b *Batch) Close() error {
	b.batch.Clear()
	if b.opts != nil {
		b.opts.Destroy()
	}
	return nil
}
