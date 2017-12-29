package gorocksdb

import (
	"sync"

	"github.com/dtynn/winston/pkg/storage"
	"github.com/tecbot/gorocksdb"
)

// Batch batch operations
type Batch struct {
	s     *Storage
	opts  *gorocksdb.WriteOptions
	batch *gorocksdb.WriteBatch

	closed bool
	sync.Mutex
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
	b.Lock()
	defer b.Unlock()

	if b.closed {
		return storage.ErrBatchClosed
	}

	defer b.close()

	return b.s.db.Write(b.opts, b.batch)
}

// Close close the batch
func (b *Batch) Close() error {
	b.Lock()
	defer b.Unlock()

	if b.closed {
		return storage.ErrBatchClosed
	}

	b.close()

	return nil
}

func (b *Batch) close() {
	b.closed = true

	b.batch.Clear()
	if b.opts != nil {
		b.opts.Destroy()
	}
}
