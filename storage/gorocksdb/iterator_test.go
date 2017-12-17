package gorocksdb

import (
	"testing"

	"github.com/dtynn/winston/storage/test"
)

func TestGoRockdbIterator(t *testing.T) {
	s := setupTestStorage(t)
	defer teardownTestStorage(s)

	test.Iterator(t, s)
}
