package gorocksdb

import (
	"testing"

	"github.com/dtynn/winston/pkg/storage/test"
)

func TestGoRockdbIterator(t *testing.T) {
	s := setupTestStorage(t)
	defer teardownTestStorage(s)

	test.Iterator(t, s)
}
