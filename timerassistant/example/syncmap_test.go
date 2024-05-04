package example

import (
	"sync"
	"testing"
)

func TestMM(t *testing.T) {
	var m sync.Map
	m.Store(1, 1)
	m.Store(2, 2)
	m.Store(3, 3)
	m.Store(4, 4)
	m.Range(func(key, value any) bool {
		return true
	})
}
