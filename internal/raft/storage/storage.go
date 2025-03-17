package storage

import (
	"encoding/gob"
	"fmt"
	_ "unsafe"
)

type EntryNotFoundError struct {
	key int
}

func (err EntryNotFoundError) Error() string {
	return fmt.Sprintf("entry with key %d hasn't been found", err.key)
}

//go:linkname throw runtime.throw
func throw(message string)

var encoder *gob.Encoder

type Storage interface {
	Get(id int) any
	Set(id int, data any)
}
