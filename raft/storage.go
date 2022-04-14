package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
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

type DefaultStorage struct {
	Storage
	mu     sync.Mutex
	m      map[int][]byte
	writer *bytes.Buffer
	reader *bytes.Reader
}

func NewDefaultStorage() *DefaultStorage {
	ds := new(DefaultStorage)
	ds.m = make(map[int][]byte)
	var writerBs []byte
	ds.writer = bytes.NewBuffer(writerBs)
	encoder = gob.NewEncoder(ds.writer)
	return ds
}

func (ds *DefaultStorage) Set(clientId int, command any) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if encoder == nil {
		throw("call NewDefaultStorage first")
	}
	var writerBs []byte
	buffer := bytes.NewBuffer(writerBs)
	if err := gob.NewEncoder(buffer).Encode(command); err != nil {
		return err
	}
	ds.m[clientId] = buffer.Bytes()
	return nil
}

func (ds *DefaultStorage) Get(clientId int) (any, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	var ok bool
	var bs []byte
	if bs, ok = ds.m[clientId]; !ok {
		return nil, &EntryNotFoundError{key: clientId}
	}

	var item string
	if err := gob.NewDecoder(bytes.NewReader(bs)).Decode(&item); err != nil {
		return nil, err
	}
	return item, nil
}
