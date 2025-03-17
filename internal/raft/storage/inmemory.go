package storage

import (
	"bytes"
	"encoding/gob"
	"sync"
)

type InMemory struct {
	mu     sync.RWMutex
	m      map[int][]byte
	writer *bytes.Buffer
	reader *bytes.Reader
}

func NewInMemory() *InMemory {
	ds := new(InMemory)
	ds.m = make(map[int][]byte)
	var writerBs []byte
	ds.writer = bytes.NewBuffer(writerBs)
	encoder = gob.NewEncoder(ds.writer)
	return ds
}

func (ds *InMemory) Set(clientId int, command any) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if encoder == nil {
		throw("call NewInMemory first")
	}
	var writerBs []byte
	buffer := bytes.NewBuffer(writerBs)
	if err := gob.NewEncoder(buffer).Encode(command); err != nil {
		return err
	}
	ds.m[clientId] = buffer.Bytes()
	return nil
}

func (ds *InMemory) Get(clientId int) (any, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

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
