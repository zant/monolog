package server

import (
	"fmt"
	"sync"
)

type Log struct {
	mu      sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")

func (c *Log) Read(offset uint64) (Record, error) {
	// clasic mutex porque por la pinta quiere que el log solo lo toque una people a la vez
	// me imagino que es porque el offset y eso todo es muy sincrono, no queres que
	// hayan race conditions aca
	c.mu.Lock()
	defer c.mu.Unlock()
	// le pone onda con el error handling, en vez de panic o algo, ve si esta
	// dentro del array, buena onda me parece
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}
