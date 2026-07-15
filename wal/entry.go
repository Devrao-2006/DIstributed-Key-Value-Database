package wal

import (
	"bytes"
	"encoding/binary"
)

type Operation int8

const (
	PutOperation    Operation = 1
	DeleteOperation Operation = 2
)

type Entry struct {
	Operation Operation
	Key       string
	Value     []byte
}

func (e Entry) Serialize() []byte {
	var buffer bytes.Buffer

	op := e.Operation
	keylen := len(e.Key)
	valuelen := len(e.Value)

	header := make([]byte, 1+4+4)
	header[0] = byte(op)
	binary.BigEndian.PutUint32(header[1:5], uint32(keylen))
	binary.BigEndian.PutUint32(header[5:], uint32(valuelen))

	buffer.Write(header)
	buffer.WriteString(e.Key)
	buffer.Write(e.Value)
	return buffer.Bytes()
}
