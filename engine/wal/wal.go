package wal

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

type WAL struct {
	lock sync.RWMutex
	path string
	file *os.File
}

func NewWalEngine() *WAL {
	return &WAL{}
}

func (w *WAL) Open(path string) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	Opened_file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	w.file = Opened_file
	w.path = path
	return nil
}

func (w *WAL) Close() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.file.Close()
}

func (w *WAL) Reset() error {
    w.lock.Lock()
    defer w.lock.Unlock()
    if err := w.file.Truncate(0); err != nil {
        return err
    }
    _, err := w.file.Seek(0, 0)
    return err
}


func (w *WAL) Write(entry Entry) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	_, err := w.file.Write(entry.Serialize())
	if err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) ReadAll(fn func(Entry) error) error {
	w.lock.RLock()
	defer w.lock.RUnlock()
	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	for {
		header := make([]byte, 1+4+4)
		_, err := io.ReadFull(w.file, header)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		op := Operation(header[0])
		keyLen := binary.BigEndian.Uint32(header[1:5])
		valueLen := binary.BigEndian.Uint32(header[5:9])

		payload := make([]byte, keyLen+valueLen)

		_, err = io.ReadFull(w.file, payload)

		key := string(payload[0:keyLen])
		value := payload[keyLen : keyLen+valueLen]

		entry := Entry{
			Operation: op,
			Key:       key,
			Value:     value,
		}

		if err = fn(entry); err != nil {
			return err
		}
	}
}
