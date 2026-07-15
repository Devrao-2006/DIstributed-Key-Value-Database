package sstable

import (
	"DDB/bloom"
	"DDB/memtable"
	"DDB/wal"
	"encoding/binary"
	"io"
	"os"
)

var sstable_count int = 0

func Flush(path string, kvs []memtable.KV) (*bloom.Filter, error) {
	bf := bloom.NewFilter(len(kvs))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return bf, err
	}
	defer file.Close()

	for _, kv := range kvs {
		bf.Add(kv.Key)
		op := wal.PutOperation
		if kv.Tombstone {
			op = wal.DeleteOperation
		}

		entry := wal.Entry{
			Operation: op,
			Key:       kv.Key,
			Value:     []byte(kv.Value),
		}

		_, err := file.Write(entry.Serialize())
		if err != nil {
			return bf, err
		}
	}

	return bf, file.Sync()
}

func Search(path, Key string) (string, bool, bool, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return "", false, false, err
	}
	defer file.Close()
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return "", false, false, err
	}

	for {
		header := make([]byte, 1+4+4)
		_, err := io.ReadFull(file, header)
		if err == io.EOF {
			return "", false, false, nil
		}
		if err != nil {
			return "", false, false, err
		}

		op := wal.Operation(header[0])
		keyLen := binary.BigEndian.Uint32(header[1:5])
		valueLen := binary.BigEndian.Uint32(header[5:9])

		payload := make([]byte, keyLen+valueLen)

		_, err = io.ReadFull(file, payload)

		key := string(payload[0:keyLen])
		value := payload[keyLen : keyLen+valueLen]

		if key > Key {
			return "", false, false, nil
		}

		if key == Key {
			if op == wal.DeleteOperation {
				return "", true, true, nil
			}
			return string(value), true, false, nil
		}
	}
}

func ReadAll(path string, fn func(key, value string, isTombStone bool)) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return
	}
	defer file.Close()
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	for {
		header := make([]byte, 1+4+4)
		_, err := io.ReadFull(file, header)
		if err == io.EOF {
			return
		}
		if err != nil {
			return
		}

		op := wal.Operation(header[0])
		keyLen := binary.BigEndian.Uint32(header[1:5])
		valueLen := binary.BigEndian.Uint32(header[5:9])

		payload := make([]byte, keyLen+valueLen)

		_, err = io.ReadFull(file, payload)

		key := string(payload[0:keyLen])
		value := payload[keyLen : keyLen+valueLen]

		if op == wal.DeleteOperation {
			fn(key, "", true)
		} else {
			fn(key, string(value), false)
		}
	}
}
