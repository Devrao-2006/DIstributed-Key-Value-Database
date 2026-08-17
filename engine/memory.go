package engine

import (
	"DDB/engine/bloom"
	"DDB/engine/memtable"
	"DDB/engine/sstable"
	"DDB/engine/wal"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
)

var (
	ErrKeyNotFound     = errors.New("Key Not Found")
	ErrKeyAlreadyExist = errors.New("Key Already exist")
	CurroptedOperation = errors.New("Invalid Operation, File Curropted")
	ErrWALWrite        = errors.New("Failed to Write in WAL")
	ErrKeyDeleted      = errors.New("Key Deleted")
)

type ssTableMetadata struct {
	Path   string
	Filter *bloom.Filter
}

type MemoryEngine struct {
	lock          sync.RWMutex
	wal           *wal.WAL
	avl           *memtable.AVLTree
	sstable_count int
	ssTables      []ssTableMetadata
	dirPath       string
}

func NewMemoryEngine(w *wal.WAL, avl *memtable.AVLTree, path string) *MemoryEngine {
	return &MemoryEngine{
		wal:           w,
		avl:           avl,
		sstable_count: 0,
		dirPath: path,
	}
}

func (m *MemoryEngine) flushToDisk() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	kvs := m.avl.GetAll()

	path := m.dirPath + "/data_" + fmt.Sprintf("%d", m.sstable_count + 1) + ".sst"

	m.sstable_count++
	bf, err := sstable.Flush(path, kvs)
	if err != nil {
		return err
	}
	m.ssTables = append(m.ssTables, ssTableMetadata{
		Path:   path,
		Filter: bf,
	})
	m.avl = memtable.NewAVLTree()
	m.wal.Reset()
	return nil
}

func (m *MemoryEngine) Put(key, value string) error {
	err := m.wal.Write(wal.Entry{
		Operation: wal.PutOperation,
		Key:       key,
		Value:     []byte(value),
	})

	if err != nil {
		return ErrWALWrite
	}
	m.avl.Insert(key, value)
	if m.avl.GetCount() >= 10 {
		m.flushToDisk()
	}
	return nil
}

func (m *MemoryEngine) Get(key string) (string, error) {
	val, found, isTombStone := m.avl.Get(key)
	if found {
		if isTombStone {
			return "", ErrKeyNotFound
		}
		return val, nil
	} else {
		for _, v := range slices.Backward(m.ssTables) {
			if !v.Filter.MightContain(key) {
				continue
			}
			value, found, isTombStone, err := sstable.Search(v.Path, key)
			if err != nil {
				return "", err
			}
			if found {
				if isTombStone {
					return "", ErrKeyNotFound
				}
				return value, nil
			}
		}
	}
	return "", ErrKeyNotFound
}

func (m *MemoryEngine) Compact() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.avl.GetCount() > 0 {
		m.flushToDisk()
	}

	if len(m.ssTables) == 0 {
		return os.WriteFile(m.dirPath+"/data_compact.sst", []byte(""), 0644)
	}

	compactor := memtable.NewAVLTree()

	for _, v := range m.ssTables {
		sstable.ReadAll(v.Path, func(key string, value string, isTombStone bool) {
			if isTombStone == true {
				compactor.Delete(key)
			} else {
				compactor.Insert(key, value)
			}
		})
	}

	newKV := compactor.GetAll()

	var aliveKV []memtable.KV

	for _, val := range newKV {
		if val.Tombstone == false {
			aliveKV = append(aliveKV, val)
		}
	}

	bf, err := sstable.Flush(m.dirPath + "/data_compact.sst", aliveKV)

	for _, val := range m.ssTables {
		os.Remove(val.Path)
	}

	m.ssTables = make([]ssTableMetadata, 0)
	m.ssTables = append(m.ssTables, ssTableMetadata{
		Path:   m.dirPath + "/data_compact.sst",
		Filter: bf,
	})
	m.sstable_count = 1
	return err
}

func (m *MemoryEngine) Delete(key string) error {
	err := m.wal.Write(wal.Entry{
		Operation: wal.DeleteOperation,
		Key:       key,
	})
	if err != nil {
		return ErrWALWrite
	}
	m.avl.Delete(key)
	if m.avl.GetCount() >= 10 {
		m.flushToDisk()
	}
	return nil
}
func (m *MemoryEngine) ClearState() {
	m.lock.Lock()
	defer m.lock.Unlock()
	
	m.avl = memtable.NewAVLTree()
	m.wal.Reset()
	
	for _, val := range m.ssTables {
		os.Remove(val.Path)
	}
	m.ssTables = make([]ssTableMetadata, 0)
	m.sstable_count = 0
}

func (m *MemoryEngine) LoadSSTables() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	
	files, err := os.ReadDir(m.dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".sst") {
			continue
		}
		
		path := m.dirPath + "/" + file.Name()
		var keys []string
		
		sstable.ReadAll(path, func(key, value string, isTombStone bool) {
			keys = append(keys, key)
		})
		
		expected := len(keys)
		if expected == 0 {
			expected = 1
		}
		
		bf := bloom.NewFilter(expected)
		for _, key := range keys {
			bf.Add(key)
		}
		
		m.ssTables = append(m.ssTables, ssTableMetadata{
			Path:   path,
			Filter: bf,
		})
		m.sstable_count++
	}
	
	fmt.Printf("Recovered %d SSTables from disk!\n", m.sstable_count)
	return nil
}

func (m *MemoryEngine) Recovery() error {
	m.LoadSSTables()
	
	return m.wal.ReadAll(func(entry wal.Entry) error {
		switch entry.Operation {
		case wal.PutOperation:
			m.avl.Insert(entry.Key, string(entry.Value))
		case wal.DeleteOperation:
			m.avl.Delete(entry.Key)
		default:
			return CurroptedOperation
		}
		return nil
	})
}
