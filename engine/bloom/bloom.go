package bloom

import (
	"hash/fnv"
)

type Filter struct {
	bitset []bool
	size uint32
	numHashFunctions int
}

func NewFilter(expectedInsertions int) *Filter {
	return &Filter{
		size: uint32(expectedInsertions * 10),
		bitset: make([]bool,expectedInsertions * 10),
		numHashFunctions: 7,
	}
}

func (f *Filter) hash(key []byte, seed uint32) uint32 {
	h := fnv.New32a()
	h.Write(key)
	return (h.Sum32() ^ seed) % f.size
}

func (f *Filter) Add(key string) {
	for i := 0; i < f.numHashFunctions; i++ {
		index := f.hash([]byte(key), uint32(i))
		f.bitset[index] = true
	}
}

func (f *Filter) MightContain(key string) bool {
	for i := 0; i < f.numHashFunctions; i++ {
		index := f.hash([]byte(key), uint32(i))
		if !f.bitset[index] {
			return false
		}
	}
	return true
}