package art

import (
	"bytes"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newARTWithKeys(keys ...string) *ART {
	art := NewART()
	for _, k := range keys {
		art.Put([]byte(k), k)
	}
	return art
}

func TestSimplePrefix(t *testing.T) {
	assert := assert.New(t)
	art := newARTWithKeys(
		"abcd",
		"abc",
		"abe",
		"aberadasdad",
		"ab",
		"acadsadad",
		"bqe1231",
		"acdsadsad",
		"1231231",
	)

	result := make([]string, 0, 5)
	art.Prefix([]byte("ab"), func(key []byte, value interface{}) bool {
		result = append(result, value.(string))
		return false
	})
	except := []string{"ab", "abc", "abcd", "abe", "aberadasdad"}
	assert.Len(result, len(except))
	for i := range except {
		assert.Equal(except[i], result[i])
	}
}

func TestLongPrefix(t *testing.T) {
	assert := assert.New(t)
	art := newARTWithKeys(
		"absdwqbsbdbfbabfbqi21234",
		"absdwqbsbdbfbbbfaqi21334",
		"absdwqbsbdbfbbbfbqi11234",
		"acsdwqbsbdbfbfbfbqi21234",
		"adsdwqbsbdbfbfbfbqi21234",
	)

	result := make([]string, 0, 3)
	art.Prefix([]byte("absdwqbsbdbfb"), func(key []byte, value interface{}) bool {
		result = append(result, value.(string))
		return false
	})
	except := []string{"absdwqbsbdbfbabfbqi21234", "absdwqbsbdbfbbbfaqi21334", "absdwqbsbdbfbbbfbqi11234"}
	assert.Len(result, len(except))
	for i := range except {
		assert.Equal(except[i], result[i])
	}
}

func TestSimpleRange(t *testing.T) {
	assert := assert.New(t)
	art := newARTWithKeys(
		"1234",
		"12345",
		"123456",
		"234556",
		"23461",
		"235",
		"333",
		"33",
		"3",
	)

	result := make([]string, 0, 8)
	art.Range([]byte("1234"), []byte("33"), true, true, func(key []byte, value interface{}) bool {
		result = append(result, value.(string))
		return false
	})
	assert.Len(result, 8)
	except := []string{"1234", "12345", "123456", "234556", "23461", "235", "3", "33"}
	for i := range except {
		assert.Equal(except[i], result[i])
	}

	_, minV := art.Min()
	assert.Equal("1234", minV)
	_, maxV := art.Max()
	assert.Equal("333", maxV)

	result = result[:0]
	art.RangeTop(4, []byte("1234"), []byte("33"), true, true, func(key []byte, value interface{}) bool {
		result = append(result, value.(string))
		return false
	})
	except = []string{"1234", "12345", "123456", "234556"}
	assert.Len(result, len(except))
	for i := range except {
		assert.Equal(except[i], result[i])
	}
}

func TestLongBeginAndEnd(t *testing.T) {
	assert := assert.New(t)
	art := newARTWithKeys(
		"1234",
		"1235",
		"1236",
		"213",
	)

	var result []string
	art.Range([]byte("12345"), []byte("12367"), true, false, func(key []byte, value interface{}) bool {
		result = append(result, value.(string))
		return false
	})
	except := []string{"1235", "1236"}
	assert.Len(result, len(except))
	for i := range except {
		assert.Equal(except[i], result[i])
	}
}

func TestLargeRange(t *testing.T) {
	assert := assert.New(t)
	keys := loadTestData("words.txt", nil)
	art := NewART()
	for _, d := range keys {
		art.Put(d, d)
	}

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	var result [][]byte
	b, e := len(keys)/500, len(keys)/10*8
	art.Range(keys[b], keys[e], true, false, func(key []byte, _ interface{}) bool {
		result = append(result, key)
		return false
	})
	except := keys[b:e]
	assert.Len(result, len(except))
	for i := range except {
		assert.Equal(except[i], result[i], fmt.Sprintf("except %s at %d got %s", except[i], i, result[i]))
	}
}

func TestConcurrentPutAndRange(t *testing.T) {
	assert := assert.New(t)
	art := NewART()
	keys := loadTestData("words.txt", nil)
	pivot := len(keys) / 2
	mustExist, putKeys := keys[:pivot], keys[pivot:]
	for _, d := range mustExist {
		art.Put(d, d)
	}
	sort.Slice(mustExist, func(i, j int) bool {
		return bytes.Compare(mustExist[i], mustExist[j]) < 0
	})

	var start, done sync.WaitGroup
	start.Add(1)

	sz := runtime.GOMAXPROCS(0) - 1
	for i := 0; i < sz; i++ {
		done.Add(1)
		go func(i int) {
			start.Wait()
			b, e := (len(putKeys)/sz)*i, (len(putKeys)/sz)*(i+1)
			for _, d := range putKeys[b:e] {
				art.Put(d, d)
			}
			done.Done()
		}(i)
	}

	start.Done()
	var result [][]byte
	art.Range(mustExist[0], mustExist[pivot-1], true, true, func(key []byte, value interface{}) bool {
		result = append(result, key)
		return false
	})
	done.Wait()

	position := make(map[string]int)
	for i := range mustExist {
		position[string(mustExist[i])] = i
	}

	pos := 0
	for _, k := range result {
		str := string(k)
		if p, ok := position[str]; ok {
			assert.Equal(p, pos)
			pos++
		}
	}
}
