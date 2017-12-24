package art

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleCRUD(t *testing.T) {
	assert := assert.New(t)
	art := NewART()

	art.Put([]byte("hello"), "world")
	assert.Equal("world", art.Get([]byte("hello")))

	art.Delete([]byte("hello false"))
	assert.Equal("world", art.Get([]byte("hello")))

	art.Delete([]byte("hello"))
	assert.Nil(art.Get([]byte("hello")))
}

func TestPrefixInsert(t *testing.T) {
	assert := assert.New(t)
	art := NewART()
	testCase := []struct {
		key   []byte
		value interface{}
	}{
		{[]byte{1}, "1"},
		{[]byte{1, 2, 3, 4}, "1 2 3 4"},
		{[]byte{1, 2}, "1 2"},
		{[]byte{1, 2, 3, 4, 5}, "1 2 3 4 5"},
		{[]byte{1, 2, 3}, "1 2 3"},
		{[]byte{2, 3, 4}, "2 3 4"},
		{[]byte{2, 3, 5}, "2 3 5"},
		{[]byte{2, 3}, "2 3"},
		{[]byte{3, 1}, "3 1"},
		{[]byte{3, 2, 3, 7, 5}, "3 2 3 7 5"},
		{[]byte{3, 2, 3, 4, 5}, "3 2 3 4 5"},
		{[]byte{3, 2}, "3 2"},
	}

	for _, c := range testCase {
		art.Put(c.key, c.value)
	}

	for _, c := range testCase {
		assert.Equal(c.value, art.Get(c.key))
	}
}

func TestEmptyKey(t *testing.T) {
	assert := assert.New(t)
	art := NewART()

	art.Put([]byte{}, "empty")
	assert.Equal("empty", art.Get([]byte{}))

	art.Put(nil, "nil")
	assert.Equal("nil", art.Get(nil))
	assert.Equal("nil", art.Get([]byte{}))
}

func TestNotFound(t *testing.T) {
	assert := assert.New(t)
	art := NewART()

	assert.Nil(art.Get(nil))
	assert.Nil(art.Get([]byte{1, 2, 3}))

	art.Put([]byte{1, 2}, "1 2")
	assert.Nil(art.Get([]byte{1, 2, 3}))

	art.Put([]byte{2, 3, 4, 5}, "2 3 4 5")
	assert.Nil(art.Get([]byte{2, 3, 6}))
	assert.Nil(art.Get([]byte{2, 3}))
}

func TestExpandLeaf(t *testing.T) {
	assert := assert.New(t)
	art := NewART()
	testCase := []struct {
		key   []byte
		value interface{}
	}{
		{[]byte("abcdefghijklmn"), 1},
		{[]byte("abcdefghijklmnopq"), 2},
		{[]byte("abcdefg"), 3},
		{[]byte("abcdefghijklmn123"), 4},

		{[]byte("deanthropomorphic"), 5},
		{[]byte("deanthropomorphism"), 6},
		{[]byte("deanthropomorphization"), 7},
		{[]byte("deanthropomorphize"), 8},
	}

	for _, c := range testCase {
		art.Put(c.key, c.value)
	}

	for _, c := range testCase {
		assert.Equal(c.value, art.Get(c.key))
	}
}

func TestCompressPath(t *testing.T) {
	assert := assert.New(t)
	art := NewART()

	art.Put([]byte{2, 1}, "21")
	art.Put([]byte{1, 2}, "12")
	art.Put([]byte{1, 2, 5}, "125")
	art.Put([]byte{1, 2, 3, 7}, "1237")
	art.Put([]byte{1, 2, 3, 4, 5}, "12345")
	art.Put([]byte{1, 2, 3, 4, 6}, "12346")

	art.Delete([]byte{1, 2, 3, 7})
	art.Delete([]byte{1, 2, 5})

	assert.Equal("12345", art.Get([]byte{1, 2, 3, 4, 5}))
	assert.Equal("12346", art.Get([]byte{1, 2, 3, 4, 6}))

	art.Delete([]byte{2, 1})
	assert.Equal("12", art.Get([]byte{1, 2}))

	art.Delete([]byte{1, 2, 3, 4, 5})
	assert.Equal("12346", art.Get([]byte{1, 2, 3, 4, 6}))

	art.Delete([]byte{1, 2})
	assert.Nil(art.Get([]byte{1, 2}))

	root := (*node4)(art.root)
	leaf := (*leaf)(root.children[0])
	assert.EqualValues(typeLeaf, leaf.nodeType)
	assert.True(leaf.match([]byte{1, 2, 3, 4, 6}))
	assert.Equal("12346", leaf.value)
}

func TestShrink(t *testing.T) {
	assert := assert.New(t)
	art := NewART()

	for i := 0; i < 256; i++ {
		art.Put([]byte{byte(i)}, i)
	}
	assert.EqualValues(typeNode256, (*node)(art.root).nodeType)

	for i := 0; i <= 256-node256MinSize; i++ {
		art.Delete([]byte{byte(i)})
	}
	assert.EqualValues(typeNode48, (*node)(art.root).nodeType)

	for i := 256 - node256MinSize + 1; i <= 256-node48MinSize; i++ {
		art.Delete([]byte{byte(i)})
	}
	assert.EqualValues(typeNode16, (*node)(art.root).nodeType)

	for i := 256 - node48MinSize + 1; i <= 256-node16MinSize; i++ {
		art.Delete([]byte{byte(i)})
	}
	assert.EqualValues(typeNode4, (*node)(art.root).nodeType)

	ptr := art.root
	for i := 256 - node16MinSize + 1; i < 256; i++ {
		art.Delete([]byte{byte(i)})
	}
	assert.EqualValues(typeNode4, (*node)(art.root).nodeType)
	assert.Equal(ptr, art.root)
}

func TestUpdateValue(t *testing.T) {
	assert := assert.New(t)
	art := NewART()

	art.Put([]byte("12"), "12")
	art.Put([]byte("12"), "12 new")
	assert.Equal("12 new", art.Get([]byte("12")))
	art.Put([]byte("123"), "123")
	art.Put([]byte("12"), "12 new2")
	assert.Equal("12 new2", art.Get([]byte("12")))
}

func TestGrowNode(t *testing.T) {
	assert := assert.New(t)
	art := NewART()

	var testCase []struct {
		key   []byte
		value interface{}
	}
	addCase := func(key ...uint8) {
		testCase = append(testCase, struct {
			key   []byte
			value interface{}
		}{key: key, value: fmt.Sprintf("%v", key)})
	}

	for i := 0; i < 256; i++ {
		addCase(byte(i))
	}
	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			addCase(byte(i), byte(j))
		}
	}

	for _, c := range testCase {
		art.Put(c.key, c.value)
	}
	for _, c := range testCase {
		assert.Equal(c.value, art.Get(c.key))
	}
}

func TestLargeFile(t *testing.T) {
	assert := assert.New(t)
	art := NewART()
	words := loadTestData("words.txt", nil)

	for _, d := range words {
		art.Put(d, d)
	}
	for _, d := range words {
		assert.Equal(d, art.Get(d))
	}

	art = NewART()
	uuid := loadTestData("uuid.txt", nil)
	for _, d := range uuid {
		art.Put(d, d)
	}
	for _, d := range uuid {
		assert.Equal(d, art.Get(d))
	}
}

func TestConcurrentPut(t *testing.T) {
	assert := assert.New(t)
	words := loadTestData("words.txt", nil)
	sz := runtime.GOMAXPROCS(0)

	for count := 0; count < 10; count++ {
		var wg, start sync.WaitGroup
		art := NewART()
		start.Add(1)
		for i := 0; i < sz; i++ {
			wg.Add(1)
			go func(i int) {
				start.Wait()
				b, e := (len(words)/sz)*i, (len(words)/sz)*(i+1)
				for _, d := range words[b:e] {
					art.Put(d, d)
				}
				wg.Done()
			}(i)
		}
		start.Done()
		wg.Wait()

		for _, d := range words[:(len(words)/sz)*sz] {
			assert.Equal(d, art.Get(d))
		}
	}
}

func TestConcurrentDelete(t *testing.T) {
	assert := assert.New(t)
	words := loadTestData("words.txt", nil)
	sz := runtime.GOMAXPROCS(0)

	deleteRng := words[:len(words)/5*4]
	for count := 0; count < 10; count++ {
		art := NewART()
		for _, d := range words {
			art.Put(d, d)
		}

		var wg, start sync.WaitGroup
		start.Add(1)
		for i := 0; i < sz; i++ {
			wg.Add(1)
			go func(i int) {
				start.Wait()
				b, e := (len(deleteRng)/sz)*i, (len(deleteRng)/sz)*(i+1)
				for _, d := range deleteRng[b:e] {
					art.Delete(d)
				}
				wg.Done()
			}(i)
		}
		start.Done()
		wg.Wait()

		for i, d := range words {
			v := art.Get(d)
			if i >= (len(deleteRng)/sz)*sz {
				assert.Equal(d, v)
			} else {
				assert.Nil(v)
			}
		}
	}
}

func TestConcurrentPutWithGet(t *testing.T) {
	assert := assert.New(t)
	words := loadTestData("words.txt", nil)
	sz := runtime.GOMAXPROCS(0)

	for count := 0; count < 10; count++ {
		var wg, start sync.WaitGroup
		art := NewART()
		start.Add(1)
		for i := 0; i < sz; i++ {
			wg.Add(1)
			go func(i int) {
				start.Wait()
				b, e := (len(words)/sz)*i, (len(words)/sz)*(i+1)
				for _, d := range words[b:e] {
					art.Put(d, d)
					assert.Equal(d, art.Get(d))
				}
				wg.Done()
			}(i)
		}
		start.Done()
		wg.Wait()

		for _, d := range words[:(len(words)/sz)*sz] {
			assert.Equal(d, art.Get(d))
		}
	}
}

func loadTestData(file string, b *testing.B) (data [][]byte) {
	if b != nil {
		b.Helper()
	}

	f, _ := os.Open("testdata/" + file)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		data = append(data, []byte(sc.Text()))
	}

	return
}

func benchPut(file string, b *testing.B) {
	b.Helper()
	art := NewART()
	data := loadTestData(file, b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, d := range data {
			art.Put(d, d)
		}
	}
}

func BenchmarkPutWords(b *testing.B) {
	benchPut("words.txt", b)
}

func BenchmarkPutUUID(b *testing.B) {
	benchPut("uuid.txt", b)
}

func benchGet(file string, b *testing.B) {
	b.Helper()
	art := NewART()
	data := loadTestData(file, b)
	for _, d := range data {
		art.Put(d, d)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, d := range data {
			art.Get(d)
		}
	}
}

func BenchmarkGetWords(b *testing.B) {
	benchGet("words.txt", b)
}

func BenchmarkGetUUID(b *testing.B) {
	benchGet("uuid.txt", b)
}
