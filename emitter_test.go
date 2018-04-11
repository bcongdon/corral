package corral

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"testing"

	"github.com/bcongdon/corral/internal/pkg/corfs"

	"github.com/stretchr/testify/assert"
)

type testWriteCloser struct {
	*bytes.Buffer
}

func (t *testWriteCloser) Close() error {
	return nil
}

func TestKeyToBin(t *testing.T) {
	for i := uint(0); i < 100; i++ {
		me := newMapperEmitter(100, i, nil)
		bin := me.keyToBin("foo")
		assert.Equal(t, bin, uint(0x63))
	}
}

func TestReducerEmitter(t *testing.T) {
	writer := &testWriteCloser{new(bytes.Buffer)}
	emitter := newReducerEmitter(writer)

	err := emitter.Emit("key", "value")
	assert.Nil(t, err)

	written, err := ioutil.ReadAll(writer)
	assert.Nil(t, err)
	assert.Equal(t, "key\tvalue\n", string(written))

	err = emitter.close()
	assert.Nil(t, err)
}

func TestReducerEmitterThreadSafety(t *testing.T) {
	writer := &testWriteCloser{new(bytes.Buffer)}
	emitter := newReducerEmitter(writer)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(key int) {
			defer wg.Done()
			err := emitter.Emit(fmt.Sprint(key), "value")
			assert.Nil(t, err)
		}(i)
	}
	wg.Wait()

	written, err := ioutil.ReadAll(writer)
	assert.Nil(t, err)

	records := strings.Split(string(written), "\n")
	assert.Len(t, records, 11)
	for i := 0; i < 10; i++ {
		assert.Contains(t, records, fmt.Sprintf("%d\tvalue", i))
	}

	err = emitter.close()
	assert.Nil(t, err)
}

type mockFs struct {
	writers map[string]*testWriteCloser
}

func (m *mockFs) ListFiles() ([]corfs.FileInfo, error) {
	return []corfs.FileInfo{}, nil
}

func (m *mockFs) OpenReader(filePath string, startAt int64) (io.ReadCloser, error) {
	return ioutil.NopCloser(new(bytes.Buffer)), nil
}

func (m *mockFs) OpenWriter(filePath string) (io.WriteCloser, error) {
	if _, ok := m.writers[filePath]; !ok {
		buf := new(bytes.Buffer)
		m.writers[filePath] = &testWriteCloser{buf}
	}
	return m.writers[filePath], nil
}

func (m *mockFs) Stat(filePath string) (corfs.FileInfo, error) {
	return corfs.FileInfo{filePath, 0}, nil
}

func (m *mockFs) Init(s string) error { return nil }

func TestMapperEmitter(t *testing.T) {
	mFs := &mockFs{writers: make(map[string]*testWriteCloser)}
	var fs corfs.FileSystem = mFs
	emitter := newMapperEmitter(3, 0, &fs)

	err := emitter.Emit("key1", "val1")
	assert.Nil(t, err)

	err = emitter.Emit("key123", "val2")
	assert.Nil(t, err)

	err = emitter.Emit("key359", "val3")
	assert.Nil(t, err)

	assert.Len(t, mFs.writers, 3)
	fmt.Println(mFs.writers)

	assert.Equal(t, `{"key":"key123","value":"val2"}`+"\n", string(mFs.writers["map-bin0-0.out"].Bytes()))
	assert.Equal(t, `{"key":"key359","value":"val3"}`+"\n", string(mFs.writers["map-bin1-0.out"].Bytes()))
	assert.Equal(t, `{"key":"key1","value":"val1"}`+"\n", string(mFs.writers["map-bin2-0.out"].Bytes()))

	assert.Nil(t, emitter.close())
}
