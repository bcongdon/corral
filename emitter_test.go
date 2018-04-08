package corral

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"testing"

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

func TestMapperEmitter(t *testing.T) {
	// TODO
}
