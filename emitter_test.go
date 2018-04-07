package corral

import "testing"
import "github.com/stretchr/testify/assert"

func TestKeyToBin(t *testing.T) {
	for i := uint(0); i < 100; i++ {
		me := newMapperEmitter(100, i, nil)
		bin := me.keyToBin("foo")
		assert.Equal(t, bin, uint(0x63))
	}
}

func TestReducerEmitter(t *testing.T) {
	// TODO
}

func TestMapperEmitter(t *testing.T) {
	// TODO
}
