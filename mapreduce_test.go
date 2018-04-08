package corral

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValueIterator(t *testing.T) {
	values := []string{"foo", "bar", "baz"}
	ch := make(chan string, 3)

	for _, val := range values {
		ch <- val
	}
	close(ch)

	iterator := newValueIterator(ch)
	i := 0
	for val := range iterator.Iter() {
		assert.Equal(t, values[i], val)
		i++
	}
}
