package corral

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitInputRecord(t *testing.T) {
	var splitRecordTests = []struct {
		input         string
		expectedKey   string
		expectedValue string
	}{
		{"foo\tbar", "foo", "bar"},
		{"foo\tbar\tbaz", "", "foo\tbar\tbaz"},
		{"foo bar baz", "", "foo bar baz"},
		{"key without value\t", "key without value", ""},
		{"\tvalue without key", "", "value without key"},
	}

	for _, test := range splitRecordTests {
		keyVal := splitInputRecord(test.input)
		assert.Equal(t, test.expectedKey, keyVal.Key)
		assert.Equal(t, test.expectedValue, keyVal.Value)
	}
}
