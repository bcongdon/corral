package corral

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDriver(t *testing.T) {
	j := &Job{}
	driver := NewDriver(
		j,
		WithSplitSize(100),
		WithMapBinSize(200),
		WithReduceBinSize(300),
		WithWorkingLocation("s3://foo"),
	)

	assert.Equal(t, j, driver.jobs[0])
	assert.Equal(t, int64(100), driver.config.SplitSize)
	assert.Equal(t, int64(200), driver.config.MapBinSize)
	assert.Equal(t, int64(300), driver.config.ReduceBinSize)
	assert.Equal(t, "s3://foo", driver.config.WorkingLocation)
}

type testWCJob struct{}

func (testWCJob) Map(key, value string, emitter Emitter) {
	for _, word := range strings.Fields(value) {
		emitter.Emit(word, "1")
	}
}

func (testWCJob) Reduce(key string, values ValueIterator, emitter Emitter) {
	count := 0
	for _ = range values.Iter() {
		count++
	}
	emitter.Emit(key, fmt.Sprintf("%d", count))
}

type testFilterJob struct {
	prefix string
}

func (j *testFilterJob) Map(key, value string, emitter Emitter) {
	if strings.HasPrefix(key, j.prefix) {
		emitter.Emit(key, value)
	}
}

func (j *testFilterJob) Reduce(key string, values ValueIterator, emitter Emitter) {
	// Identity reducer
	for value := range values.Iter() {
		emitter.Emit(key, value)
	}

}

func testOutputToKeyValues(output string) []keyValue {
	lines := strings.Split(output, "\n")
	keyVals := make([]keyValue, 0, len(lines))

	for _, line := range lines {
		split := strings.Split(line, "\t")
		if len(split) != 2 {
			continue
		}
		keyVals = append(keyVals, keyValue{
			Key:   split[0],
			Value: split[1],
		})
	}
	return keyVals
}

func TestLocalMapReduce(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpdir)

	inputPath := filepath.Join(tmpdir, "test_input")
	ioutil.WriteFile(inputPath, []byte("the test input\nthe input test\nfoo bar baz"), 0700)

	job := NewJob(testWCJob{}, testWCJob{})
	driver := NewDriver(
		job,
		WithInputs(tmpdir),
		WithWorkingLocation(tmpdir),
	)

	driver.Main()

	output, err := ioutil.ReadFile(filepath.Join(tmpdir, "output-part-0"))
	assert.Nil(t, err)

	keyVals := testOutputToKeyValues(string(output))
	assert.Len(t, keyVals, 6)

	correct := []keyValue{
		{"the", "2"},
		{"test", "2"},
		{"input", "2"},
		{"foo", "1"},
		{"bar", "1"},
		{"baz", "1"},
	}
	for _, kv := range correct {
		assert.Contains(t, keyVals, kv)
	}
}

func TestLocalMultiJob(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpdir)

	inputPath := filepath.Join(tmpdir, "test_input")
	ioutil.WriteFile(inputPath, []byte("the test input\nthe input test\nfoo bar baz"), 0700)

	mr1 := testWCJob{}
	job1 := NewJob(mr1, mr1)

	// Second job filters out any keys that don't start with 't'
	mr2 := &testFilterJob{prefix: "t"}
	job2 := NewJob(mr2, mr2)

	driver := NewMultiStageDriver([]*Job{job1, job2},
		WithInputs(tmpdir),
		WithWorkingLocation(tmpdir),
	)

	driver.Main()

	output, err := ioutil.ReadFile(filepath.Join(tmpdir, "output-part-0"))
	assert.Nil(t, err)

	keyVals := testOutputToKeyValues(string(output))
	assert.Len(t, keyVals, 2)

	correct := []keyValue{
		{"the", "2"},
		{"test", "2"},
	}
	for _, kv := range correct {
		assert.Contains(t, keyVals, kv)
	}
}
