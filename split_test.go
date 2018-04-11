package corral

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/bcongdon/corral/internal/pkg/corfs"
	"github.com/stretchr/testify/assert"
)

func TestPackInputSplits(t *testing.T) {
	var packingTests = []struct {
		splitSizes []int
		maxBinSize int64
	}{
		{[]int{}, 0},
		{[]int{1, 2, 3}, 3},
		{[]int{3, 3, 1, 2, 3}, 3},
	}

	for _, test := range packingTests {
		splits := make([]inputSplit, len(test.splitSizes))
		for i, size := range test.splitSizes {
			splits[i] = inputSplit{
				StartOffset: 0,
				EndOffset:   int64(size) - 1,
			}
		}

		splitsSeen := 0
		bins := packInputSplits(splits, test.maxBinSize)
		for _, bin := range bins {
			binSize := int64(0)
			splitsSeen += len(bin)
			for _, split := range bin {
				binSize += split.Size()
			}
			assert.True(t, binSize <= test.maxBinSize)
		}

		// Make sure that all splits have been put in exactly 1 bin
		assert.Equal(t, len(test.splitSizes), splitsSeen)
	}
}

func TestCalculateInputSplits(t *testing.T) {
	var calculateSplitTests = []struct {
		fileSize            int64
		maxSplitSize        int64
		expectedSplitStarts []int64
		expectedSplitEnds   []int64
	}{
		{3, 3, []int64{0}, []int64{2}},
		{10, 3, []int64{0, 3, 6, 9}, []int64{2, 5, 8, 9}},
		{5, 10, []int64{0}, []int64{4}},
	}

	for _, test := range calculateSplitTests {
		fInfo := corfs.FileInfo{
			Size: test.fileSize,
		}

		splits := splitInputFile(fInfo, test.maxSplitSize)

		assert.Equal(t, len(test.expectedSplitStarts), len(splits), fmt.Sprintln(splits))
		for i, split := range splits {
			assert.Equal(t, test.expectedSplitStarts[i], split.StartOffset)
			assert.Equal(t, test.expectedSplitEnds[i], split.EndOffset)
		}
	}
}

func TestSplitSize(t *testing.T) {
	var splitSizeTests = []struct {
		startOffset  int64
		endOffset    int64
		expectedSize int64
	}{
		{0, 9, 10},
		{100, 999, 900},
		{1000, 1000, 1},
	}

	for _, test := range splitSizeTests {
		split := inputSplit{
			StartOffset: test.startOffset,
			EndOffset:   test.endOffset,
		}
		assert.Equal(t, test.expectedSize, split.Size())
	}
}

func TestCountingSplitFunc(t *testing.T) {
	var bytesRead int64
	splitFunc := countingSplitFunc(bufio.ScanLines, &bytesRead)

	buf := new(bytes.Buffer)
	buf.Write([]byte("foo\n123456\na"))

	scanner := bufio.NewScanner(buf)
	scanner.Split(splitFunc)

	assert.Equal(t, int64(0), bytesRead)

	scanner.Scan()
	assert.Equal(t, int64(4), bytesRead)
	assert.Equal(t, "foo", scanner.Text())

	scanner.Scan()
	assert.Equal(t, int64(4+7), bytesRead)
	assert.Equal(t, "123456", scanner.Text())

	scanner.Scan()
	assert.Equal(t, int64(4+7+1), bytesRead)
	assert.Equal(t, "a", scanner.Text())
}
