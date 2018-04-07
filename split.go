package corral

import (
	"bufio"

	"github.com/bcongdon/corral/internal/pkg/backend"
)

type inputSplit struct {
	filename    string
	startOffset int64
	endOffset   int64
}

func (i inputSplit) Size() int64 {
	return i.endOffset - i.startOffset + 1
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func splitInputFile(file backend.FileInfo, maxSplitSize int64) []inputSplit {
	splits := make([]inputSplit, 0)

	for startOffset := int64(0); startOffset < file.Size; startOffset += maxSplitSize {
		endOffset := min(startOffset+maxSplitSize-1, file.Size-1)
		newSplit := inputSplit{
			filename:    file.Name,
			startOffset: startOffset,
			endOffset:   endOffset,
		}
		splits = append(splits, newSplit)
	}

	return splits
}

type inputBin struct {
	splits []inputSplit
	size   int64
}

// packInputSplits partitions inputSplits into bins.
// The combined size of each bin will be no greater than maxBinSize
func packInputSplits(splits []inputSplit, maxBinSize int64) [][]inputSplit {
	if len(splits) == 0 {
		return [][]inputSplit{}
	}

	bins := make([]*inputBin, 1)
	bins[0] = &inputBin{
		splits: make([]inputSplit, 0),
		size:   0,
	}

	// Partition splits into bins using a naive Next-Fit packing algorithm
	for _, split := range splits {
		currBin := bins[len(bins)-1]

		if currBin.size+split.Size() <= maxBinSize {
			currBin.splits = append(currBin.splits, split)
			currBin.size += split.Size()
		} else {
			newBin := &inputBin{
				splits: []inputSplit{split},
				size:   split.Size(),
			}
			bins = append(bins, newBin)
		}
	}

	binnedSplits := make([][]inputSplit, len(bins))
	for i, bin := range bins {
		binnedSplits[i] = bin.splits
	}
	return binnedSplits
}

func countingSplitFunc(split bufio.SplitFunc, bytesRead *int64) bufio.SplitFunc {
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		adv, tok, err := split(data, atEOF)
		(*bytesRead) += int64(adv)
		return adv, tok, err
	}
}
