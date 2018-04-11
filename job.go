package corral

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/bcongdon/corral/internal/pkg/corfs"
	log "github.com/sirupsen/logrus"
)

// Job is the logical container for a MapReduce job
type Job struct {
	Map    Mapper
	Reduce Reducer

	fileSystem       corfs.FileSystem
	config           *config
	intermediateBins uint
}

// Logic for running a single map task
func (j *Job) runMapper(mapperID uint, splits []inputSplit) error {
	emitter := newMapperEmitter(j.intermediateBins, mapperID, &j.fileSystem)
	defer emitter.close()

	for _, split := range splits {
		err := j.runMapperSplit(split, &emitter)
		if err != nil {
			return err
		}
	}

	return nil
}

// runMapperSplit runs the mapper on a single inputSplit
func (j *Job) runMapperSplit(split inputSplit, emitter Emitter) error {
	offset := split.startOffset
	if split.startOffset != 0 {
		offset--
	}

	inputSource, err := j.fileSystem.OpenReader(split.filename, split.startOffset)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(inputSource)
	var bytesRead int64
	splitter := countingSplitFunc(bufio.ScanLines, &bytesRead)
	scanner.Split(splitter)

	if split.startOffset != 0 {
		scanner.Scan()
	}

	for scanner.Scan() {
		record := scanner.Text()
		j.Map.Map("", record, emitter)

		// Stop reading when end of inputSplit is reached
		pos := bytesRead
		if split.Size() > 0 && pos > split.Size() {
			break
		}
	}

	return nil
}

// Logic for running a single reduce task
func (j *Job) runReducer(binID uint) error {
	// Determine the intermediate data files this reducer is responsible for
	intermediateFiles := make([]corfs.FileInfo, 0)

	files, err := j.fileSystem.ListFiles()
	if err != nil {
		return err
	}
	for _, file := range files {
		if strings.Contains(file.Name, fmt.Sprintf("map-bin%d", binID)) {
			intermediateFiles = append(intermediateFiles, file)
		}
	}

	// Open emitter for output data
	emitWriter, err := j.fileSystem.OpenWriter(fmt.Sprintf("output-part-%d", binID))
	defer emitWriter.Close()
	if err != nil {
		return err
	}

	emitter := newReducerEmitter(emitWriter)

	keyChannels := make(map[string](chan string))
	var waitGroup sync.WaitGroup

	for _, file := range intermediateFiles {
		reader, err := j.fileSystem.OpenReader(file.Name, 0)
		if err != nil {
			return err
		}
		// log.Infof("Reducing on intermediate file: %s", file.Name)

		// Feed intermediate data into reducers
		decoder := json.NewDecoder(reader)
		for decoder.More() {
			var kv keyValue
			if err := decoder.Decode(&kv); err != nil {
				return err
			}

			// Create a reducer for the current key if necessary
			keyChan, exists := keyChannels[kv.Key]
			if !exists {
				keyChan = make(chan string)
				keyIter := newValueIterator(keyChan)
				keyChannels[kv.Key] = keyChan

				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()
					j.Reduce.Reduce(kv.Key, keyIter, emitter)
				}()
			}

			// Pass current value to the appropriate key channel
			keyChan <- kv.Value
		}
	}

	// Close key channels to signal that all intermediate data has been read
	for _, keyChan := range keyChannels {
		close(keyChan)
	}
	waitGroup.Wait()

	return nil
}

// inputSplits calculates all input files' inputSplits.
// inputSplits also determines and saves the number of intermediate bins that will be used during the shuffle.
func (j *Job) inputSplits(files []string, maxSplitSize int64) []inputSplit {
	splits := make([]inputSplit, 0)
	var totalSize int64
	for _, inputFileName := range files {
		fInfo, err := j.fileSystem.Stat(inputFileName)
		if err != nil {
			log.Warnf("Unable to load input file: %s (%s)", inputFileName, err)
			continue
		}

		totalSize += fInfo.Size
		splits = append(splits, splitInputFile(fInfo, maxSplitSize)...)
	}

	j.intermediateBins = uint(float64(totalSize/j.config.ReduceBinSize) * 1.25)
	if j.intermediateBins == 0 {
		j.intermediateBins = 1
	}

	return splits
}

// NewJob creates a new job from a Mapper and Reducer.
func NewJob(mapper Mapper, reducer Reducer) *Job {
	return &Job{
		Map:    mapper,
		Reduce: reducer,
	}
}
