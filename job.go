package corral

import (
	"bufio"
	"encoding/json"
	"fmt"
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
	outputPath       string
}

// Logic for running a single map task
func (j *Job) runMapper(mapperID uint, splits []inputSplit) error {
	emitter := newMapperEmitter(j.intermediateBins, mapperID, j.outputPath, j.fileSystem)
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
	offset := split.StartOffset
	if split.StartOffset != 0 {
		offset--
	}

	inputSource, err := j.fileSystem.OpenReader(split.Filename, split.StartOffset)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(inputSource)
	var bytesRead int64
	splitter := countingSplitFunc(bufio.ScanLines, &bytesRead)
	scanner.Split(splitter)

	if split.StartOffset != 0 {
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
	path := j.fileSystem.Join(j.outputPath, fmt.Sprintf("map-bin%d*", binID))
	files, err := j.fileSystem.ListFiles(path)
	if err != nil {
		return err
	}

	// Open emitter for output data
	path = j.fileSystem.Join(j.outputPath, fmt.Sprintf("output-part-%d", binID))
	emitWriter, err := j.fileSystem.OpenWriter(path)
	defer emitWriter.Close()
	if err != nil {
		return err
	}

	emitter := newReducerEmitter(emitWriter)

	data := make(map[string][]string, 10000)

	keyChannels := make(map[string](chan string))
	var waitGroup sync.WaitGroup

	for _, file := range files {
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

			if _, ok := data[kv.Key]; !ok {
				data[kv.Key] = make([]string, 0)
			}

			data[kv.Key] = append(data[kv.Key], kv.Value)

		}
	}

	for key, values := range data {
		// Create a reducer for the current key if necessary
		keyChan, exists := keyChannels[key]
		if !exists {
			keyChan = make(chan string)
			keyIter := newValueIterator(keyChan)
			keyChannels[key] = keyChan

			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				j.Reduce.Reduce(key, keyIter, emitter)
			}()
		}

		for _, value := range values {
			// Pass current value to the appropriate key channel
			keyChan <- value
		}
		close(keyChan)
	}

	// Close key channels to signal that all intermediate data has been read
	// for _, keyChan := range keyChannels {
	// 	close(keyChan)
	// }
	waitGroup.Wait()

	return nil
}

// inputSplits calculates all input files' inputSplits.
// inputSplits also determines and saves the number of intermediate bins that will be used during the shuffle.
func (j *Job) inputSplits(inputs []string, maxSplitSize int64) []inputSplit {
	splits := make([]inputSplit, 0)

	files := make([]string, 0)
	for _, inputPath := range inputs {
		fileInfos, err := j.fileSystem.ListFiles(inputPath)
		if err != nil {
			log.Warn(err)
			continue
		}

		for _, fInfo := range fileInfos {
			files = append(files, fInfo.Name)
		}
	}

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
