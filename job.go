package corral

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/bcongdon/corral/backend"
	log "github.com/sirupsen/logrus"
)

type Job struct {
	Inputs       []string
	Map          Mapper
	Reduce       Reducer
	MaxSplitSize int64

	fileSystem       backend.FileSystem
	intermediateBins uint
}

func (j *Job) runMapper(mapperID uint, splits []inputSplit, mapper Mapper) error {
	emitter := newMapperEmitter(j.intermediateBins, mapperID, &j.fileSystem)

	for _, split := range splits {
		err := j.processMapperSplit(split, mapper, &emitter)
		if err != nil {
			return err
		}
	}

	emitter.close()

	return nil
}

func (j *Job) processMapperSplit(split inputSplit, mapper Mapper, emitter Emitter) error {
	inputSource, err := j.fileSystem.OpenReader(split.filename)
	if err != nil {
		return err
	}

	if split.startOffset > 0 {
		_, err := inputSource.Seek(split.startOffset, io.SeekStart)
		if err != nil {
			return err
		}
	}

	scanner := bufio.NewScanner(inputSource)
	startOffset, _ := inputSource.Seek(0, io.SeekStart)
	for scanner.Scan() {
		record := scanner.Text()
		mapper.Map("", record, emitter)

		// Stop reading when end of inputSplit is reached
		pos, _ := inputSource.Seek(0, io.SeekCurrent)
		if split.Size() > 0 && (pos-startOffset) > split.Size() {
			break
		}
	}

	return nil
}

func (j *Job) runReducer(binID uint, reducer Reducer) error {
	// Determine the intermediate data files this reducer is responsible for
	intermediateFiles := make([]backend.FileInfo, 0)

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
	if err != nil {
		return err
	}

	emitter := newReducerEmitter(emitWriter)

	keyChannels := make(map[string](chan string))
	var waitGroup sync.WaitGroup

	for _, file := range intermediateFiles {
		reader, err := j.fileSystem.OpenReader(file.Name)
		if err != nil {
			return err
		}
		log.Infof("Reducing on intermediate file: %s", file.Name)

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
					reducer.Reduce(kv.Key, keyIter, emitter)
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

func (j *Job) inputSplits() []inputSplit {
	splits := make([]inputSplit, 0)
	for _, inputFileName := range j.Inputs {
		fInfo, err := j.fileSystem.Stat(inputFileName)
		if err != nil {
			log.Warn("Unable to load input file: %s (%s)", inputFileName, err)
			continue
		}

		splits = append(splits, calculateInputSplits(fInfo, j.MaxSplitSize)...)
	}
	return splits
}

func NewJob(mapper Mapper, reducer Reducer) *Job {
	return &Job{
		Map:              mapper,
		Reduce:           reducer,
		intermediateBins: 10,

		// Default MaxSplitSize is 10MB
		// MaxSplitSize: 10 * 1000000,
		MaxSplitSize: 1000,
	}
}

func (j *Job) Main() {
	flag.Parse()
	j.Inputs = flag.Args()

	fs := new(backend.LocalBackend)
	fs.Init(".")
	j.fileSystem = fs

	var wg sync.WaitGroup
	for splitID, split := range j.inputSplits() {
		wg.Add(1)
		go func(sID uint, s inputSplit) {
			defer wg.Done()
			j.runMapper(sID, []inputSplit{s}, j.Map)
		}(uint(splitID), split)
	}
	wg.Wait()

	for splitID := uint(0); splitID < j.intermediateBins; splitID++ {
		wg.Add(1)
		go func(sID uint) {
			defer wg.Done()
			j.runReducer(sID, j.Reduce)
		}(splitID)
	}
	wg.Wait()
}
