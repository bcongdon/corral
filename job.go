package corral

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/bcongdon/corral/backend"
	log "github.com/sirupsen/logrus"
)

type Job struct {
	Input  string
	Map    Mapper
	Reduce Reducer

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
	for scanner.Scan() {
		record := scanner.Text()
		mapper.Map("", record, emitter)
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

func (j *Job) calculateinputSplits() []inputSplit {
	return []inputSplit{
		inputSplit{j.Input, 0, 0},
	}
}

func NewJob(mapper Mapper, reducer Reducer) *Job {
	return &Job{
		Map:              mapper,
		Reduce:           reducer,
		intermediateBins: 10,
	}
}

func (j *Job) Main() {
	fs := new(backend.LocalBackend)
	fs.Init(".")
	j.Input = "metamorphosis.txt"
	j.fileSystem = fs

	for splitID, split := range j.calculateinputSplits() {
		fmt.Println(split)
		j.runMapper(uint(splitID), []inputSplit{split}, j.Map)
	}

	for splitID := uint(0); splitID < j.intermediateBins; splitID++ {
		j.runReducer(splitID, j.Reduce)
	}
}
