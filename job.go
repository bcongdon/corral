package corral

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/bcongdon/corral/backend"
	log "github.com/sirupsen/logrus"
)

type Job struct {
	Inputs          []string
	Map             Mapper
	Reduce          Reducer
	MaxSplitSize    int64
	MaxInputBinSize int64

	fileSystem       backend.FileSystem
	intermediateBins uint
}

func (j *Job) runMapper(mapperID uint, splits []inputSplit, mapper Mapper) error {
	emitter := newMapperEmitter(j.intermediateBins, mapperID, &j.fileSystem)
	defer emitter.close()

	for _, split := range splits {
		err := j.processMapperSplit(split, mapper, &emitter)
		if err != nil {
			return err
		}
	}

	return nil
}

func (j *Job) processMapperSplit(split inputSplit, mapper Mapper, emitter Emitter) error {
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
		mapper.Map("", record, emitter)
		fmt.Println(record)

		// Stop reading when end of inputSplit is reached
		pos := bytesRead
		fmt.Println(split.Size(), pos)
		if split.Size() > 0 && pos > split.Size() {
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
		reader, err := j.fileSystem.OpenReader(file.Name, 0)
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
		intermediateBins: 1,

		// Default MaxSplitSize is 50MB
		MaxSplitSize: 50 * 1000000,

		// Default MaxInputBinSize is 1.5GB
		MaxInputBinSize: 1500 * 1000000,
	}
}

func (j *Job) runningInLambda() bool {
	expectedEnvVars := []string{"LAMBDA_TASK_ROOT", "AWS_EXECUTION_ENV", "LAMBDA_RUNTIME_DIR"}
	for _, envVar := range expectedEnvVars {
		if os.Getenv(envVar) == "" {
			return false
		}
	}
	return true
}

func HandleRequest(ctx context.Context, task task) (string, error) {
	return fmt.Sprintf("Hello %d!", task.Phase), nil
}

func (j *Job) Main() {
	if j.runningInLambda() {
		lambda.Start(HandleRequest)
	}

	flag.Parse()
	j.Inputs = flag.Args()

	fs := new(backend.LocalBackend)
	fs.Init(".")
	j.fileSystem = fs

	var wg sync.WaitGroup
	inputSplits := j.inputSplits()

	inputBins := packInputSplits(inputSplits, j.MaxInputBinSize)
	for binID, bin := range inputBins {
		wg.Add(1)
		go func(bID uint, b []inputSplit) {
			defer wg.Done()
			j.runMapper(bID, b, j.Map)
		}(uint(binID), bin)
	}
	wg.Wait()

	for binID := uint(0); binID < j.intermediateBins; binID++ {
		wg.Add(1)
		go func(bID uint) {
			defer wg.Done()
			j.runReducer(bID, j.Reduce)
		}(binID)
	}
	wg.Wait()
}
