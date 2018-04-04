package corral

import (
	"bufio"
	"fmt"
	"io"

	"github.com/bcongdon/corral/backend"
)

type Job struct {
	Input      string
	Map        Mapper
	Reduce     Reducer
	fileSystem backend.FileSystem
}

type emitter struct {
	writer io.Writer
}

func (e emitter) Emit(key, value string) {
	fmt.Println(key, value)
	e.writer.Write([]byte(fmt.Sprintf("%s\t%s\n", key, value)))
}

func (j *Job) runMapper(splitID int, split InputSplit, mapper Mapper) error {
	inputSource := j.fileSystem.OpenReader(split.filename)
	emitWriter := j.fileSystem.OpenEmitter(fmt.Sprintf("%s-map-%d", split.filename, splitID))

	if split.startOffset > 0 {
		_, err := inputSource.Seek(split.startOffset, io.SeekStart)
		if err != nil {
			return err
		}
	}

	emitter := emitter{writer: emitWriter}

	scanner := bufio.NewScanner(inputSource)
	for scanner.Scan() {
		record := scanner.Text()
		mapper.Map("", record, emitter)
	}

	emitWriter.Close()

	return nil
}

func (j *Job) runReducer(splitID int, split InputSplit, reducer Reducer) error {
	inputSource := j.fileSystem.OpenReader(split.filename)
	emitWriter := j.fileSystem.OpenEmitter(fmt.Sprintf("%s-map-%d", split.filename, splitID))

	if split.startOffset > 0 {
		_, err := inputSource.Seek(split.startOffset, io.SeekStart)
		if err != nil {
			return err
		}
	}

	emitter := emitter{writer: emitWriter}

	scanner := bufio.NewScanner(inputSource)
	for scanner.Scan() {
		record := scanner.Text()
	}

	mapper.Reduce()

	emitWriter.Close()

	return nil
}

func (j *Job) calculateInputSplits() []InputSplit {
	return []InputSplit{
		InputSplit{j.Input, 0, 0},
	}
}

func (j *Job) Main() {
	fs := new(backend.LocalBackend)
	fs.Init("./tmp")
	j.Input = "word_count.go"
	j.fileSystem = fs

	for splitID, inputSplit := range j.calculateInputSplits() {
		fmt.Println(inputSplit)
		j.runMapper(splitID, inputSplit, j.Map)
	}

	for splitID := range j.calculateInputSplits() {
		inputSplit := InputSplit{fmt.Sprintf("%s-map-%d", j.Input, splitID), 0, 0}
		j.runReducer(splitID, inputSplit, j.Reduce)
	}
}
