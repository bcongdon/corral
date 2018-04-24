package corral

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
	"sync"

	"github.com/bcongdon/corral/internal/pkg/corfs"
	log "github.com/sirupsen/logrus"
)

// Emitter enables mappers and reducers to yield key-value pairs.
type Emitter interface {
	Emit(key, value string) error
	close() error
	bytesWritten() int64
}

// reducerEmitter is a threadsafe emitter.
type reducerEmitter struct {
	writer       io.WriteCloser
	mut          *sync.Mutex
	writtenBytes int64
}

// newReducerEmitter initializes and returns a new reducerEmitter
func newReducerEmitter(writer io.WriteCloser) *reducerEmitter {
	return &reducerEmitter{
		writer: writer,
		mut:    &sync.Mutex{},
	}
}

// Emit yields a key-value pair to the framework.
func (e *reducerEmitter) Emit(key, value string) error {
	e.mut.Lock()
	defer e.mut.Unlock()

	n, err := e.writer.Write([]byte(fmt.Sprintf("%s\t%s\n", key, value)))
	e.writtenBytes += int64(n)
	return err
}

// close terminates the reducerEmitter. close must not be called more than once
func (e *reducerEmitter) close() error {
	return e.writer.Close()
}

func (e *reducerEmitter) bytesWritten() int64 {
	return e.writtenBytes
}

// mapperEmitter is an emitter that partitions keys written to it.
// mapperEmitter maintains a map of writers. Keys are partitioned into one of numBins
// intermediate "shuffle" bins. Each bin is written as a separate file.
type mapperEmitter struct {
	numBins       uint                    // number of intermediate shuffle bins
	writers       map[uint]io.WriteCloser // maps a parition number to an open writer
	fs            corfs.FileSystem        // filesystem to use when opening writers
	mapperID      uint                    // numeric identifier of the mapper using this emitter
	outDir        string                  // folder to save map output to
	partitionFunc PartitionFunc           // PartitionFunc to use when partitioning map output keys into intermediate bins
	writtenBytes  int64                   // counter for number of bytes written from emitted key/val pairs
}

// Initializes a new mapperEmitter
func newMapperEmitter(numBins uint, mapperID uint, outDir string, fs corfs.FileSystem) mapperEmitter {
	return mapperEmitter{
		numBins:       numBins,
		writers:       make(map[uint]io.WriteCloser, numBins),
		fs:            fs,
		mapperID:      mapperID,
		outDir:        outDir,
		partitionFunc: hashPartition,
	}
}

// hashPartition partitions a key to one of numBins shuffle bins
func hashPartition(key string, numBins uint) uint {
	h := fnv.New64()
	h.Write([]byte(key))
	return uint(h.Sum64() % uint64(numBins))
}

// Emit yields a key-value pair to the framework.
func (me *mapperEmitter) Emit(key, value string) error {
	bin := me.partitionFunc(key, me.numBins)

	// Open writer for the bin, if necessary
	writer, exists := me.writers[bin]
	if !exists {
		var err error
		path := me.fs.Join(me.outDir, fmt.Sprintf("map-bin%d-%d.out", bin, me.mapperID))

		writer, err = me.fs.OpenWriter(path)
		if err != nil {
			return err
		}
		me.writers[bin] = writer
	}

	kv := keyValue{
		Key:   key,
		Value: value,
	}

	data, err := json.Marshal(kv)
	if err != nil {
		log.Error(err)
		return err
	}

	data = append(data, '\n')
	_, err = writer.Write(data)
	return err
}

// close terminates the mapperEmitter. Must not be called more than once
func (me *mapperEmitter) close() error {
	errs := make([]string, 0)
	for _, writer := range me.writers {
		err := writer.Close()
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "\n"))
	}

	return nil
}

func (me *mapperEmitter) bytesWritten() int64 {
	return me.writtenBytes
}
