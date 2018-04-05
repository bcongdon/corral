package corral

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"sync"

	"github.com/bcongdon/corral/backend"
	log "github.com/sirupsen/logrus"
)

type Emitter interface {
	Emit(key, value string) error
}

type reducerEmitter struct {
	writer io.Writer
	mut    *sync.Mutex
}

func newReducerEmitter(writer io.Writer) *reducerEmitter {
	return &reducerEmitter{
		writer: writer,
		mut:    &sync.Mutex{},
	}
}

func (e *reducerEmitter) Emit(key, value string) error {
	e.mut.Lock()
	defer e.mut.Unlock()

	_, err := e.writer.Write([]byte(fmt.Sprintf("%s\t%s\n", key, value)))
	return err
}

type mapperEmitter struct {
	numBins  uint
	writers  map[uint]io.WriteCloser
	fs       *backend.FileSystem
	mapperID uint
}

func newMapperEmitter(numBins uint, mapperID uint, fs *backend.FileSystem) mapperEmitter {
	return mapperEmitter{
		numBins:  numBins,
		writers:  make(map[uint]io.WriteCloser, numBins),
		fs:       fs,
		mapperID: mapperID,
	}
}

func (me *mapperEmitter) keyToBin(key string) uint {
	h := fnv.New64()
	h.Write([]byte(key))
	return uint(h.Sum64() % uint64(me.numBins))
}

func (me *mapperEmitter) Emit(key, value string) error {
	bin := me.keyToBin(key)
	writer, exists := me.writers[bin]
	if !exists {
		var err error
		writer, err = (*me.fs).OpenWriter(fmt.Sprintf("map-bin%d-%d.out", bin, me.mapperID))
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

func (me *mapperEmitter) close() {
	for _, writer := range me.writers {
		writer.Close()
	}
}
