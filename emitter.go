package corral

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
	"sync"

	"github.com/bcongdon/corral/internal/pkg/backend"
	log "github.com/sirupsen/logrus"
)

type Emitter interface {
	Emit(key, value string) error
	Close() error
}

type reducerEmitter struct {
	writer io.WriteCloser
	mut    *sync.Mutex
}

func newReducerEmitter(writer io.WriteCloser) *reducerEmitter {
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

func (e *reducerEmitter) Close() error {
	return e.writer.Close()
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

func (me *mapperEmitter) Close() error {
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
