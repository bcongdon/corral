package backend

import (
	"io"
)

type NameSizer interface {
	Name() string
	Size() int64
}

type FileSystem interface {
	ListFiles() ([]FileInfo, error)
	Stat(filename string) (FileInfo, error)
	OpenReader(filename string, startAt int64) (io.ReadCloser, error)
	OpenWriter(filename string) (io.WriteCloser, error)
}

type FileInfo struct {
	Name string
	Size int64
}
