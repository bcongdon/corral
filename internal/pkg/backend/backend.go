package backend

import (
	"io"
)

type FileSystemType string

const (
	Local = "local"
	S3    = "s3"
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
	init(location string)
}

type FileInfo struct {
	Name string
	Size int64
}

func InitFilesystem(fsType, location string) FileSystem {
	var fs FileSystem
	switch fsType {
	case Local:
		fs = &LocalFilesystem{}
	case S3:
		fs = &S3Backend{}
	}

	fs.init(location)
	return fs
}
