package backend

import (
	"io"
)

type FileSystem interface {
	ListFiles() []string
	OpenReader(filename string) io.ReadSeeker
	OpenEmitter(filename string) io.WriteCloser
}
