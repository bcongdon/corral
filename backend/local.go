package backend

import (
	"io"
	"os"
	"path/filepath"
)

type LocalBackend struct {
	basePath string
}

func (l *LocalBackend) ListFiles() []string {
	files := make([]string, 0)

	filepath.Walk(l.basePath, func(path string, f os.FileInfo, err error) error {
		files = append(files, path)
		return nil
	})

	return files
}

func (l *LocalBackend) OpenReader(path string) io.ReadSeeker {
	file, _ := os.OpenFile(path, os.O_RDONLY, 0777)
	return file
}

func (l *LocalBackend) OpenEmitter(path string) io.WriteCloser {
	file, _ := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0777)
	return file
}

func (l *LocalBackend) Init(basePath string) {
	l.basePath = basePath
}
