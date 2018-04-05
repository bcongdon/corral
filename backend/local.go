package backend

import (
	"io"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

type LocalBackend struct {
	basePath string
}

func (l *LocalBackend) ListFiles() ([]FileInfo, error) {
	files := make([]FileInfo, 0)

	err := filepath.Walk(l.basePath, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			log.Error(err)
			return err
		}
		files = append(files, FileInfo{
			Name: f.Name(),
			Size: f.Size(),
		})
		return err
	})

	return files, err
}

func (l *LocalBackend) OpenReader(path string) (io.ReadSeeker, error) {
	return os.OpenFile(path, os.O_RDONLY, 0600)
}

func (l *LocalBackend) OpenWriter(path string) (io.WriteCloser, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
}

func (l *LocalBackend) Stat(path string) (FileInfo, error) {
	fInfo, err := os.Stat(path)
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{
		Name: fInfo.Name(),
		Size: fInfo.Size(),
	}, nil
}

func (l *LocalBackend) Init(basePath string) {
	l.basePath = basePath
}
