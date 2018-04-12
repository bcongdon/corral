package corfs

import (
	"io"
)

// FileSystemType is an identifier for supported FileSystems
type FileSystemType int

// Identifiers for supported FileSystemTypes
const (
	Local FileSystemType = iota
	S3
)

// FileSystem provides the file backend for MapReduce jobs.
// Input data is read from a file system. Intermediate and output data
// is written to a file system.
// This is abstracted to allow remote filesystems like S3 to be supported.
type FileSystem interface {
	ListFiles(pathGlob string) ([]FileInfo, error)
	Stat(filePath string) (FileInfo, error)
	OpenReader(filePath string, startAt int64) (io.ReadCloser, error)
	OpenWriter(filePath string) (io.WriteCloser, error)
	Init() error
}

// FileInfo provides information about a file
type FileInfo struct {
	Name string // file path
	Size int64  // file size in bytes
}

// InitFilesystem intializes a filesystem of the given type relative to
// the specified location.
func InitFilesystem(fsType FileSystemType, location string) FileSystem {
	var fs FileSystem
	switch fsType {
	case Local:
		fs = &LocalFilesystem{}
	case S3:
		fs = &S3Backend{}
	}

	fs.Init()
	return fs
}
