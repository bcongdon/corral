package backend

import "testing"

func TestImplementsFileSystem(t *testing.T) {
	backend := S3Backend{}
	var fileSystem FileSystem
	fileSystem = backend
}
