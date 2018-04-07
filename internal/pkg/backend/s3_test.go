package backend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestS3ImplementsFileSystem(t *testing.T) {
	backend := S3Backend{}
	var fileSystem FileSystem
	fileSystem = &backend

	assert.NotNil(t, fileSystem)
}
