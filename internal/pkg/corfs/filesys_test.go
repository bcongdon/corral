package corfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitFilesystem(t *testing.T) {
	fs := InitFilesystem(S3)
	assert.NotNil(t, fs)
	assert.IsType(t, &S3FileSystem{}, fs)

	fs = InitFilesystem(Local)
	assert.NotNil(t, fs)
	assert.IsType(t, &LocalFileSystem{}, fs)
}

func TestInferFilesystem(t *testing.T) {
	fs := InferFilesystem("s3://foo/bar.txt")
	assert.NotNil(t, fs)
	assert.IsType(t, &S3FileSystem{}, fs)

	fs = InferFilesystem("./bar.txt")
	assert.NotNil(t, fs)
	assert.IsType(t, &LocalFileSystem{}, fs)
}
