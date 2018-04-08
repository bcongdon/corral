package backend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitFilesystem(t *testing.T) {
	fs := InitFilesystem(S3, "someBucket")
	assert.NotNil(t, fs)
	assert.IsType(t, &S3Backend{}, fs)

	fs = InitFilesystem(Local, "someDir")
	assert.NotNil(t, fs)
	assert.IsType(t, &LocalFilesystem{}, fs)
}
