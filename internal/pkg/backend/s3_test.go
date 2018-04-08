package backend

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getS3TestBackend(t *testing.T) *S3Backend {
	backend := &S3Backend{}
	// TODO: Load test bucket from os.Environ
	err := backend.init("test-bucketfdasfasdfas34432")
	if err != nil {
		t.Skipf("Could not initialize S3 filesystem: %s", err)
	}
	return backend
}

func cleanup(backend *S3Backend, t *testing.T) {
	objects, err := backend.ListFiles()
	assert.Nil(t, err)
	for _, obj := range objects {
		err = backend.bucket.Delete(obj.Name)
		assert.Nil(t, err)
	}
}

func TestS3ImplementsFileSystem(t *testing.T) {
	backend := S3Backend{}
	var fileSystem FileSystem
	fileSystem = &backend

	assert.NotNil(t, fileSystem)
}

func TestS3ReaderWriter(t *testing.T) {
	backend := getS3TestBackend(t)
	defer cleanup(backend, t)

	// Test writer
	writer, err := backend.OpenWriter("testobj")
	assert.Nil(t, err)

	_, err = writer.Write([]byte("foo bar baz"))
	assert.Nil(t, err)

	err = writer.Close()
	assert.Nil(t, err)

	// Test reader starting at beginning of file
	reader, err := backend.OpenReader("testobj", 0)
	assert.Nil(t, err)

	contents, err := ioutil.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, "foo bar baz", string(contents))

	err = reader.Close()
	assert.Nil(t, err)
}

func TestS3ReaderWriterWithOffset(t *testing.T) {
	backend := getS3TestBackend(t)
	defer cleanup(backend, t)

	// Test writer
	writer, err := backend.OpenWriter("testobj")
	assert.Nil(t, err)

	_, err = writer.Write([]byte("foo bar baz"))
	assert.Nil(t, err)

	err = writer.Close()
	assert.Nil(t, err)

	// Test reader starting in middle of file
	reader, err := backend.OpenReader("testobj", 4)
	assert.Nil(t, err)

	contents, err := ioutil.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, "bar baz", string(contents))

	err = reader.Close()
	assert.Nil(t, err)
}

func TestS3ListFiles(t *testing.T) {
	backend := getS3TestBackend(t)
	defer cleanup(backend, t)

	for i := 0; i < 5; i++ {
		fName := fmt.Sprintf("file%d", i)
		writer, err := backend.OpenWriter(fName)
		assert.Nil(t, err)

		_, err = writer.Write([]byte(fName))
		assert.Nil(t, err)
		err = writer.Close()
		assert.Nil(t, err)
	}

	files, err := backend.ListFiles()
	assert.Nil(t, err)
	assert.Len(t, files, 5)

	for _, file := range files {
		assert.True(t, strings.HasPrefix(file.Name, "file"))
		assert.Equal(t, int64(5), file.Size)
	}
}

func TestS3Stat(t *testing.T) {
	backend := getS3TestBackend(t)
	defer cleanup(backend, t)

	writer, err := backend.OpenWriter("testfile")
	assert.Nil(t, err)

	_, err = writer.Write([]byte("foo bar baz"))
	assert.Nil(t, err)
	err = writer.Close()
	assert.Nil(t, err)

	file, err := backend.Stat("testfile")
	assert.Nil(t, err)

	assert.Equal(t, "testfile", file.Name)
	assert.Equal(t, int64(11), file.Size)
}
