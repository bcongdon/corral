package corfs

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getS3TestBackend(t *testing.T) (string, *S3Backend) {
	backend := &S3Backend{}

	bucket := os.Getenv("AWS_TEST_BUCKET")
	if bucket == "" {
		t.Fatal("No test bucket is set under $AWS_TEST_BUCKET")
	}
	err := backend.Init()
	if err != nil {
		t.Skipf("Could not initialize S3 filesystem: %s", err)
	}
	return fmt.Sprintf("s3://%s", bucket), backend
}

func cleanup(backend *S3Backend, t *testing.T) {
	bucket := os.Getenv("AWS_TEST_BUCKET")
	objects, err := backend.ListFiles("s3://" + bucket + "/")

	assert.Nil(t, err)
	for _, obj := range objects {
		err = backend.Delete(obj.Name)
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
	bucket, backend := getS3TestBackend(t)
	defer cleanup(backend, t)

	path := bucket + "/testobj"

	// Test writer
	writer, err := backend.OpenWriter(path)
	assert.Nil(t, err)

	_, err = writer.Write([]byte("foo bar baz"))
	assert.Nil(t, err)

	err = writer.Close()
	assert.Nil(t, err)

	// Test reader starting at beginning of file
	reader, err := backend.OpenReader(path, 0)
	assert.Nil(t, err)

	contents, err := ioutil.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, "foo bar baz", string(contents))

	err = reader.Close()
	assert.Nil(t, err)
}

func TestS3ReaderWriterWithOffset(t *testing.T) {
	bucket, backend := getS3TestBackend(t)
	defer cleanup(backend, t)

	path := bucket + "/testobj"

	// Test writer
	writer, err := backend.OpenWriter(path)
	assert.Nil(t, err)

	_, err = writer.Write([]byte("foo bar baz"))
	assert.Nil(t, err)

	err = writer.Close()
	assert.Nil(t, err)

	// Test reader starting in middle of file
	reader, err := backend.OpenReader(path, 4)
	assert.Nil(t, err)

	contents, err := ioutil.ReadAll(reader)
	assert.Nil(t, err)
	assert.Equal(t, "bar baz", string(contents))

	err = reader.Close()
	assert.Nil(t, err)
}

func TestS3ListFiles(t *testing.T) {
	bucket, backend := getS3TestBackend(t)
	defer cleanup(backend, t)

	for i := 0; i < 5; i++ {
		fName := fmt.Sprintf("file%d", i)
		writer, err := backend.OpenWriter(bucket + "/" + fName)
		assert.Nil(t, err)

		_, err = writer.Write([]byte(fName))
		assert.Nil(t, err)
		err = writer.Close()
		assert.Nil(t, err)
	}

	files, err := backend.ListFiles(bucket)
	assert.Nil(t, err)
	assert.Len(t, files, 5)

	expectedPrefix := bucket + "/file"
	for _, file := range files {
		fmt.Println(file.Name, expectedPrefix)
		assert.True(t, strings.HasPrefix(file.Name, expectedPrefix))
		assert.Equal(t, int64(5), file.Size)
	}
}

func TestS3ListGlob(t *testing.T) {
	bucket, backend := getS3TestBackend(t)
	defer cleanup(backend, t)

	for i := 0; i < 3; i++ {
		fName := fmt.Sprintf("foo/file%d", i)
		writer, err := backend.OpenWriter(bucket + "/" + fName)
		assert.Nil(t, err)

		_, err = writer.Write([]byte(fName))
		assert.Nil(t, err)
		err = writer.Close()
		assert.Nil(t, err)
	}

	files, err := backend.ListFiles(bucket + "/foo/*")
	assert.Nil(t, err)
	assert.Len(t, files, 3)

	expectedPrefix := bucket + "/foo/file"
	for _, file := range files {
		fmt.Println(file.Name, expectedPrefix)
		assert.True(t, strings.HasPrefix(file.Name, expectedPrefix))
		assert.Equal(t, int64(9), file.Size)
	}
}

func TestS3Stat(t *testing.T) {
	bucket, backend := getS3TestBackend(t)
	defer cleanup(backend, t)

	path := bucket + "/testobj"

	writer, err := backend.OpenWriter(path)
	assert.Nil(t, err)

	_, err = writer.Write([]byte("foo bar baz"))
	assert.Nil(t, err)
	err = writer.Close()
	assert.Nil(t, err)

	file, err := backend.Stat(path)
	assert.Nil(t, err)

	assert.Equal(t, path, file.Name)
	assert.Equal(t, int64(11), file.Size)
}

func TestS3Join(t *testing.T) {
	_, backend := getS3TestBackend(t)

	res := backend.Join("s3://foo", "bar", "baz")
	assert.Equal(t, res, "s3://foo/bar/baz")

	res = backend.Join("s3://foo/", "/bar", "baz/")
	assert.Equal(t, res, "s3://foo/bar/baz/")
}
