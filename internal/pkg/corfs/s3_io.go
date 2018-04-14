package corfs

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mattetti/filebuffer"
)

type s3Writer struct {
	client *s3.S3
	bucket string
	key    string
	buf    *filebuffer.Buffer
}

func (s *s3Writer) Write(p []byte) (n int, err error) {
	n, err = s.buf.Write(p)
	return n, err
}

func (s *s3Writer) Close() error {
	s.buf.Seek(0, io.SeekStart)
	input := &s3.PutObjectInput{
		Body:   s.buf,
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	}
	_, err := s.client.PutObject(input)
	return err
}

type s3Reader struct {
	client    *s3.S3
	bucket    string
	key       string
	offset    int64
	chunkSize int64
	chunk     io.ReadCloser
	totalSize int64
}

func (s *s3Reader) loadNextChunk() error {
	size := min64(s.chunkSize, s.totalSize-s.offset)
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", s.offset, s.offset+size-1)),
	}
	s.offset += size
	output, err := s.client.GetObject(params)
	s.chunk = output.Body
	return err
}

func (s *s3Reader) Read(b []byte) (n int, err error) {
	n, err = s.chunk.Read(b)
	if err == io.EOF && s.offset != s.totalSize {
		err = s.loadNextChunk()
	}
	return n, err
}

func (s *s3Reader) Close() error {
	return s.chunk.Close()
}
