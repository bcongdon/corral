package corfs

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mattetti/filebuffer"
)

type s3Writer struct {
	client          *s3.S3
	bucket          string
	key             string
	buf             *filebuffer.Buffer
	uploadChunkSize int64
	uploadID        string
	complatedParts  []*s3.CompletedPart
}

func (s *s3Writer) Init() error {
	params := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	}
	result, err := s.client.CreateMultipartUpload(params)

	if result != nil {
		s.uploadID = *result.UploadId
	}
	return err
}

func (s *s3Writer) uploadChunk() error {
	s.buf.Seek(0, io.SeekStart)
	partNumber := int64(len(s.complatedParts) + 1)

	uploadParams := &s3.UploadPartInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(s.key),
		UploadId:   aws.String(s.uploadID),
		Body:       s.buf,
		PartNumber: aws.Int64(partNumber),
	}
	result, err := s.client.UploadPart(uploadParams)
	if result != nil {
		s.complatedParts = append(s.complatedParts, &s3.CompletedPart{
			ETag:       result.ETag,
			PartNumber: aws.Int64(partNumber),
		})
	}

	// Reset buffer
	s.buf = filebuffer.New(nil)

	return err
}

func (s *s3Writer) Write(p []byte) (n int, err error) {
	n, err = s.buf.Write(p)
	if int64(len(s.buf.Bytes())) > s.uploadChunkSize {
		err = s.uploadChunk()
	}
	return n, err
}

func (s *s3Writer) Close() error {
	err := s.uploadChunk()

	completeParams := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(s.key),
		UploadId: aws.String(s.uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: s.complatedParts,
		},
	}

	_, err = s.client.CompleteMultipartUpload(completeParams)

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
		s.chunk.Close()
		err = s.loadNextChunk()
	}
	return n, err
}

func (s *s3Reader) Close() error {
	return s.chunk.Close()
}
