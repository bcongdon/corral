package backend

import (
	"io"

	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Backend struct {
	bucket string
	client *s3.S3
}

func (s *S3Backend) ListFiles() []FileInfo {
	s3Files := make([]FileInfo, 0)

	params := &s3.ListObjectsInput{
		Bucket: &s.bucket,
	}
	err := s.client.ListObjectsPages(params,
		func(page *s3.ListObjectsOutput, _ bool) bool {
			for _, object := range page.Contents {
				s3Files = append(s3Files, FileInfo{
					Name: *object.Key,
					Size: *object.Size,
				})
			}
			return true
		})
	if err != nil {
		panic(err)
	}

	return s3Files
}

func (s *S3Backend) OpenReader(filename string) io.ReadSeeker {
	return nil
}

func (s *S3Backend) OpenWriter(filename string) io.WriteCloser {
	return nil
}
