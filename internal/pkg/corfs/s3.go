package corfs

import (
	"errors"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/bcongdon/s3gof3r"
)

type S3Backend struct {
	bucket *s3gof3r.Bucket
	client *s3.S3
}

func (s *S3Backend) ListFiles() ([]FileInfo, error) {
	s3Files := make([]FileInfo, 0)

	params := &s3.ListObjectsInput{
		Bucket: &s.bucket.Name,
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

	return s3Files, err
}

func (s *S3Backend) OpenReader(filename string, startAt int64) (io.ReadCloser, error) {
	reader, _, err := s.bucket.GetOffsetReader(filename, nil, startAt)
	return reader, err
}

func (s *S3Backend) OpenWriter(filename string) (io.WriteCloser, error) {
	return s.bucket.PutWriter(filename, nil, nil)
}

func (s *S3Backend) Stat(filename string) (FileInfo, error) {
	params := &s3.ListObjectsInput{
		Bucket: &s.bucket.Name,
		Prefix: &filename,
	}
	result, err := s.client.ListObjects(params)
	if err != nil {
		return FileInfo{}, err
	}

	for _, object := range result.Contents {
		if *object.Key == filename {
			return FileInfo{
				Name: *object.Key,
				Size: *object.Size,
			}, nil
		}
	}

	return FileInfo{}, errors.New("No file with given filename")
}

func (s *S3Backend) Init(location string) error {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	sess, err := session.NewSession()
	if err != nil {
		return err
	}
	s.client = s3.New(sess)

	creds, err := sess.Config.Credentials.Get()
	if err != nil {
		return err
	}

	s3gof3rClient := s3gof3r.New("", s3gof3r.Keys{
		AccessKey:     creds.AccessKeyID,
		SecretKey:     creds.SecretAccessKey,
		SecurityToken: creds.SessionToken,
	})

	s.bucket = s3gof3rClient.Bucket(location)
	s.bucket.Md5Check = false
	return nil
}
