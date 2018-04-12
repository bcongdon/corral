package corfs

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/bcongdon/s3gof3r"
)

var validS3Schemes = map[string]bool{
	"s3":  true,
	"s3a": true,
	"s3n": true,
}

var globRegex = regexp.MustCompile(`^(.*?)([\[\*\?].*)$`)

type S3Backend struct {
	s3Client      *s3.S3
	s3Gof3rClient *s3gof3r.S3
}

func parseS3URI(uri string) (*url.URL, error) {
	parsed, err := url.Parse(uri)

	if _, ok := validS3Schemes[parsed.Scheme]; !ok {
		return nil, fmt.Errorf("Invalid s3 scheme: '%s'", parsed.Scheme)
	}

	// if !strings.Contains(parsed.Path, "/") {
	// 	return nil, fmt.Errorf("Invalid s3 url: '%s'", uri)
	// }

	if strings.HasPrefix(parsed.Path, "/") {
		parsed.Path = parsed.Path[1:]
	}

	return parsed, err
}

func (s *S3Backend) ListFiles(pathGlob string) ([]FileInfo, error) {
	s3Files := make([]FileInfo, 0)

	parsed, err := parseS3URI(pathGlob)
	if err != nil {
		return nil, err
	}

	baseURI := parsed.Path
	if globRegex.MatchString(parsed.Path) {
		baseURI = globRegex.FindStringSubmatch(parsed.Path)[1]
	}

	var dirGlob string
	if !strings.HasSuffix(pathGlob, "/") {
		dirGlob = pathGlob + "/*"
	} else {
		dirGlob = pathGlob + "*"
	}

	params := &s3.ListObjectsInput{
		Bucket: aws.String(parsed.Hostname()),
		Prefix: aws.String(baseURI),
	}

	objectPrefix := fmt.Sprintf("%s://%s/", parsed.Scheme, parsed.Hostname())
	err = s.s3Client.ListObjectsPages(params,
		func(page *s3.ListObjectsOutput, _ bool) bool {
			for _, object := range page.Contents {
				fullPath := objectPrefix + *object.Key

				dirMatch, _ := filepath.Match(dirGlob, fullPath)
				pathMatch, _ := filepath.Match(pathGlob, fullPath)
				if !(dirMatch || pathMatch) {
					continue
				}

				s3Files = append(s3Files, FileInfo{
					Name: fullPath,
					Size: *object.Size,
				})
			}
			return true
		})

	return s3Files, err
}

func (s *S3Backend) OpenReader(filePath string, startAt int64) (io.ReadCloser, error) {
	parsed, err := parseS3URI(filePath)
	if err != nil {
		return nil, err
	}

	bucket := s.s3Gof3rClient.Bucket(parsed.Hostname())

	reader, _, err := bucket.GetOffsetReader(parsed.Path, nil, startAt)
	return reader, err
}

func (s *S3Backend) OpenWriter(filePath string) (io.WriteCloser, error) {
	parsed, err := parseS3URI(filePath)
	if err != nil {
		return nil, err
	}

	bucket := s.s3Gof3rClient.Bucket(parsed.Hostname())

	cfg := s3gof3r.DefaultConfig
	cfg.Concurrency = 1
	cfg.Md5Check = false
	return bucket.PutWriter(parsed.Path, nil, cfg)
}

func (s *S3Backend) Stat(filePath string) (FileInfo, error) {
	parsed, err := parseS3URI(filePath)
	if err != nil {
		return FileInfo{}, err
	}

	params := &s3.ListObjectsInput{
		Bucket: aws.String(parsed.Hostname()),
		Prefix: aws.String(parsed.Path),
	}
	result, err := s.s3Client.ListObjects(params)
	if err != nil {
		return FileInfo{}, err
	}

	for _, object := range result.Contents {
		if *object.Key == parsed.Path {
			return FileInfo{
				Name: filePath,
				Size: *object.Size,
			}, nil
		}
	}

	return FileInfo{}, errors.New("No file with given filename")
}

func (s *S3Backend) Init() error {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	sess, err := session.NewSession()
	if err != nil {
		return err
	}
	s.s3Client = s3.New(sess)

	creds, err := sess.Config.Credentials.Get()
	if err != nil {
		return err
	}

	s.s3Gof3rClient = s3gof3r.New("", s3gof3r.Keys{
		AccessKey:     creds.AccessKeyID,
		SecretKey:     creds.SecretAccessKey,
		SecurityToken: creds.SessionToken,
	})

	return nil
}

func (s *S3Backend) Delete(filePath string) error {
	parsed, err := parseS3URI(filePath)
	if err != nil {
		return err
	}

	bucket := s.s3Gof3rClient.Bucket(parsed.Hostname())
	return bucket.Delete(parsed.Path)
}

func (s *S3Backend) Join(elem ...string) string {
	stripped := make([]string, len(elem))
	for i, str := range elem {
		if strings.HasPrefix(str, "/") {
			str = str[1:]
		}
		if strings.HasSuffix(str, "/") && i != len(elem)-1 {
			str = str[:len(str)-1]
		}
		stripped[i] = str
	}
	return strings.Join(stripped, "/")
}
