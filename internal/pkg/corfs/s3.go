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
	lru "github.com/hashicorp/golang-lru"
	"github.com/mattetti/filebuffer"
)

var validS3Schemes = map[string]bool{
	"s3":  true,
	"s3a": true,
	"s3n": true,
}

var globRegex = regexp.MustCompile(`^(.*?)([\[\*\?].*)$`)

// S3FileSystem abstracts AWS S3 as a filesystem
type S3FileSystem struct {
	s3Client    *s3.S3
	objectCache *lru.Cache
}

var _ FileSystem = &S3FileSystem{}

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

// ListFiles lists files that match pathGlob.
func (s *S3FileSystem) ListFiles(pathGlob string) ([]FileInfo, error) {
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
				s.objectCache.Add(fullPath, object)
			}
			return true
		})

	return s3Files, err
}

// OpenReader opens a reader to the file at filePath. The reader
// is initially seeked to "startAt" bytes into the file.
func (s *S3FileSystem) OpenReader(filePath string, startAt int64) (io.ReadCloser, error) {
	parsed, err := parseS3URI(filePath)
	if err != nil {
		return nil, err
	}

	objStat, err := s.Stat(filePath)
	if err != nil {
		return nil, err
	}

	reader := &s3Reader{
		client:    s.s3Client,
		bucket:    parsed.Hostname(),
		key:       parsed.Path,
		offset:    startAt,
		chunkSize: 20 * 1024 * 1024, // 20 Mb chunk size
		totalSize: objStat.Size,
	}
	err = reader.loadNextChunk()
	return reader, err
}

// OpenWriter opens a writer to the file at filePath.
func (s *S3FileSystem) OpenWriter(filePath string) (io.WriteCloser, error) {
	parsed, err := parseS3URI(filePath)
	if err != nil {
		return nil, err
	}

	writer := &s3Writer{
		client:         s.s3Client,
		bucket:         parsed.Hostname(),
		key:            parsed.Path,
		buf:            filebuffer.New(nil),
		complatedParts: []*s3.CompletedPart{},
	}
	err = writer.Init()
	return writer, err
}

// Stat returns information about the file at filePath.
func (s *S3FileSystem) Stat(filePath string) (FileInfo, error) {
	if object, exists := s.objectCache.Get(filePath); exists {
		return FileInfo{
			Name: filePath,
			Size: *object.(*s3.Object).Size,
		}, nil
	}

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
			s.objectCache.Add(filePath, object)
			return FileInfo{
				Name: filePath,
				Size: *object.Size,
			}, nil
		}
	}

	return FileInfo{}, errors.New("No file with given filename")
}

// Init initializes the filesystem.
func (s *S3FileSystem) Init() error {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	sess, err := session.NewSession()
	if err != nil {
		return err
	}
	s.s3Client = s3.New(sess)

	s.objectCache, _ = lru.New(10000)

	return nil
}

// Delete deletes the file at filePath.
func (s *S3FileSystem) Delete(filePath string) error {
	parsed, err := parseS3URI(filePath)
	if err != nil {
		return err
	}

	params := &s3.DeleteObjectInput{
		Bucket: aws.String(parsed.Hostname()),
		Key:    aws.String(parsed.Path),
	}
	_, err = s.s3Client.DeleteObject(params)
	return err
}

// Join joins file path elements
func (s *S3FileSystem) Join(elem ...string) string {
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
