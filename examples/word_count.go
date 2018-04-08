package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/bcongdon/corral"
)

type wordCount struct{}

func (w wordCount) Map(key, value string, emitter corral.Emitter) {
	re := regexp.MustCompile("[^a-zA-Z0-9\\s]+")

	sanitized := strings.ToLower(re.ReplaceAllString(value, " "))
	for _, word := range strings.Fields(sanitized) {
		if len(word) == 0 {
			continue
		}
		emitter.Emit(word, strconv.Itoa(1))
	}
}

func (w wordCount) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	count := 0
	for _ = range values.Iter() {
		count++
	}
	emitter.Emit(key, strconv.Itoa(count))
}

func main() {
	job := corral.NewJob(wordCount{}, wordCount{})

	options := []corral.Option{
		corral.WithSplitSize(10 * 1024),
		corral.WithMapBinSize(10 * 1024),
	}

	useS3 := flag.Bool("s3", false, "use s3 as the backend")
	flag.Parse()

	if *useS3 {
		bucket := os.Getenv("AWS_TEST_BUCKET")
		options = append(options, corral.WithWorkingLocation(fmt.Sprintf("s3://%s", bucket)))
	}

	driver := corral.NewDriver(job, options...)
	driver.Main()
}
