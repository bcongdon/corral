package main

import (
	"fmt"
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
		err := emitter.Emit(word, strconv.Itoa(1))
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (w wordCount) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	count := 0
	for range values.Iter() {
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

	driver := corral.NewDriver(job, options...)
	driver.Main()
}
