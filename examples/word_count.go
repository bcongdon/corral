package main

import (
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

	job.Main()
}
