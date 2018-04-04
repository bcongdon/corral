package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bcongdon/corral"
)

type wordCount struct{}

func (w wordCount) Map(key, value string, emitter corral.Emitter) {
	fmt.Println("Inside mapper")
	for _, word := range strings.Fields(value) {
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
	wc := wordCount{}

	job := corral.Job{
		Map:    wc,
		Reduce: wc,
	}

	job.Main()
}
