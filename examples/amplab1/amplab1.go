package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bcongdon/corral"
)

const pageRankCutoff = 50

type amplab1 struct{}

func (a amplab1) Map(key, value string, emitter corral.Emitter) {
	fields := strings.Split(value, ",")
	if len(fields) != 3 {
		fmt.Printf("Invalid record: '%s'\n", value)
		return
	}

	pageURL := fields[0]
	pageRank, err := strconv.Atoi(fields[1])
	if err == nil && pageRank > pageRankCutoff {
		emitter.Emit(pageURL, fields[1])
	}
}

func (a amplab1) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	for value := range values.Iter() {
		emitter.Emit(key, value)
	}
}

func main() {
	job := corral.NewJob(amplab1{}, amplab1{})

	driver := corral.NewDriver(job)
	driver.Main()
}
