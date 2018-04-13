package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bcongdon/corral"
)

const subStrX = 8

type amplab2 struct{}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (a amplab2) Map(key, value string, emitter corral.Emitter) {
	fields := strings.Split(value, ",")
	if len(fields) != 9 {
		fmt.Printf("Invalid record: '%s'\n", value)
		return
	}

	sourceIP := fields[0]
	adRevenue := fields[3]
	emitter.Emit(sourceIP[:min(subStrX, len(sourceIP))], adRevenue)
}

func (a amplab2) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	totalRevenue := 0.0
	for value := range values.Iter() {
		adRevenue, err := strconv.ParseFloat(value, 64)
		if err == nil {
			totalRevenue += adRevenue
		}
	}
	emitter.Emit(key, fmt.Sprintf("%f", totalRevenue))
}

func main() {
	job := corral.NewJob(amplab2{}, amplab2{})

	driver := corral.NewDriver(job)
	driver.Main()
}
