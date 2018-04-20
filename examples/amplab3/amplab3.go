package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/bcongdon/corral"
)

type amplab3Join struct{}
type amplab3Aggregate struct{}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

const (
	rankingType = iota
	visitType
)

type Record struct {
	RecordType int
	PageURL    string
	PageRank   int
	DestURL    string
	AdRevenue  float64
	SourceIP   string
}

func (a amplab3Join) Map(key, value string, emitter corral.Emitter) {
	fields := strings.Split(value, ",")

	switch len(fields) {
	case 3: // Rankings Record
		pageRank, _ := strconv.Atoi(fields[1])
		ranking := Record{
			RecordType: rankingType,
			PageURL:    fields[0],
			PageRank:   pageRank,
		}
		emitRecord(ranking.PageURL, ranking, emitter)
	case 9: // Visits record
		adRevenue, _ := strconv.ParseFloat(fields[3], 64)
		visit := Record{
			RecordType: visitType,
			DestURL:    fields[1],
			AdRevenue:  adRevenue,
			SourceIP:   fields[0],
		}
		emitRecord(visit.DestURL, visit, emitter)
	default:
		fmt.Printf("Invalid record: '%s'\n", value)
		return
	}
}

func emitRecord(key string, record Record, emitter corral.Emitter) error {
	payload, _ := json.Marshal(record)
	return emitter.Emit(key, string(payload))
}

func (a amplab3Join) Reduce(URL string, values corral.ValueIterator, emitter corral.Emitter) {
	bufferedVisits := make([]Record, 0)
	var matchingRank *Record

	for value := range values.Iter() {
		var record Record
		json.Unmarshal([]byte(value), &record)

		if record.RecordType == rankingType {
			matchingRank = &record
			for _, visit := range bufferedVisits {
				visit.PageRank = matchingRank.PageRank
				emitRecord(visit.SourceIP, visit, emitter)
			}
			bufferedVisits = nil
		} else if matchingRank != nil {
			record.PageRank = matchingRank.PageRank
			emitRecord(record.SourceIP, record, emitter)
		} else {
			bufferedVisits = append(bufferedVisits, record)
		}
	}
}

func (amplab3Aggregate) Map(key, value string, emitter corral.Emitter) {
	emitter.Emit(key, value)
}

func (amplab3Aggregate) Reduce(sourceIP string, values corral.ValueIterator, emitter corral.Emitter) {
	sumPageRank := 0
	sumAdRevenue := 0.0
	count := 0

	for value := range values.Iter() {
		var record Record
		json.Unmarshal([]byte(value), &record)

		sumPageRank += record.PageRank
		sumAdRevenue += record.AdRevenue
		count++
	}

	avgPageRank := float64(sumPageRank) / float64(count)
	avgAdRevenue := sumAdRevenue / float64(count)
	emitter.Emit(sourceIP, fmt.Sprintf("%f\t%f", avgPageRank, avgAdRevenue))
}

func main() {
	job1 := corral.NewJob(amplab3Join{}, amplab3Join{})
	job2 := corral.NewJob(amplab3Aggregate{}, amplab3Aggregate{})

	driver := corral.NewMultiStageDriver([]*corral.Job{job1, job2})
	driver.Main()
}
