# 🐎 corral: Serverless MapReduce

[![Build Status](https://travis-ci.org/bcongdon/corral.svg?branch=master)](https://travis-ci.org/bcongdon/corral)
[![Go Report Card](https://goreportcard.com/badge/github.com/bcongdon/corral)](https://goreportcard.com/report/github.com/bcongdon/corral)
[![codecov](https://codecov.io/gh/bcongdon/corral/branch/master/graph/badge.svg)](https://codecov.io/gh/bcongdon/corral)
[![GoDoc](https://godoc.org/github.com/bcongdon/corral?status.svg)](https://godoc.org/github.com/bcongdon/corral)

(Nothing to see here yet...)

## Examples

Every good MapReduce framework needs a WordCount™ example. Here's how to write word count in corral:

```golang
type wordCount struct{}

func (w wordCount) Map(key, value string, emitter corral.Emitter) {
	for _, word := range strings.Fields(value) {
		emitter.Emit(word, "")
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
    wc := wordCount{}
	job := corral.NewJob(wc, wc)

	driver := corral.NewDriver(job)
	driver.Main()
}
```

This can be invoked locally by building/running the above source and adding input files as arguments:

```sh
go run word_count.go /path/to/some_file.txt
```

By default, job output will be stored relative to the current directory.

We can also input/output to S3 by pointing to an S3 bucket/files for input/output:
```
go run word_count.go s3://my-input-bucket/* --out s3://my-output-bucket/
```

More comprehensive examples can be found in [the examples folder](https://github.com/bcongdon/corral/tree/master/examples).

## Deploying in Lambda

### AWS Credentials

## Configuration

## Architecture


## Previous Work / Attributions

- [lambda-refarch-mapreduce](https://github.com/awslabs/lambda-refarch-mapreduce) - Python/Node.JS reference MapReduce Architecture
    - Uses a "recursive" style reducer instead of parallel reducers
    - Requires that all reducer output can fit in memory of a single lambda function
- [dmrgo](https://github.com/dgryski/dmrgo)
    - TODO:
- [mrjob](https://github.com/Yelp/mrjob)
    - TODO: