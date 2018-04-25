# 🐎 corral

> Serverless MapReduce

[![Build Status](https://travis-ci.org/bcongdon/corral.svg?branch=master)](https://travis-ci.org/bcongdon/corral)
[![Go Report Card](https://goreportcard.com/badge/github.com/bcongdon/corral)](https://goreportcard.com/report/github.com/bcongdon/corral)
[![codecov](https://codecov.io/gh/bcongdon/corral/branch/master/graph/badge.svg)](https://codecov.io/gh/bcongdon/corral)
[![GoDoc](https://godoc.org/github.com/bcongdon/corral?status.svg)](https://godoc.org/github.com/bcongdon/corral)

<p align="center">
    <img src="logo.svg" width="50%"/>
</p>

**[WIP] This project is still very much a work-in-progress**

Corral is a MapReduce framework designed to be deployed to serverless platforms, like [AWS Lambda](https://aws.amazon.com/lambda/).
It presents a lightweight alternative to Hadoop MapReduce. Much of the design philosophy was inspired by Yelp's [mrjob](https://pythonhosted.org/mrjob/) --
corral retains mrjob's ease-of-use while gaining the type safety and speed of Go.

Corral's runtime model consists of stateless, transient executors controlled by a central driver. Currently, the best environment for deployment is AWS Lambda,
but corral is modular enough that support for other serverless platforms can be added as support for Go in cloud functions improves.

Corral is best suited for data-intensive but computationally inexpensive tasks, such as ETL jobs.

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

No formal deployment step needs run to deploy a corral application to Lambda. Instead, add the `--lambda` flag to an invocation of a corral app, and the project code will be automatically recompiled for Lambda and uploaded.

For example, 
```
./word_count --lambda s3://my-input-bucket/* --out s3://my-output-bucket
```

Note that you must use `s3` for input/output directories, as local data files will not be present in the Lambda environment.

**NOTE**: Due to the fact that corral recompiles application code to target Lambda, invocation of the command with the `--lambda` flag must be done in the root directory of your application's source code.

### AWS Credentials

AWS credentials are automatically loaded from the environment. See [this page](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/sessions.html) for details.

As per the AWS documentation, AWS credentials are loaded in order from:

1. Environment variables
1. Shared credentials file
1. IAM role (if executing in AWS Lambda or EC2)

In short, setup credentials in `.aws/credentials` as one would with any other AWS powered service. If you have more than one profile in `.aws/credentials`, make sure to set the `AWS_PROFILE` environment variable to select the profile to be used.

## Configuration

## Architecture

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

## Previous Work / Attributions

- [lambda-refarch-mapreduce](https://github.com/awslabs/lambda-refarch-mapreduce) - Python/Node.JS reference MapReduce Architecture
    - Uses a "recursive" style reducer instead of parallel reducers
    - Requires that all reducer output can fit in memory of a single lambda function
- [mrjob](https://github.com/Yelp/mrjob)
    - TODO:
- [dmrgo](https://github.com/dgryski/dmrgo)
    - TODO:
- Logo: [Fence by Vitaliy Gorbachev from the Noun Project](https://thenounproject.com/search/?q=fence&i=1291185)