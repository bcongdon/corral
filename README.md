# 🐎 corral

> Serverless MapReduce

[![Build Status](https://travis-ci.org/bcongdon/corral.svg?branch=master)](https://travis-ci.org/bcongdon/corral)
[![Go Report Card](https://goreportcard.com/badge/github.com/bcongdon/corral)](https://goreportcard.com/report/github.com/bcongdon/corral)
[![codecov](https://codecov.io/gh/bcongdon/corral/branch/master/graph/badge.svg)](https://codecov.io/gh/bcongdon/corral)
[![GoDoc](https://godoc.org/github.com/bcongdon/corral?status.svg)](https://godoc.org/github.com/bcongdon/corral)

<p align="center">
    <img src="img/logo.svg" width="50%"/>
</p>

Corral is a MapReduce framework designed to be deployed to serverless platforms, like [AWS Lambda](https://aws.amazon.com/lambda/).
It presents a lightweight alternative to Hadoop MapReduce. Much of the design philosophy was inspired by Yelp's [mrjob](https://pythonhosted.org/mrjob/) --
corral retains mrjob's ease-of-use while gaining the type safety and speed of Go.

Corral's runtime model consists of stateless, transient executors controlled by a central driver. Currently, the best environment for deployment is AWS Lambda,
but corral is modular enough that support for other serverless platforms can be added as support for Go in cloud functions improves.

Corral is best suited for data-intensive but computationally inexpensive tasks, such as ETL jobs.


**Contents:**
---
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Examples](#examples)
- [Deploying in Lambda](#deploying-in-lambda)
  - [AWS Credentials](#aws-credentials)
- [Configuration](#configuration)
  - [Configuration Settings](#configuration-settings)
    - [Framework Settings](#framework-settings)
    - [Lambda Settings](#lambda-settings)
  - [Command Line Flags](#command-line-flags)
  - [Environment Variables](#environment-variables)
  - [Config Files](#config-files)
- [Architecture](#architecture)
  - [Input Files / Splits](#input-files--splits)
  - [Mappers](#mappers)
  - [Partition / Shuffle](#partition--shuffle)
  - [Reducers / Output](#reducers--output)
- [Contributing](#contributing)
  - [Running Tests](#running-tests)
- [License](#license)
- [Previous Work / Attributions](#previous-work--attributions)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Examples

Every good MapReduce framework needs a WordCount™ example. Here's how to write a "word count" in corral:

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
go run word_count.go --out s3://my-output-bucket/ s3://my-input-bucket/*
```

More comprehensive examples can be found in [the examples folder](https://github.com/bcongdon/corral/tree/master/examples).

## Deploying in Lambda

<p align="center">
    <img src="img/word_count.gif" width="100%"/>
</p>

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

There are a number of ways to specify configuraiton for corral applications. To hard-code configuration, there are a variety of [Options](https://godoc.org/github.com/bcongdon/corral#Option) that may be used when instantiating a Job.

Configuration values are used in the order, with priority given to whichever location is set first:

1. Hard-coded job [Options](https://godoc.org/github.com/bcongdon/corral#Option).
1. Command line flags
1. Environment variables
1. Configuration file
1. Default values

### Configuration Settings

Below are the config settings that may be changed. 

#### Framework Settings
* `splitSize` (int64) - The maximum size (in bytes) of any single file input split. (Default: 100Mb)
* `mapBinSize` (int64) - The maximum size (in bytes) of the combined input size to a mapper. (Default: 512Mb)
* `reduceBinSize` (int64) - The maximum size (in bytes) of the combined input size to a reducer. This is an "expected" maximum, assuming uniform key distribution. (Default: 512Mb)
* `maxConcurrency` (int) - The maximum number of executors (local, Lambda, or otherwise) that may run concurrently. (Default: `100`)
* `workingLocation` (string) - The location (local or S3) to use for writing intermediate and output data.
* `verbose` (bool) - Enables debug logging if set to `true`

#### Lambda Settings
* `lambdaFunctionName` (string) - The name to use for created Lambda functions. (Default: `corral_function`)
* `lambdaManageRole` (bool) - Whether corral should manage creating an IAM role for Lambda execution. (Default: `true`)
* `lambdaRoleARN` (string) - If `lambdaManageRole` is disabled, the ARN specified in `lambdaRoleARN` is used as the Lambda function's executor role.
* `lambdaTimeout` (int64) - The timeout (maximum function duration) in seconds of created Lambda functions. See [AWS lambda docs](https://docs.aws.amazon.com/lambda/latest/dg/resource-model.html) for details. (Default: `180`)
* `lambdaMemory` (int64) - The maximum memory that a Lambda function may use. See [AWS lambda docs](https://docs.aws.amazon.com/lambda/latest/dg/resource-model.html) for details. (Default: `1500`)

### Command Line Flags

The following flags are available at runtime as command-line flags:
```
      --lambda            Use lambda backend
      --memprofile file   Write memory profile to file
  -o, --out directory     Output directory (can be local or in S3)
      --undeploy          Undeploy the Lambda function and IAM permissions without running the driver
  -v, --verbose           Output verbose logs
```

### Environment Variables

Corral leverages [Viper](https://github.com/spf13/viper) for specifying config. Any of the above configuration settings can be set as environment variables by upper-casing the setting name, and prepending `CORRAL_`.

For example, `lambdaFunctionName` can be configured using an env var by setting `CORRAL_LAMBDAFUNCTIONNAME`.

### Config Files

Corral will read settings from a file called `corralrc`. Corral checks to see if this file exists in the current directory (`.`). It can also read global settings from `$HOME/.corral/corralrc`.

Reference the "Configuration Settings" section for the configuration keys that may be used.

Config files can be in JSON, YAML, or TOML format. See [Viper](https://github.com/spf13/viper) for more details.

## Architecture

Below is a high-level diagram describing the MapReduce architecture corral uses.

<p align="center">
    <img src="img/architecture.svg" width="80%"/>
</p>

### Input Files / Splits

Input files are split byte-wise into contiguous chunks of maximum size `splitSize`. These splits are packed into "input bins" of maximum size `mapBinSize`. The bin packing algorithm tries to assign contiguous chunks of a single file to the same mapper, but this behavior is not guaranteed.

There is a one-to-one correspondance between an "input bin" and the data that a mapper reads. i.e. Each mapper is assigned to process exactly 1 input bin. For jobs that run on Lambda, you should tune `mapBinSize`, `splitSize`, and `lambdaTimeout` accordingly so that mappers are able to process their entire input before timing out.

Input data is stramed into the mapper, so the entire input data needn't fit in memory.

### Mappers

Input data is fed into the map function line-by-line. Input splits are calculated byte-wise, but this is rectified during the Map phase into a logical split "by line" (to prevent partial reads, or the loss of records that span input splits).

Mappers may maintain state if desired (though not encouraged).

### Partition / Shuffle

Key/value pairs emitted during the map stage are written to intermediate files. Keys are partitioned into one `N` buckets, where `N` is the number of reducers. As a result, each mapper may write to as many as `N` separate files.

This results in a set of files labeled `map-binX-Y` where `X` is a number between 0 and N-1, and `Y` is the mapper's ID (a number between 0 and the number of mappers).

### Reducers / Output

Currently, reducer input must be able to fit in memory. This is because keys are only partitioned, not sorted. The reducer performs an in-memory per-key partition.

Reducers receive per-key values in an arbitrary order. It is guaranteed that all values for a given key will be provided in a single call to Reduce by-key.

Values emitted from a reducer will be stored in tab separated format (i.e. `KEY\tVALUE`) in files labeled `output-X` where `X` is the reducer's ID (a number between 0 and the number of reducers).

Reducers may maintain state if desired (though not encouraged).

## Contributing

Contributions to corral are more than welcomed! In general, the preference is to discuss potential changes in the issues before changes are made.

More information is included in the [CONTRIBUTING.md](CONTRIBUTING.md)

### Running Tests

To run tests, run the following command in the root project directory:

```
go test ./...
```

Note that some tests (i.e. the tests of `corfs`) require AWS credentials to be present.

The main corral has TravisCI setup. If you fork this repo, you can enable TravisCI on your fork. You will need to set the following environment variables for all the tests to work:

* `AWS_ACCESS_KEY_ID`: Credentials access key
* `AWS_SECRET_ACCESS_KEY`: Credentials secret key
* `AWS_DEFAULT_REGION`: Region to use for S3 tests
* `AWS_TEST_BUCKET`: The S3 bucket to use for tests (just the name; i.e. `testBucket` instead of `s3://testBucket`)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

## Previous Work / Attributions

- [lambda-refarch-mapreduce](https://github.com/awslabs/lambda-refarch-mapreduce) - Python/Node.JS reference MapReduce Architecture
    - Uses a "recursive" style reducer instead of parallel reducers
    - Requires that all reducer output can fit in memory of a single lambda function
- [mrjob](https://github.com/Yelp/mrjob)
    - Excellent Python library for writing MapReduce jobs for Hadoop, EMR/Dataproc, and others
- [dmrgo](https://github.com/dgryski/dmrgo)
    - mrjob-inspired Go MapReduce library
- [Zappa](https://github.com/Miserlou/Zappa)
	- Serverless Python toolkit. Inspired much of the way that corral does automatic Lambda deployment
- Logo: [Fence by Vitaliy Gorbachev from the Noun Project](https://thenounproject.com/search/?q=fence&i=1291185)
