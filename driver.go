package corral

import (
	"flag"
	"os"
	"sync"

	"github.com/bcongdon/corral/internal/pkg/backend"

	"github.com/aws/aws-lambda-go/lambda"
)

type Driver struct {
	job    *Job
	config *Config
}

type Config struct {
	Inputs             []string
	MaxSplitSize       int64
	MaxInputBinSize    int64
	MaxConcurrency     int
	FileSystemType     string
	FileSystemLocation string

	intermediateBins uint
}

func newConfig() *Config {
	if !flag.Parsed() {
		flag.Parse()
	}
	return &Config{
		Inputs:             flag.Args(),
		MaxSplitSize:       100 * 1024 * 1024,
		MaxInputBinSize:    500 * 1024 * 1024,
		MaxConcurrency:     100,
		FileSystemType:     backend.Local,
		FileSystemLocation: ".",
		intermediateBins:   100,
	}
}

func NewDriver(job *Job, options ...func(*Config)) *Driver {
	d := &Driver{}

	c := newConfig()
	for _, f := range options {
		f(c)
	}

	d.config = c
	d.job = job

	return d
}

func runningInLambda() bool {
	expectedEnvVars := []string{"LAMBDA_TASK_ROOT", "AWS_EXECUTION_ENV", "LAMBDA_RUNTIME_DIR"}
	for _, envVar := range expectedEnvVars {
		if os.Getenv(envVar) == "" {
			return false
		}
	}
	return true
}

func MaxSplitSize(m int64) func(*Config) {
	return func(c *Config) {
		c.MaxSplitSize = m
	}
}

func (d *Driver) run() {
	if runningInLambda() {
		currentJob = d.job
		lambda.Start(HandleRequest)
	}

	d.job.fileSystem = backend.InitFilesystem(d.config.FileSystemType, d.config.FileSystemLocation)
	d.job.config = d.config

	var wg sync.WaitGroup
	inputSplits := d.job.inputSplits(d.config.Inputs, d.config.MaxSplitSize)

	// Mapper Phase
	inputBins := packInputSplits(inputSplits, d.config.MaxInputBinSize)
	for binID, bin := range inputBins {
		wg.Add(1)
		go func(bID uint, b []inputSplit) {
			defer wg.Done()
			d.job.RunMapper(bID, b)
		}(uint(binID), bin)
	}
	wg.Wait()

	// Reducer Phase
	for binID := uint(0); binID < d.config.intermediateBins; binID++ {
		wg.Add(1)
		go func(bID uint) {
			defer wg.Done()
			d.job.runReducer(bID)
		}(binID)
	}
	wg.Wait()
}

func (d *Driver) Main() {
	d.run()
}
