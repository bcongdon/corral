package corral

import (
	"flag"
	"strings"
	"sync"

	"github.com/bcongdon/corral/internal/pkg/backend"
	log "github.com/sirupsen/logrus"
	pb "gopkg.in/cheggaaa/pb.v1"

	"github.com/aws/aws-lambda-go/lambda"
)

type Driver struct {
	job    *Job
	config *Config
}

type Config struct {
	Inputs             []string
	SplitSize          int64
	MapBinSize         int64
	ReduceBinSize      int64
	MaxConcurrency     int
	FileSystemType     backend.FileSystemType
	FileSystemLocation string
}

func newConfig() *Config {
	if !flag.Parsed() {
		flag.Parse()
	}
	return &Config{
		Inputs:             flag.Args(),
		SplitSize:          100 * 1024 * 1024, // Default input split size is 100Mb
		MapBinSize:         500 * 1024 * 1024, // Default map bin size is 500Mb
		ReduceBinSize:      500 * 1024 * 1024, // Default reduce bin size is 500Mb
		MaxConcurrency:     100,               // TODO: Not currently enforced
		FileSystemType:     backend.Local,
		FileSystemLocation: ".",
	}
}

type Option func(*Config)

// NewDriver creates a new Driver with the provided job and optional configuration
func NewDriver(job *Job, options ...Option) *Driver {
	d := &Driver{}

	c := newConfig()
	for _, f := range options {
		f(c)
	}

	if c.SplitSize > c.MapBinSize {
		log.Warn("Configured Split Size is larger than Map Bin size")
		c.SplitSize = c.MapBinSize
	}

	d.config = c
	d.job = job

	return d
}

func WithSplitSize(s int64) Option {
	return func(c *Config) {
		c.SplitSize = s
	}
}

func WithMapBinSize(s int64) Option {
	return func(c *Config) {
		c.MapBinSize = s
	}
}

func WithReduceBinSize(s int64) Option {
	return func(c *Config) {
		c.ReduceBinSize = s
	}
}

func WithWorkingLocation(location string) Option {
	return func(c *Config) {
		if strings.HasPrefix(location, "s3://") {
			c.FileSystemType = backend.S3
			location = strings.TrimPrefix(location, "s3://")
		}
		c.FileSystemLocation = location
	}
}

// run starts the Driver
func (d *Driver) run() {
	if runningInLambda() {
		currentJob = d.job
		lambda.Start(handleRequest)
	}

	d.job.fileSystem = backend.InitFilesystem(d.config.FileSystemType, d.config.FileSystemLocation)
	d.job.config = d.config

	var wg sync.WaitGroup
	inputSplits := d.job.inputSplits(d.config.Inputs, d.config.SplitSize)
	if len(inputSplits) == 0 {
		log.Warnf("No input splits")
		return
	}

	inputBins := packInputSplits(inputSplits, d.config.MapBinSize)
	bar := pb.New(len(inputBins)).Prefix("Map").Start()
	for binID, bin := range inputBins {
		wg.Add(1)
		go func(bID uint, b []inputSplit) {
			defer wg.Done()
			defer bar.Increment()
			d.job.runMapper(bID, b)
		}(uint(binID), bin)
	}
	wg.Wait()
	bar.Finish()

	// Reducer Phase
	bar = pb.New(int(d.job.intermediateBins)).Prefix("Reduce").Start()
	for binID := uint(0); binID < d.job.intermediateBins; binID++ {
		wg.Add(1)
		go func(bID uint) {
			defer wg.Done()
			defer bar.Increment()
			d.job.runReducer(bID)
		}(binID)
	}
	wg.Wait()
	bar.Finish()
}

// Main starts the Driver.
// TODO: more information about backends, config, etc.
func (d *Driver) Main() {
	d.run()
}
