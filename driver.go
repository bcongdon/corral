package corral

import (
	"flag"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	pb "gopkg.in/cheggaaa/pb.v1"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/bcongdon/corral/internal/pkg/corfs"
	"github.com/bcongdon/corral/internal/pkg/corlambda"
)

// Driver controls the execution of a MapReduce Job
type Driver struct {
	job      *Job
	config   *config
	executor executor
}

// config configures a Driver's execution of jobs
type config struct {
	Inputs             []string
	SplitSize          int64
	MapBinSize         int64
	ReduceBinSize      int64
	MaxConcurrency     int
	FileSystemType     corfs.FileSystemType
	FileSystemLocation string
}

func newConfig() *config {
	if !flag.Parsed() {
		flag.Parse()
	}
	return &config{
		Inputs:             flag.Args(),
		SplitSize:          100 * 1024 * 1024, // Default input split size is 100Mb
		MapBinSize:         500 * 1024 * 1024, // Default map bin size is 500Mb
		ReduceBinSize:      500 * 1024 * 1024, // Default reduce bin size is 500Mb
		MaxConcurrency:     100,               // TODO: Not currently enforced
		FileSystemType:     corfs.Local,
		FileSystemLocation: ".",
	}
}

type Option func(*config)

// NewDriver creates a new Driver with the provided job and optional configuration
func NewDriver(job *Job, options ...Option) *Driver {
	d := &Driver{
		job:      job,
		executor: &lambdaExecutor{corlambda.NewLambdaClient(), "corral_test_function"},
	}

	c := newConfig()
	for _, f := range options {
		f(c)
	}

	if c.SplitSize > c.MapBinSize {
		log.Warn("Configured Split Size is larger than Map Bin size")
		c.SplitSize = c.MapBinSize
	}

	d.config = c

	return d
}

// WithSplitSize sets the SplitSize of the Driver
func WithSplitSize(s int64) Option {
	return func(c *config) {
		c.SplitSize = s
	}
}

// WithSplitSize sets the MapBinSize of the Driver
func WithMapBinSize(s int64) Option {
	return func(c *config) {
		c.MapBinSize = s
	}
}

// WithSplitSize sets the ReduceBinSize of the Driver
func WithReduceBinSize(s int64) Option {
	return func(c *config) {
		c.ReduceBinSize = s
	}
}

// WithSplitSize sets the location and filesystem backend of the Driver
func WithWorkingLocation(location string) Option {
	return func(c *config) {
		if strings.HasPrefix(location, "s3://") {
			c.FileSystemType = corfs.S3
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

	if lBackend, ok := d.executor.(*lambdaExecutor); ok {
		lBackend.Deploy()
	}

	d.job.fileSystem = corfs.InitFilesystem(d.config.FileSystemType, d.config.FileSystemLocation)
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
			d.executor.RunMapper(d.job, bID, b)
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
			d.executor.RunReducer(d.job, bID)
		}(binID)
	}
	wg.Wait()
	bar.Finish()
}

// Main starts the Driver.
// TODO: more information about backends, config, etc.
func (d *Driver) Main() {
	log.SetLevel(log.DebugLevel)
	d.run()
}
