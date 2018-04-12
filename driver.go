package corral

import (
	"flag"
	"os"
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
	job             *Job
	config          *config
	executor        executor
	inputFilesystem corfs.FileSystem
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
	return &config{
		Inputs:             []string{},
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
		executor: localExecutor{},
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

// WithMapBinSize sets the MapBinSize of the Driver
func WithMapBinSize(s int64) Option {
	return func(c *config) {
		c.MapBinSize = s
	}
}

// WithReduceBinSize sets the ReduceBinSize of the Driver
func WithReduceBinSize(s int64) Option {
	return func(c *config) {
		c.ReduceBinSize = s
	}
}

// WithWorkingLocation sets the location and filesystem backend of the Driver
func WithWorkingLocation(location string) Option {
	return func(c *config) {
		if strings.HasPrefix(location, "s3://") {
			c.FileSystemType = corfs.S3
			location = strings.TrimPrefix(location, "s3://")
		}
		c.FileSystemLocation = location
	}
}

func (d *Driver) runMapPhase() {
	var wg sync.WaitGroup
	inputSplits := d.job.inputSplits(d.config.Inputs, d.config.SplitSize)
	if len(inputSplits) == 0 {
		log.Warnf("No input splits")
		os.Exit(0)
	}
	log.Debugf("Calculated %d inputsplits", len(inputSplits))

	inputBins := packInputSplits(inputSplits, d.config.MapBinSize)
	bar := pb.New(len(inputBins)).Prefix("Map").Start()
	for binID, bin := range inputBins {
		wg.Add(1)
		go func(bID uint, b []inputSplit) {
			defer wg.Done()
			defer bar.Increment()
			err := d.executor.RunMapper(d.job, bID, b)
			if err != nil {
				log.Errorf("Error when running mapper %d: %s", bID, err)
			}
		}(uint(binID), bin)
	}
	wg.Wait()
	bar.Finish()
}

func (d *Driver) runReducePhase() {
	var wg sync.WaitGroup
	bar := pb.New(int(d.job.intermediateBins)).Prefix("Reduce").Start()
	for binID := uint(0); binID < d.job.intermediateBins; binID++ {
		wg.Add(1)
		go func(bID uint) {
			defer wg.Done()
			defer bar.Increment()
			err := d.executor.RunReducer(d.job, bID)
			if err != nil {
				log.Errorf("Error when running reducer %d: %s", bID, err)
			}
		}(binID)
	}
	wg.Wait()
	bar.Finish()
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

	log.Debugf("Initializing job filesystem")
	d.job.fileSystem = corfs.InitFilesystem(d.config.FileSystemType, d.config.FileSystemLocation)
	d.job.config = d.config

	d.runMapPhase()
	d.runReducePhase()
}

func (d *Driver) initInputFilesystem() {
	if len(d.config.Inputs) == 0 {
		return
	}
}

// Main starts the Driver.
// TODO: more information about backends, config, etc.
func (d *Driver) Main() {
	log.SetLevel(log.DebugLevel)

	lambda := flag.Bool("lambda", false, "Use lambda backend")
	flag.Parse()

	d.config.Inputs = flag.Args()
	if *lambda {
		d.executor = &lambdaExecutor{
			corlambda.NewLambdaClient(),
			"corral_test_function",
		}
	}

	d.run()
}
