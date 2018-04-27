package corral

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/spf13/viper"

	"golang.org/x/sync/semaphore"

	log "github.com/sirupsen/logrus"
	pb "gopkg.in/cheggaaa/pb.v1"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/bcongdon/corral/internal/pkg/corfs"
	flag "github.com/spf13/pflag"
)

// Driver controls the execution of a MapReduce Job
type Driver struct {
	jobs     []*Job
	config   *config
	executor executor
}

// config configures a Driver's execution of jobs
type config struct {
	Inputs          []string
	SplitSize       int64
	MapBinSize      int64
	ReduceBinSize   int64
	MaxConcurrency  int
	WorkingLocation string
	Cleanup         bool
}

func newConfig() *config {
	loadConfig() // Load viper config from settings file(s) and environment

	// Register command line flags
	flag.Parse()
	viper.BindPFlags(flag.CommandLine)

	return &config{
		Inputs:          []string{},
		SplitSize:       viper.GetInt64("splitSize"),
		MapBinSize:      viper.GetInt64("mapBinSize"),
		ReduceBinSize:   viper.GetInt64("reduceBinSize"),
		MaxConcurrency:  viper.GetInt("maxConcurrency"),
		WorkingLocation: viper.GetString("workingLocation"),
		Cleanup:         viper.GetBool("cleanup"),
	}
}

// Option allows configuration of a Driver
type Option func(*config)

// NewDriver creates a new Driver with the provided job and optional configuration
func NewDriver(job *Job, options ...Option) *Driver {
	d := &Driver{
		jobs:     []*Job{job},
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
	log.Debugf("Loaded config: %#v", c)

	return d
}

// NewMultiStageDriver creates a new Driver with the provided jobs and optional configuration
func NewMultiStageDriver(jobs []*Job, options ...Option) *Driver {
	driver := NewDriver(nil, options...)
	driver.jobs = jobs
	return driver
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
		c.WorkingLocation = location
	}
}

// WithInputs specifies job inputs (i.e. input files/directories)
func WithInputs(inputs ...string) Option {
	return func(c *config) {
		c.Inputs = append(c.Inputs, inputs...)
	}
}

func (d *Driver) runMapPhase(job *Job, jobNumber int, inputs []string) {
	inputSplits := job.inputSplits(inputs, d.config.SplitSize)
	if len(inputSplits) == 0 {
		log.Warnf("No input splits")
		os.Exit(0)
	}
	log.Debugf("Number of job input splits: %d", len(inputSplits))

	inputBins := packInputSplits(inputSplits, d.config.MapBinSize)
	log.Debugf("Number of job input bins: %d", len(inputBins))
	bar := pb.New(len(inputBins)).Prefix("Map").Start()

	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(d.config.MaxConcurrency))
	for binID, bin := range inputBins {
		sem.Acquire(context.Background(), 1)
		wg.Add(1)
		go func(bID uint, b []inputSplit) {
			defer wg.Done()
			defer sem.Release(1)
			defer bar.Increment()
			err := d.executor.RunMapper(job, jobNumber, bID, b)
			if err != nil {
				log.Errorf("Error when running mapper %d: %s", bID, err)
			}
		}(uint(binID), bin)
	}
	wg.Wait()
	bar.Finish()
}

func (d *Driver) runReducePhase(job *Job, jobNumber int) {
	var wg sync.WaitGroup
	bar := pb.New(int(job.intermediateBins)).Prefix("Reduce").Start()
	for binID := uint(0); binID < job.intermediateBins; binID++ {
		wg.Add(1)
		go func(bID uint) {
			defer wg.Done()
			defer bar.Increment()
			err := d.executor.RunReducer(job, jobNumber, bID)
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
		lambdaDriver = d
		lambda.Start(handleRequest)
	}
	if lBackend, ok := d.executor.(*lambdaExecutor); ok {
		lBackend.Deploy()
	}

	if len(d.config.Inputs) == 0 {
		log.Error("No inputs!")
		os.Exit(1)
	}

	inputs := d.config.Inputs
	for idx, job := range d.jobs {
		// Initialize job filesystem
		job.fileSystem = corfs.InferFilesystem(inputs[0])

		jobWorkingLoc := d.config.WorkingLocation
		log.Infof("Starting job%d (%d/%d)", idx, idx+1, len(d.jobs))

		if len(d.jobs) > 1 {
			jobWorkingLoc = job.fileSystem.Join(jobWorkingLoc, fmt.Sprintf("job%d", idx))
		}
		job.outputPath = jobWorkingLoc

		*job.config = *d.config
		d.runMapPhase(job, idx, inputs)
		d.runReducePhase(job, idx)

		// Set inputs of next job to be outputs of current job
		inputs = []string{job.fileSystem.Join(jobWorkingLoc, "output-*")}

		log.Infof("Job %d - Total Bytes Read:\t%s", idx, humanize.Bytes(uint64(job.bytesRead)))
		log.Infof("Job %d - Total Bytes Written:\t%s", idx, humanize.Bytes(uint64(job.bytesWritten)))
	}
}

var lambdaFlag = flag.Bool("lambda", false, "Use lambda backend")
var outputDir = flag.StringP("out", "o", "", "Output `directory` (can be local or in S3)")
var memprofile = flag.String("memprofile", "", "Write memory profile to `file`")
var verbose = flag.BoolP("verbose", "v", false, "Output verbose logs")
var undeploy = flag.Bool("undeploy", false, "Undeploy the Lambda function and IAM permissions without running the driver")

// Main starts the Driver, running the submitted jobs.
func (d *Driver) Main() {
	if viper.GetBool("verbose") {
		log.SetLevel(log.DebugLevel)
	}

	if *undeploy {
		lambda := newLambdaExecutor(viper.GetString("lambdaFunctionName"))
		lambda.Undeploy()
		return
	}

	d.config.Inputs = append(d.config.Inputs, flag.Args()...)
	if *lambdaFlag {
		d.executor = newLambdaExecutor(viper.GetString("lambdaFunctionName"))
	}

	if *outputDir != "" {
		d.config.WorkingLocation = *outputDir
	}

	start := time.Now()
	d.run()
	end := time.Now()
	fmt.Printf("Job Execution Time: %s\n", end.Sub(start))

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
}
