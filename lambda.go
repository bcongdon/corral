package corral

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/bcongdon/corral/internal/pkg/corfs"
	"github.com/bcongdon/corral/internal/pkg/coriam"
	"github.com/bcongdon/corral/internal/pkg/corlambda"
)

var (
	lambdaDriver *Driver
)

// corralRoleName is the name to use when deploying an IAM role
const corralRoleName = "CorralExecutionRole"

// runningInLambda infers if the program is running in AWS lambda via inspection of the environment
func runningInLambda() bool {
	expectedEnvVars := []string{"LAMBDA_TASK_ROOT", "AWS_EXECUTION_ENV", "LAMBDA_RUNTIME_DIR"}
	for _, envVar := range expectedEnvVars {
		if os.Getenv(envVar) == "" {
			return false
		}
	}
	return true
}

func prepareResult(job *Job) string {
	result := taskResult{
		BytesRead:    int(job.bytesRead),
		BytesWritten: int(job.bytesWritten),
	}

	payload, _ := json.Marshal(result)
	return string(payload)
}

func handleRequest(ctx context.Context, task task) (string, error) {
	// Precaution to avoid running out of memory for reused Lambdas
	debug.FreeOSMemory()

	// Setup current job
	fs := corfs.InitFilesystem(task.FileSystemType)
	currentJob := lambdaDriver.jobs[task.JobNumber]
	currentJob.fileSystem = fs
	currentJob.intermediateBins = task.IntermediateBins
	currentJob.outputPath = task.WorkingLocation
	currentJob.config.Cleanup = task.Cleanup

	// Need to reset job counters in case this is a reused lambda
	currentJob.bytesRead = 0
	currentJob.bytesWritten = 0

	if task.Phase == MapPhase {
		err := currentJob.runMapper(task.BinID, task.Splits)
		return prepareResult(currentJob), err
	} else if task.Phase == ReducePhase {
		err := currentJob.runReducer(task.BinID)
		return prepareResult(currentJob), err
	}
	return "", fmt.Errorf("Unknown phase: %d", task.Phase)
}

type lambdaExecutor struct {
	*corlambda.LambdaClient
	*coriam.IAMClient
	functionName string
}

func newLambdaExecutor(functionName string) *lambdaExecutor {
	return &lambdaExecutor{
		LambdaClient: corlambda.NewLambdaClient(),
		IAMClient:    coriam.NewIAMClient(),
		functionName: functionName,
	}
}

func loadTaskResult(payload []byte) taskResult {
	// Unescape JSON string
	payloadStr, _ := strconv.Unquote(string(payload))

	var result taskResult
	err := json.Unmarshal([]byte(payloadStr), &result)
	if err != nil {
		log.Errorf("%s", err)
	}
	return result
}

func (l *lambdaExecutor) RunMapper(job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error {
	mapTask := task{
		JobNumber:        jobNumber,
		Phase:            MapPhase,
		BinID:            binID,
		Splits:           inputSplits,
		IntermediateBins: job.intermediateBins,
		FileSystemType:   corfs.S3,
		WorkingLocation:  job.outputPath,
	}
	payload, err := json.Marshal(mapTask)
	if err != nil {
		return err
	}

	resultPayload, err := l.Invoke(l.functionName, payload)
	taskResult := loadTaskResult(resultPayload)

	atomic.AddInt64(&job.bytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.bytesWritten, int64(taskResult.BytesWritten))

	return err
}

func (l *lambdaExecutor) RunReducer(job *Job, jobNumber int, binID uint) error {
	mapTask := task{
		JobNumber:       jobNumber,
		Phase:           ReducePhase,
		BinID:           binID,
		FileSystemType:  corfs.S3,
		WorkingLocation: job.outputPath,
		Cleanup:         job.config.Cleanup,
	}
	payload, err := json.Marshal(mapTask)
	if err != nil {
		return err
	}

	resultPayload, err := l.Invoke(l.functionName, payload)
	taskResult := loadTaskResult(resultPayload)

	atomic.AddInt64(&job.bytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.bytesWritten, int64(taskResult.BytesWritten))

	return err
}

func (l *lambdaExecutor) Deploy() {
	var roleARN string
	var err error
	if viper.GetBool("lambdaManageRole") {
		roleARN, err = l.DeployPermissions(corralRoleName)
		if err != nil {
			panic(err)
		}
	} else {
		roleARN = viper.GetString("lambdaRoleARN")
	}

	config := &corlambda.FunctionConfig{
		Name:       l.functionName,
		RoleARN:    roleARN,
		Timeout:    viper.GetInt64("lambdaTimeout"),
		MemorySize: viper.GetInt64("lambdaMemory"),
	}
	err = l.DeployFunction(config)
	if err != nil {
		panic(err)
	}
}

func (l *lambdaExecutor) Undeploy() {
	log.Info("Undeploying function")
	err := l.LambdaClient.DeleteFunction(l.functionName)
	if err != nil {
		log.Errorf("Error when undeploying function: %s", err)
	}

	log.Info("Undeploying IAM Permissions")
	err = l.IAMClient.DeletePermissions(corralRoleName)
	if err != nil {
		log.Errorf("Error when undeploying IAM permissions: %s", err)
	}
}
