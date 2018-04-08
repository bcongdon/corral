package corral

import (
	"context"
	"fmt"
	"os"

	"github.com/bcongdon/corral/internal/pkg/backend"
)

var (
	currentJob *Job
)

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

func handleRequest(ctx context.Context, task Task) (string, error) {
	fs := backend.InitFilesystem(task.FileSystemType, task.FileSystemLocation)
	currentJob.fileSystem = fs

	if task.Phase == MapPhase {
		err := currentJob.runMapper(task.MapTask.MapperID, task.MapTask.Splits)
		return "", err
	} else if task.Phase == ReducePhase {
		err := currentJob.runReducer(0)
		return "", err
	}
	return "", fmt.Errorf("Unknown phase: %d", task.Phase)
}
