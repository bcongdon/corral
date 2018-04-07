package corral

import (
	"context"
	"fmt"

	"github.com/bcongdon/corral/internal/pkg/backend"
)

var (
	currentJob *Job
)

func HandleRequest(ctx context.Context, task Task) (string, error) {
	fs := backend.InitFilesystem(task.FileSystemType, task.FileSystemLocation)
	currentJob.fileSystem = fs

	if task.Phase == MapPhase {
		err := currentJob.RunMapper(task.MapTask.MapperID, task.MapTask.Splits)
		return "", err
	} else if task.Phase == ReducePhase {
		err := currentJob.runReducer(0)
		return "", err
	}
	return "", fmt.Errorf("Unknown phase: %d", task.Phase)
}
