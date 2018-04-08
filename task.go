package corral

import "github.com/bcongdon/corral/internal/pkg/backend"

// Phase is a descriptor of the phase (i.e. Map or Reduce) of a Job
type Phase int

// Descriptors of the Job phase
const (
	MapPhase Phase = iota
	ReducePhase
)

type MapTask struct {
	MapperID   uint
	Splits     []inputSplit
	FileSystem backend.FileSystem
}

// Task defines a serializable description of a single unit of work
// in a MapReduce job, as well as the necessary information for a
// remote executor to initialize itself and begin working.
type Task struct {
	Phase              Phase
	MapTask            MapTask
	ReduceTask         interface{}
	FileSystemType     backend.FileSystemType
	FileSystemLocation string
}
