package corral

import (
	"github.com/bcongdon/corral/internal/pkg/corfs"
)

// Phase is a descriptor of the phase (i.e. Map or Reduce) of a Job
type Phase int

// Descriptors of the Job phase
const (
	MapPhase Phase = iota
	ReducePhase
)

// task defines a serialized description of a single unit of work
// in a MapReduce job, as well as the necessary information for a
// remote executor to initialize itself and begin working.
type task struct {
	JobNumber        int
	Phase            Phase
	BinID            uint
	IntermediateBins uint
	Splits           []inputSplit
	FileSystemType   corfs.FileSystemType
	WorkingLocation  string
	Cleanup          bool
}

type taskResult struct {
	BytesRead    int
	BytesWritten int
}
