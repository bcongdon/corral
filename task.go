package corral

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
)

type Task struct {
	Phase              Phase
	MapTask            MapTask
	ReduceTask         interface{}
	FileSystemType     string
	FileSystemLocation string
}
