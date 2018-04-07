package corral

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
)

type task struct {
	Phase       Phase
	InputSplits []inputSplit
	MapperID    int
	BinID       int
}
