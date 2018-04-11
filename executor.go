package corral

type executor interface {
	RunMapper(job *Job, binID uint, inputSplits []inputSplit) error
	RunReducer(job *Job, binID uint) error
}

type localExecutor struct{}

func (localExecutor) RunMapper(job *Job, binID uint, inputSplits []inputSplit) error {
	return job.runMapper(binID, inputSplits)
}

func (localExecutor) RunReducer(job *Job, binID uint) error {
	return job.runReducer(binID)
}
