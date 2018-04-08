package corral

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunningInLambda(t *testing.T) {
	res := runningInLambda()
	assert.False(t, res)

	for _, env := range []string{"LAMBDA_TASK_ROOT", "AWS_EXECUTION_ENV", "LAMBDA_RUNTIME_DIR"} {
		os.Setenv(env, "value")
	}

	res = runningInLambda()
	assert.True(t, res)
}
