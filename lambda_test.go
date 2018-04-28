package corral

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/spf13/viper"

	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/lambda/lambdaiface"

	"github.com/bcongdon/corral/internal/pkg/corfs"
	"github.com/bcongdon/corral/internal/pkg/corlambda"

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

func TestHandleRequest(t *testing.T) {
	testTask := task{
		JobNumber:        0,
		Phase:            MapPhase,
		BinID:            0,
		IntermediateBins: 10,
		Splits:           []inputSplit{},
		FileSystemType:   corfs.Local,
		WorkingLocation:  ".",
	}

	job := &Job{
		config: &config{},
	}

	// These values should be reset to 0 by Lambda handler function
	job.bytesRead = 10
	job.bytesWritten = 20

	lambdaDriver = NewDriver(job)

	output, err := handleRequest(context.Background(), testTask)
	assert.Nil(t, err)
	assert.Equal(t, "{\"BytesRead\":0,\"BytesWritten\":0}", output)

	testTask.Phase = ReducePhase
	output, err = handleRequest(context.Background(), testTask)
	assert.Nil(t, err)
	assert.Equal(t, "{\"BytesRead\":0,\"BytesWritten\":0}", output)
}

type mockLambdaClient struct {
	lambdaiface.LambdaAPI
	capturedPayload []byte
}

func (m *mockLambdaClient) Invoke(input *lambda.InvokeInput) (*lambda.InvokeOutput, error) {
	m.capturedPayload = input.Payload
	return &lambda.InvokeOutput{}, nil
}

func (*mockLambdaClient) GetFunction(*lambda.GetFunctionInput) (*lambda.GetFunctionOutput, error) {
	return nil, nil
}

func (*mockLambdaClient) CreateFunction(*lambda.CreateFunctionInput) (*lambda.FunctionConfiguration, error) {
	return nil, nil
}

func TestRunLambdaMapper(t *testing.T) {
	mock := &mockLambdaClient{}
	executor := &lambdaExecutor{
		&corlambda.LambdaClient{
			Client: mock,
		},
		nil,
		"FunctionName",
	}

	job := &Job{
		config: &config{WorkingLocation: "."},
	}
	err := executor.RunMapper(job, 0, 10, []inputSplit{})
	assert.Nil(t, err)

	var taskPayload task
	err = json.Unmarshal(mock.capturedPayload, &taskPayload)
	assert.Nil(t, err)

	assert.Equal(t, uint(10), taskPayload.BinID)
	assert.Equal(t, MapPhase, taskPayload.Phase)
}

func TestRunLambdaReducer(t *testing.T) {
	mock := &mockLambdaClient{}
	executor := &lambdaExecutor{
		&corlambda.LambdaClient{
			Client: mock,
		},
		nil,
		"FunctionName",
	}

	job := &Job{
		config: &config{WorkingLocation: "."},
	}
	err := executor.RunReducer(job, 0, 10)
	assert.Nil(t, err)

	var taskPayload task
	err = json.Unmarshal(mock.capturedPayload, &taskPayload)
	assert.Nil(t, err)

	assert.Equal(t, uint(10), taskPayload.BinID)
	assert.Equal(t, ReducePhase, taskPayload.Phase)
}

func TestDeployFunction(t *testing.T) {
	mock := &mockLambdaClient{}
	executor := &lambdaExecutor{
		&corlambda.LambdaClient{
			Client: mock,
		},
		nil,
		"FunctionName",
	}

	viper.SetDefault("lambdaManageRole", false) // Disable testing role deployment
	executor.Deploy()
}
