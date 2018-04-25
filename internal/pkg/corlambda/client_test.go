package corlambda

import (
	"crypto/sha256"
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go/service/lambda/lambdaiface"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/stretchr/testify/assert"
)

type lambdaInvokerMock struct {
	lambdaiface.LambdaAPI
	invokeFailures int
	outputPayload  []byte
}

func (m *lambdaInvokerMock) Invoke(*lambda.InvokeInput) (*lambda.InvokeOutput, error) {
	if m.invokeFailures > 0 {
		m.invokeFailures--
		return &lambda.InvokeOutput{
			FunctionError: aws.String("error"),
		}, nil
	}
	return &lambda.InvokeOutput{
		Payload: m.outputPayload,
	}, nil
}

type lambdaDeployMock struct {
	lambdaiface.LambdaAPI
	getFunctionOutput                 *lambda.GetFunctionOutput
	capturedCreateFunctionInput       *lambda.CreateFunctionInput
	capturedUpdateFunctionCodeInput   *lambda.UpdateFunctionCodeInput
	capturedUpdateFunctionConfigInput *lambda.UpdateFunctionConfigurationInput
	capturedDeleteFunctionInput       *lambda.DeleteFunctionInput
}

func (d *lambdaDeployMock) GetFunction(*lambda.GetFunctionInput) (*lambda.GetFunctionOutput, error) {
	return d.getFunctionOutput, nil
}

func (d *lambdaDeployMock) CreateFunction(input *lambda.CreateFunctionInput) (*lambda.FunctionConfiguration, error) {
	d.capturedCreateFunctionInput = input
	return nil, nil
}

func (d *lambdaDeployMock) UpdateFunctionCode(input *lambda.UpdateFunctionCodeInput) (*lambda.FunctionConfiguration, error) {
	d.capturedUpdateFunctionCodeInput = input
	return nil, nil
}

func (d *lambdaDeployMock) UpdateFunctionConfiguration(input *lambda.UpdateFunctionConfigurationInput) (*lambda.FunctionConfiguration, error) {
	d.capturedUpdateFunctionConfigInput = input
	return nil, nil
}

func (d *lambdaDeployMock) DeleteFunction(input *lambda.DeleteFunctionInput) (*lambda.DeleteFunctionOutput, error) {
	d.capturedDeleteFunctionInput = input
	return nil, nil
}

func TestFunctionNeedsUpdate(t *testing.T) {
	functionCode := []byte("function code")
	codeHash := sha256.New()
	codeHash.Write(functionCode)
	codeHashDigest := base64.StdEncoding.EncodeToString(codeHash.Sum(nil))

	cfg := &lambda.FunctionConfiguration{CodeSha256: aws.String(codeHashDigest)}

	assert.True(t, functionNeedsUpdate([]byte("not function code"), cfg))
	assert.False(t, functionNeedsUpdate(functionCode, cfg))
}

func TestInvoke(t *testing.T) {
	client := &LambdaClient{
		&lambdaInvokerMock{
			invokeFailures: 0,
			outputPayload:  []byte("payload"),
		},
	}

	output, err := client.Invoke("function", []byte("payload"))
	assert.Nil(t, err)

	assert.Equal(t, []byte("payload"), output)
}

func TestInvokeRetry(t *testing.T) {
	client := &LambdaClient{
		&lambdaInvokerMock{
			invokeFailures: 2,
			outputPayload:  []byte("payload"),
		},
	}

	output, err := client.Invoke("function", []byte("payload"))
	assert.Nil(t, err)

	assert.Equal(t, []byte("payload"), output)
}

func TestInvokeOutOfTries(t *testing.T) {
	client := &LambdaClient{
		&lambdaInvokerMock{
			invokeFailures: MaxLambdaRetries + 1,
		},
	}

	_, err := client.Invoke("function", []byte("payload"))
	assert.NotNil(t, err)
}

func TestCreateFunction(t *testing.T) {
	mock := &lambdaDeployMock{}
	client := &LambdaClient{mock}

	config := &FunctionConfig{
		Name:       "test function",
		RoleARN:    "testARN",
		Timeout:    10,
		MemorySize: 1000,
	}

	err := client.DeployFunction(config)
	assert.Nil(t, err)

	assert.Equal(t, "test function", *mock.capturedCreateFunctionInput.FunctionName)
	assert.Equal(t, "testARN", *mock.capturedCreateFunctionInput.Role)
	assert.Equal(t, int64(10), *mock.capturedCreateFunctionInput.Timeout)
	assert.Equal(t, int64(1000), *mock.capturedCreateFunctionInput.MemorySize)
}

func TestUpdateFunction(t *testing.T) {
	mock := &lambdaDeployMock{
		getFunctionOutput: &lambda.GetFunctionOutput{
			Configuration: &lambda.FunctionConfiguration{
				CodeSha256: aws.String("sha"),
				Role:       aws.String("wrongARN"),
				Timeout:    aws.Int64(10),
				MemorySize: aws.Int64(1000),
			},
		},
	}
	client := &LambdaClient{mock}

	config := &FunctionConfig{
		Name:       "test function",
		RoleARN:    "testARN",
		Timeout:    10,
		MemorySize: 1000,
	}

	err := client.DeployFunction(config)
	assert.Nil(t, err)

	assert.NotNil(t, mock.capturedUpdateFunctionCodeInput)
	assert.NotNil(t, mock.capturedUpdateFunctionCodeInput.ZipFile)
	assert.NotNil(t, mock.capturedUpdateFunctionCodeInput)
	assert.Equal(t, "testARN", *mock.capturedUpdateFunctionConfigInput.Role)
}

func TestDeleteFunction(t *testing.T) {
	mock := &lambdaDeployMock{}

	client := &LambdaClient{mock}

	err := client.DeleteFunction("function")
	assert.Nil(t, err)

	assert.Equal(t, "function", *mock.capturedDeleteFunctionInput.FunctionName)
}
