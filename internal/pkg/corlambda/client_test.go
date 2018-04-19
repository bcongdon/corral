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

type mockLambda struct {
	lambdaiface.LambdaAPI
	invokeFailures int
	outputPayload  []byte
}

func (m *mockLambda) Invoke(*lambda.InvokeInput) (*lambda.InvokeOutput, error) {
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

func TestFunctionNeedsUpdate(t *testing.T) {
	functionCode := []byte("function code")
	codeHash := sha256.New()
	codeHash.Write(functionCode)
	codeHashDigest := base64.StdEncoding.EncodeToString(codeHash.Sum(nil))

	cfg := &lambda.FunctionConfiguration{CodeSha256: aws.String(codeHashDigest)}

	assert.True(t, functionNeedsUpdate([]byte("not function code"), cfg))
	assert.False(t, functionNeedsUpdate(functionCode, cfg))
}

func TestFunctionConfigNeedsUpdate(t *testing.T) {
	// TODO:
}

func TestInvoke(t *testing.T) {
	client := &LambdaClient{
		&mockLambda{
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
		&mockLambda{
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
		&mockLambda{
			invokeFailures: MaxLambdaRetries + 1,
		},
	}

	_, err := client.Invoke("function", []byte("payload"))
	assert.NotNil(t, err)
}
