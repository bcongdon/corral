package corlambda

import (
	"crypto/sha256"
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/stretchr/testify/assert"
)

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
	// TODO:
}

func TestInvokeRetry(t *testing.T) {
	// TODO:
}
