package corlambda

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/dustin/go-humanize"

	lambdaMessages "github.com/aws/aws-lambda-go/lambda/messages"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	log "github.com/sirupsen/logrus"
)

// MaxLambdaRetries is the number of times to try invoking a funciton
// before giving up and returning an error
const MaxLambdaRetries = 3

// LambdaClient wraps the AWS Lambda API and provides functions for
// deploying and invoking lambda functions
type LambdaClient struct {
	client *lambda.Lambda
}

// NewLambdaClient initializes a new LambdaClient
func NewLambdaClient() *LambdaClient {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	return &LambdaClient{
		client: lambda.New(sess),
	}
}

func functionNeedsUpdate(functionCode []byte, cfg *lambda.FunctionConfiguration) bool {
	codeHash := sha256.New()
	codeHash.Write(functionCode)
	codeHashDigest := base64.StdEncoding.EncodeToString(codeHash.Sum(nil))
	return codeHashDigest != *cfg.CodeSha256
}

// DeployFunction deploys the current directory as a lamba function
func (l *LambdaClient) DeployFunction(functionName string) error {
	functionCode, err := l.buildPackage()
	if err != nil {
		panic(err)
	}

	exists, err := l.getFunction(functionName)
	if exists != nil && err == nil {
		if functionNeedsUpdate(functionCode, exists.Configuration) {
			log.Infof("Updating Lambda function '%s'", functionName)
			return l.updateFunction(functionName, functionCode)
		}
		log.Infof("Function '%s' is already up-to-date", functionName)
		return nil
	}

	log.Infof("Creating Lambda function '%s'", functionName)
	return l.createFunction(functionName, functionCode)
}

// DeleteFunction tears down the given function
func (l *LambdaClient) DeleteFunction(functionName string) error {
	deleteInput := &lambda.DeleteFunctionInput{
		FunctionName: aws.String(functionName),
	}

	_, err := l.client.DeleteFunction(deleteInput)
	return err
}

// crossCompile builds the current directory as a lambda package.
// It returns the location of a built binary file.
func crossCompile(binName string) (string, error) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		return "", err
	}

	outputPath := filepath.Join(tmpDir, binName)

	args := []string{
		"build",
		"-o", outputPath,
		"-ldflags", "-s -w",
		".",
	}
	cmd := exec.Command("go", args...)

	cmd.Env = append(os.Environ(), "GOOS=linux")

	combinedOut, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s\n%s", err, combinedOut)
	}

	return outputPath, nil
}

// buildPackage builds the current directory as a lambda package.
// It returns a byte slice containing a compressed binary that can be upload to lambda.
func (l *LambdaClient) buildPackage() ([]byte, error) {
	log.Info("Building Lambda function")
	binFile, err := crossCompile("lambda_artifact")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(filepath.Dir(binFile)) // Remove temporary binary file

	log.Debug("Opening recompiled binary to be zipped")
	binReader, err := os.Open(binFile)
	if err != nil {
		return nil, err
	}

	zipBuf := new(bytes.Buffer)
	archive := zip.NewWriter(zipBuf)
	header := &zip.FileHeader{
		Name:           "main",
		ExternalAttrs:  (0777 << 16), // File permissions
		CreatorVersion: (3 << 8),     // Magic number indicating a Unix creator
	}

	log.Debug("Adding binary to zip archive")
	writer, err := archive.CreateHeader(header)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(writer, binReader)
	if err != nil {
		return nil, err
	}

	binReader.Close()
	archive.Close()

	log.Debugf("Final zipped function binary size: %s", humanize.Bytes(uint64(len(zipBuf.Bytes()))))
	return zipBuf.Bytes(), nil
}

// updateFunction updates the lambda function with the given name with the given code as function binary
func (l *LambdaClient) updateFunction(functionName string, code []byte) error {
	updateArgs := &lambda.UpdateFunctionCodeInput{
		ZipFile:      code,
		FunctionName: aws.String(functionName),
	}

	_, err := l.client.UpdateFunctionCode(updateArgs)
	return err
}

// createFunction creates a lambda function with the given name and uses code as the function binary
func (l *LambdaClient) createFunction(functionName string, code []byte) error {
	funcCode := &lambda.FunctionCode{
		ZipFile: code,
	}

	createArgs := &lambda.CreateFunctionInput{
		Code:         funcCode,
		FunctionName: aws.String(functionName),
		Handler:      aws.String("main"),
		Runtime:      aws.String(lambda.RuntimeGo1X),
		Role:         aws.String("arn:aws:iam::847166266056:role/flask-example-dev-ZappaLambdaExecutionRole"),
		Timeout:      aws.Int64(60),
		MemorySize:   aws.Int64(1500),
	}

	_, err := l.client.CreateFunction(createArgs)
	return err
}

func (l *LambdaClient) getFunction(functionName string) (*lambda.GetFunctionOutput, error) {
	getInput := &lambda.GetFunctionInput{
		FunctionName: aws.String(functionName),
	}

	return l.client.GetFunction(getInput)
}

type invokeError struct {
	Message    string                                           `json:"errorMessage"`
	StackTrace []lambdaMessages.InvokeResponse_Error_StackFrame `json:"stackTrace"`
}

func (l *LambdaClient) tryInvoke(functionName string, payload []byte) ([]byte, error) {
	invokeInput := &lambda.InvokeInput{
		FunctionName: aws.String(functionName),
		Payload:      payload,
	}

	output, err := l.client.Invoke(invokeInput)
	if err != nil {
		return nil, err
	} else if output.FunctionError != nil {
		var errPayload invokeError
		err = json.Unmarshal(output.Payload, &errPayload)
		if err != nil {
			log.Debug(output.Payload)
			return nil, err
		}

		// Log stack trace if one was returned
		if len(errPayload.StackTrace) > 0 {
			log.Debug("Function invocation error. Stack trace:")
			for _, frame := range errPayload.StackTrace {
				log.Debugf("\t%s\t%s:%d", frame.Label, frame.Path, frame.Line)
			}
		}

		return output.Payload, fmt.Errorf("Function error: %s", errPayload.Message)
	}
	return output.Payload, err
}

// Invoke invokes the given Lambda function with the given payload.
func (l *LambdaClient) Invoke(functionName string, payload []byte) (outputPayload []byte, err error) {
	for try := 0; try < MaxLambdaRetries; try++ {
		outputPayload, err = l.tryInvoke(functionName, payload)
		if err == nil {
			break
		}
		log.Debugf("Function invocation failed. (Attempt %d of %d)", try, MaxLambdaRetries)
	}
	return outputPayload, err
}
