package corlambda

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	log "github.com/sirupsen/logrus"
)

type LambdaClient struct {
	client *lambda.Lambda
}

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
	return codeHashDigest == *cfg.CodeSha256
}

func (l *LambdaClient) DeployFunction(functionName string) error {
	functionCode, err := l.buildPackage()
	if err != nil {
		panic(err)
	}

	exists, err := l.getFunction(functionName)
	if exists != nil && err == nil {
		if functionNeedsUpdate(functionCode, exists.Configuration) {
			log.Debugf("Updating Lambda function '%s'", functionName)
			return l.updateFunction(functionName, functionCode)
		}
		log.Debugf("Function '%s' is already up-to-date", functionName)
		return nil
	}

	log.Debugf("Creating Lambda function '%s'", functionName)
	return l.createFunction(functionName, functionCode)
}

func (l *LambdaClient) DeleteFunction(functionName string) error {
	deleteInput := &lambda.DeleteFunctionInput{
		FunctionName: aws.String(functionName),
	}

	_, err := l.client.DeleteFunction(deleteInput)
	return err
}

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

func (l *LambdaClient) buildPackage() ([]byte, error) {
	log.Debug("Compiling lambda function for Lambda")
	binFile, err := crossCompile("lambda_artifact")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(filepath.Dir(binFile))

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

	return zipBuf.Bytes(), nil
}

func (l *LambdaClient) updateFunction(functionName string, code []byte) error {
	updateArgs := &lambda.UpdateFunctionCodeInput{
		ZipFile:      code,
		FunctionName: aws.String(functionName),
	}

	_, err := l.client.UpdateFunctionCode(updateArgs)
	return err
}

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
		Timeout:      aws.Int64(300),
		MemorySize:   aws.Int64(3000),
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

func (l *LambdaClient) Invoke(functionName string, payload []byte) ([]byte, error) {
	invokeInput := &lambda.InvokeInput{
		FunctionName: aws.String(functionName),
		Payload:      payload,
	}

	output, err := l.client.Invoke(invokeInput)
	return output.Payload, err
}
