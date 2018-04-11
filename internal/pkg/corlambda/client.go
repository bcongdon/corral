package corlambda

import (
	"archive/zip"
	"bytes"
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

func (l *LambdaClient) DeployFunction(functionName string) {
	functionCode, err := l.compile()
	if err != nil {
		panic(err)
	}
	err = l.createFunction(functionName, functionCode)
	if err != nil {
		err = l.updateFunction(functionName, functionCode)
	}
	if err != nil {
		panic(err)
	}
}

func build(binName string) (string, error) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		return "", err
	}

	outputPath := filepath.Join(tmpDir, binName)

	flags := `-extldflags "-static"`
	args := []string{
		"build",
		"-o", outputPath,
		"--ldflags", flags,
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

func (l *LambdaClient) compile() ([]byte, error) {
	log.Debug("Compiling lambda function for Lambda")
	binFile, err := build("lambda_artifact")
	if err != nil {
		return []byte{}, err
	}
	defer os.RemoveAll(filepath.Dir(binFile))

	log.Debug("Opening recompiled binary to be zipped")
	binReader, err := os.Open(binFile)
	if err != nil {
		return []byte{}, err
	}
	binInfo, err := os.Stat(binFile)

	zipBuf := new(bytes.Buffer)
	archive := zip.NewWriter(zipBuf)

	log.Debug("Adding binary to zip archive")

	header, err := zip.FileInfoHeader(binInfo)
	if err != nil {
		return []byte{}, err
	}

	writer, err := archive.CreateHeader(header)
	if err != nil {
		return []byte{}, err
	}

	_, err = io.Copy(writer, binReader)
	if err != nil {
		return []byte{}, err
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
	}

	log.Debug("Creating Lambda function")
	_, err := l.client.CreateFunction(createArgs)
	return err
}

func (l *LambdaClient) Invoke(functionName string, payload []byte) ([]byte, error) {
	invokeInput := &lambda.InvokeInput{
		FunctionName: aws.String(functionName),
		Payload:      payload,
	}
	output, err := l.client.Invoke(invokeInput)
	return output.Payload, err
}
