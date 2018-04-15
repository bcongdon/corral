BUCKET = ${AWS_TEST_BUCKET}
all: amplab1

amplab1: amplab1.go ../../*.go ../../internal/pkg/**/*.go
	go build -o amplab1 amplab1.go

test_al1_local_tiny: amplab1
	./amplab1 data/*

tiny_data:
	aws s3 cp --recursive ./data/ s3://${BUCKET}

test_al1_s3_tiny: amplab1 tiny_data
	./amplab1 -out s3://${BUCKET} s3://${BUCKET}/part-*

test_al1_local_1node: amplab1
	./amplab1 s3://big-data-benchmark/pavlo/text/1node/rankings/part-*

test_al1_lambda_tiny: amplab1 tiny_data
	./amplab1 -lambda -out s3://${BUCKET} s3://${BUCKET}/part-*

test_al1_lambda_1node: amplab1
	./amplab1 -lambda -out s3://${BUCKET} s3://big-data-benchmark/pavlo/text/1node/rankings/part-*

test_al1_lambda_5node: amplab1
	./amplab1 -lambda -out s3://${BUCKET} s3://big-data-benchmark/pavlo/text/5nodes/rankings/part-*

clean:
	find . -name "*.out" -print0 | xargs -0 rm
	rm -f amplab1 output*
	aws s3 rm s3://${BUCKET} --recursive
