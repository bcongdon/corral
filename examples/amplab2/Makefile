BUCKET = ${AWS_TEST_BUCKET}
all: amplab2

amplab2: amplab2.go ../../*.go ../../internal/pkg/**/*.go
	go build -o amplab2 amplab2.go

test_al2_local: amplab2
	./amplab2 data/*

tiny_data:
	aws s3 cp --recursive ./data/ s3://${BUCKET}

test_al2_s3: amplab2 tiny_data
	./amplab2 -out s3://${BUCKET} s3://${BUCKET}/*

test_al2_local_1node: amplab2
	./amplab2 s3://big-data-benchmark/pavlo/text/1node/uservisits/*

test_al2_lambda_tiny: amplab2 tiny_data
	./amplab2 -lambda -out s3://${BUCKET} s3://${BUCKET}/*

test_al2_lambda_1node: amplab2
	./amplab2 -lambda -out s3://${BUCKET} s3://big-data-benchmark/pavlo/text/1node/uservisits/*

test_al2_lambda_5node: amplab2
	./amplab2 -lambda -out s3://${BUCKET} s3://big-data-benchmark/pavlo/text/5nodes/uservisits/*

clean:
	find . -name "*.out" -print0 | xargs -0 rm
	rm -f amplab2 output*
	aws s3 rm s3://${BUCKET} --recursive
