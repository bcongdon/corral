BUCKET = ${AWS_TEST_BUCKET}
all: word_count

word_count: word_count.go ../../*.go ../../internal/pkg/**/*.go
	go build -o word_count word_count.go

input_in_s3:
	aws s3 cp ./metamorphosis.txt s3://${BUCKET}

test_wc_local: word_count
	./word_count metamorphosis.txt

test_wc_s3: word_count input_in_s3
	./word_count -out s3://${BUCKET}/ s3://${BUCKET}/metamorphosis.txt

test_wc_lambda: word_count input_in_s3
	./word_count -lambda -out s3://${BUCKET}/ s3://${BUCKET}/metamorphosis.txt

clean:
	find . -name "*.out" -print0 | xargs -0 rm
	rm -f word_count output*
	aws s3 rm s3://${BUCKET} --recursive
