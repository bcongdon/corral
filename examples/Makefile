BUCKET = ${AWS_TEST_BUCKET}
all: word_count

word_count: word_count.go ../*.go
	go build -o word_count word_count.go

test_wc_local: word_count
	./word_count metamorphosis.txt

test_wc_s3: word_count
	aws s3 cp ./metamorphosis.txt s3://${BUCKET}
	./word_count -s3 metamorphosis.txt

clean:
	find . -name "*.out" -print0 | xargs -0 rm
	rm -f word_count output*
	aws s3 rm s3://${BUCKET} --recursive
