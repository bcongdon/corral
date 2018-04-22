BUCKET = ${AWS_TEST_BUCKET}
BIN_DIR = ./bin
PROG_NAME = word_count

.PHONY: all clean $(PROG_NAME) input_in_s3

all: $(PROG_NAME)

$(PROG_NAME):
	go build -o $(BIN_DIR)/$@ .

input_in_s3:
	aws s3 cp ./metamorphosis.txt s3://${BUCKET}

test_wc_local: $(PROG_NAME)
	$(BIN_DIR)/$(PROG_NAME) metamorphosis.txt

test_wc_s3: $(PROG_NAME) input_in_s3
	$(BIN_DIR)/$(PROG_NAME) --out s3://${BUCKET}/ s3://${BUCKET}/metamorphosis.txt

test_wc_lambda: $(PROG_NAME) input_in_s3
	$(BIN_DIR)/$(PROG_NAME) --lambda --out s3://${BUCKET}/ s3://${BUCKET}/metamorphosis.txt

clean:
	find . -name "*.out" -print0 | xargs -0 rm
	rm -f $(BIN_DIR)/$(PROG_NAME) output*
	aws s3 rm s3://${BUCKET} --recursive
