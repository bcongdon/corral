BUCKET = ${AWS_TEST_BUCKET}

BIN_DIR = ./bin
PROG_NAME = amplab1
AMPLAB_PATH = big-data-benchmark/pavlo/text

.PHONY: all clean $(PROG_NAME) input_in_s3

.EXPORT_ALL_VARIABLES:
CORRAL_VERBOSE = true

all: $(PROG_NAME)

$(PROG_NAME):
	go build -o $(BIN_DIR)/$@ .

test_al1_local_tiny: $(PROG_NAME)
	$(BIN_DIR)/$(PROG_NAME) data/*

tiny_data:
	aws s3 cp --recursive ./data/ s3://${BUCKET}

test_al1_s3_tiny: $(PROG_NAME) tiny_data
	$(BIN_DIR)/$(PROG_NAME) --out s3://${BUCKET} s3://${BUCKET}/part-*

test_al1_lambda_tiny: $(PROG_NAME) tiny_data
	$(BIN_DIR)/$(PROG_NAME) --lambda --out s3://${BUCKET} s3://${BUCKET}/part-*

test_al1_lambda_1node: $(PROG_NAME)
	$(BIN_DIR)/$(PROG_NAME) --lambda --out s3://${BUCKET} s3://$(AMPLAB_PATH)/1node/rankings/part-*

test_al1_lambda_5node: $(PROG_NAME)
	.$(BIN_DIR)/$(PROG_NAME) --lambda --out s3://${BUCKET} s3://$(AMPLAB_PATH)/5nodes/rankings/part-*

clean:
	find . -name "*.out" -print0 | xargs -0 rm
	rm -f $(BIN_DIR)/$(PROG_NAME) output*
	aws s3 rm s3://${BUCKET} --recursive
