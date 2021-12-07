AWS_TEST_BUCKET = lyuze/corral/amplab2/
BUCKET = ${AWS_TEST_BUCKET}

BIN_DIR = ./bin
PROG_NAME = amplab2
AMPLAB_PATH = big-data-benchmark/pavlo/text

.PHONY: all clean $(PROG_NAME) input_in_s3

.EXPORT_ALL_VARIABLES:
CORRAL_VERBOSE=true

all: $(PROG_NAME)

$(PROG_NAME):
	go build -o $(BIN_DIR)/$@ .

test_al2_local_tiny: $(PROG_NAME)
	$(BIN_DIR)/$(PROG_NAME) data/*

tiny_data:
	aws s3 cp --recursive ./data/ s3://${BUCKET}

test_al2_s3_tiny: $(PROG_NAME) tiny_data
	$(BIN_DIR)/$(PROG_NAME) --out s3://${BUCKET} s3://${BUCKET}/part-*

test_al2_lambda_tiny: $(PROG_NAME) tiny_data
	$(BIN_DIR)/$(PROG_NAME) --lambda --out s3://${BUCKET} s3://${BUCKET}/part-*

test_al2_lambda_1node: $(PROG_NAME)
	$(BIN_DIR)/$(PROG_NAME) --lambda --out s3://${BUCKET} s3://$(AMPLAB_PATH)/1node/uservisits/part-*

test_al2_lambda_5node: $(PROG_NAME)
	.$(BIN_DIR)/$(PROG_NAME) --lambda --out s3://${BUCKET} s3://$(AMPLAB_PATH)/5nodes/uservisits/part-*

clean:
	find . -name "*.out" -print0 | xargs -0 rm
	rm -f $(BIN_DIR)/$(PROG_NAME) output*
	aws s3 rm s3://${BUCKET} --recursive
