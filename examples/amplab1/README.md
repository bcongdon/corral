# Amplab 1 Example

This example implements the ["Scan Query" benchmark](https://amplab.cs.berkeley.edu/benchmark/#query1) from the Amplab Big Data Benchmark.

## Benchmark Results

| Benchmark             | Dataset Size | Job Execution Time |
|:----------------------|:-------------|:-------------------|
| test_al1_local_tiny   | 77.6KB       | 10ms               |
| test_al1_s3_tiny      | 77.6KB       | 1.25sec            |
| test_al1_lambda_tiny  | 77.6KB       | 3.92sec            |
| test_al1_lambda_1node | 1.28GB       | 35.6sec            |
| test_al1_lambda_5node | 6.38GB       | 41.8sec            |

Compared to the results reported in the [graphs provided by Amplab](https://amplab.cs.berkeley.edu/benchmark/#query1), corral performs reasonably strongly. It does not outperform most of the listed frameworks (except for Hive), but executes within a a similar timescale.
