# Amplab 2 Example

This example implements the ["Aggregation Query" benchmark](https://amplab.cs.berkeley.edu/benchmark/#query2) from the Amplab Big Data Benchmark.

## Benchmark Results

| Benchmark             | Dataset Size | Job Execution Time |
|:----------------------|:-------------|:-------------------|
| test_al2_local_tiny   | 1.7MB        | 180ms              |
| test_al2_s3_tiny      | 1.7MB        | 3.19sec            |
| test_al2_lambda_tiny  | 1.7MB        | 2.33sec            |
| test_al2_lambda_1node | 25.4GB       | 48.48sec           |
| test_al2_lambda_5node | 126.8GB      | 168.83sec          |

Compared to the results reported in the [graphs provided by Amplab](https://amplab.cs.berkeley.edu/benchmark/#query2), corral performs very strongly. Corral appears (from the admitedly unscientific benchmarking I've done) outperform most of the listed frameworks. This is likely due to the high bandwidth available between Lambda and S3, as well as the level of parallelism afforded by using Lambda over a traditional 1-5 node cluster.