# Amplab 3 Example

This example implements the ["Join Query" benchmark](https://amplab.cs.berkeley.edu/benchmark/#query3) from the Amplab Big Data Benchmark.

## Benchmark Results

| Benchmark             | Dataset Size | Job Execution Time |
|:----------------------|:-------------|:-------------------|
| test_al3_local_tiny   | 1.7MB        | 580ms              |
| test_al3_s3_tiny      | 1.7MB        | 7.61sec            |
| test_al3_lambda_tiny  | 1.7MB        | 5.07sec            |
| test_al3_lambda_1node | 26.68GB      | 288.09sec          |
| test_al3_lambda_5node | 133.18GB     | 884.61sec          |

Compared to the results reported in the [graphs provided by Amplab](https://amplab.cs.berkeley.edu/benchmark/#query3), corral performs reasonably strongly. For smaller dataset sizes (i.e. for the "tiny" and "1node" datasets), corral outperforms its competition -- except perhaps for Redshift. However, the limitations of corral's architecture can be seen in the larger "5node" benchmark. Since corral doesn't have an internal secondary sort, joins on large datasets are quite expensive. In the "5node" benchmark, corral loses in performance to all except Tez and Hive.