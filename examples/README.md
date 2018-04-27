# Corral Examples

The below examples are provided for "Getting Started" writing applicaitons in corral.

The Amplab benchmarks are useful for comparing corral's performance to other "Big Data Frameworks". These benchmark applications are also useful as they showcase common MapReduce tasks (filters, aggregations, and joins) as written in corral.

## [Word Count](word_count)

* Reads input files line-by-line and reports the occurences of each observed word.

## [Amplab Benchmark Query 1](amplab1)

* Implements the ["Scan Query" benchmark](https://amplab.cs.berkeley.edu/benchmark/#query1) from the Amplab Big Data Benchmark
* Performs a scan of input data, with a filter enforced on certain fields

## [Amplab Benchmark Query 2](amplab2)

* Implements the ["Aggregation Query" benchmark](https://amplab.cs.berkeley.edu/benchmark/#query2) from the Amplab Big Data Benchmark
* Performs a filter on input data, and returns an aggregate (sum) value by key

## [Amplab Benchmark Query 3](amplab3)

* Implements the ["Join Query" benchmark](https://amplab.cs.berkeley.edu/benchmark/#query3) from the Amplab Big Data Benchmark
* Performs filters, aggregations, and a join on multiple input datasets.

