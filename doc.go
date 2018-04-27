/*Package corral is a MapReduce framework designed to be deployed to serverless
platforms, like AWS Lambda.

It presents a lightweight alternative to Hadoop MapReduce. Much of the design
philosophy was inspired by Yelp's mrjob -- corral retains mrjob's ease-of-use
while gaining the type safety and speed of Go.

Corral's runtime model consists of stateless, transient executors controlled by
a central driver. Currently, the best environment for deployment is AWS Lambda,
but corral is modular enough that support for other serverless platforms can be
added as support for Go in cloud functions improves.

Corral is best suited for data-intensive but computationally inexpensive tasks,
such as ETL jobs.
*/
package corral
