# Tasks and Partitioning

To allow for parallel execution of reads and more granular resumability, we implemented an abstraction called Tasks. They correspond to database namespaces (collections). A task can cover the whole namespace, or a part of it, if the specific bounds and partition key are provided.

For collections with a lot of documents (e.g. 50 mil+), it's best to split the task into parts. This way the parts can be executed in parallel and much less progress will be lost in case the process needs to be stopped and resumed later.

Unfortunately, Cosmos DB with MongoDB API doesn't properly support $sample in the aggregation framework or any other methods for statistical splitting that doesn't require scanning the whole collection. For Cosmos DB sources, we implemented a partitioning strategy based on the actual _id values, taking advantage of the Law Of Big Numbers. When _id is of ObjectId type which is a timestamp-based format, and the number of documents is in 10's of millions or more, we create partitions by splitting the time range in the collection into equal parts. This works well for large collections with relatively consistent workloads.

The target partition size is set to 50,000 documents and is subject to change in future versions. This specific value was chosen to provide a meaningful number of partitions without creating much unnecessary overhead.
