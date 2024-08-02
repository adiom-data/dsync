# Tasks and Partitioning

To allow for parallel execution of reads and more granular resumability, we implemented an abstraction called Tasks. They correspond to database namespaces (collections). A task can cover the whole namespace, or a part of it, if the specific bounds and partition key are provided.

For a collections with a lot of documents (e.g. 50 mil+), it's best to split the task into parts. This way the parts can be executed in parallel and much less progress will be lost in case the process needs to be stopped.

Unfortunately, Cosmos DB with MongoDB API doesn't properly support $sample in the aggregation framework or any other methods for generally effecient splitting. For Cosmos DB sources, we implement partitioning based on the actual _id values, taking advantage of the Law Of Big Numbers. When _id is ObjectId() which is a timestamp-based format, and the number of documents is in 10's of millions or more, we create partitions by splitting the ObjectId() range in the collection into equal parts. This works well for large collections with relatively consisent workloads.

The target partition size is hardcoded to be 512,000 documents and is subject to change. This specific value was chosen to provide a meaningful number of partitions without creating much unnecceary overhead.
