# How it works
1. When a flow integrity check is requested, Coordinator sends the integrity check calculation request to connectors.
2. Connectors respond back asynchronously with the results.
3. Coordinator ensures that both calculations were successful and compares the results.

# Algorithms

An integrity check algorithm is supposed to calculate a database-agnostic function of a dataset in a deterministic and unique way given the specific options, e.g. a list of provided namespaces. An example of such a function could be a COUNT(*) or XOR. As much as we can, we should be using functions and algorithms that are easily parallelizable and can be calculated server-side for effeciency.

## ONSL ("Ordered Namespace List Count")
### Description
An efficient heuristic-based option that calculates the digest of (namespace-count) pairs and the total number of documents.
### Steps
1. Identfiy the namespaces of the dataset in the form of "db.collection", excluding any system namespaces. 
    - If there are no namespaces, return 0 as the count and an empty string as the digest
2. For each namespace, calculate the total number of documents
3. Arrange the namespaces in a lexicographical order
4. Create a string, concatenating the namespaces and respective counts in the form of "namespace:count" and using "," to join them
5. Calculate the SHA256 hash of the string and the total number of documents across all the namespaces