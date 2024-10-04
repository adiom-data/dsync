# How it works
1. When a flow integrity check is requested, Coordinator will get a read plan from the source.
2. For each read plan task, the Coordinator will query each of the source and desination for a result.
3. A successful result is returned if all tasks resulted in a match.

# Algorithms

An integrity check algorithm is supposed to calculate a database-agnostic function of a dataset in a deterministic and unique way given the specific options, e.g. a list of provided namespaces. An example of such a function could be a COUNT(*) or XOR. As much as we can, we should be using functions and algorithms that are easily parallelizable and can be calculated server-side for efficiency.

# Currently
Right now the implementation supports a simple count and running a xxHash on every document and XORing between them for a full verification. The `--verify` option defaults to a full verification, and `--verify-quick-count` does a count only. These checks may fail if there are active updates as it cannot be guaranteed that new writes will be reflected on both sides at the time they are checked.