# Cosmos DB deletes emulation

As a work around the Cosmos DB changestream [limitation](https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/change-streams?tabs=javascript#current-limitations), dsync implements a specially-designed algorithm that detects deletes on the source and emits them on a periodic basis.

## Algorithm

- We use an external "Witness" index to track which documents we've already seen coming from the source.
    - The key assumption is that the set of keys in the Witness index is always a superset of the keys in the Source index (W = S + whatever was deleted)
    - For simplicity, we can use the destination's _id index directly as a Witness as it already has exactly what we need maintained with transactional semantics.
 - At any point in time, any document that is not present in the Witness index but is present in the source is considered deleted on the source.

1. Get namespaces list from the Witness
2. As a heuristic, compare the number of index entries for each namespace on both sides. If it matches, we assume that no deletes happened since the last check.
3. For namespaces with mismatching counts, we scan the Witness index and look for the keys not present in the Source index. Those are collected and delete messages are sent for them into the data channel.

## Pros: 
- For users, there's no need to change the application code to use soft deletes.
- The algorithm works reliably and ensures eventual consistency.
- The behavior can be turned on/off anytime in the process without restarting the sync from scratch.

## Cons:
- Index scans can be expensive hence the check period may need to be adjusted to balance the lag and the responsiveness.