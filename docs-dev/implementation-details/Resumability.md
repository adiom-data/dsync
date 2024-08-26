# Key Concepts

* __Capabilities.__ Both connectors should support resumability in order for Coordinator to enable it for a flow.
* __Flow task plan.__ Essentially defines the flow status. This is the task plan requested from the source at the start
  of the flow. Tasks have a status indicating whether they have been completed.
* __Metadata.__ Coordinator uses the metadata store to persist and retrieve the flow state and the task plan.
* __Barriers.__ Source injects special messages into the data channel - barriers. They confirm completion of a
  particular task on the source and allow the source to send that notification to the destination in a synchronized way
  with the data itself. I.e. it's safe to assume that after a barrier has been seen, there will be no more data sent for
  that particular task.
* __Notifications.__ Source and destination connectors also signal to the Coordinator directly when they are done with a
  task. When Coordinator gets a notification from the Destination, it marks the task as completed under the assumption
  that the related data was persisted on the destination.

# How it works

1. When a flow is created, Coordinator checks if that specific flow has already been persisted. Since flow ID is
   deterministic based on flow options, it's preserved across runs.
2. If the flow has been persisted, the stored plan is then reused and the tasks are sent to the source connector for
   execution.
3. With each newly completed task, the source connector sends a barrier message over the data channel to indicate that
   there will be no more data sent for that task.
4. When the destination connector encounters a barrier message, it ensures that the data for that task has been written.
   After that been confirmed (or maybe implicitly true by design), the connector signals to the coordinator.
5. The coordinator marks the task as completed.
