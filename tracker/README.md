# Gardener Tracker

The Tracker keeps track of the state of all parsing activities, persists
current state to storage, and recovers the system state from storage on
startup or recovery.

The tracker is used by other components of Gardener to decide:

1. what jobs to do next,
2. when a job has failed and needs to be recovered,
3. when postprocessing actions should be initiated.

The tracker provides an API to the other Gardener components to answer
questions about the system state.
