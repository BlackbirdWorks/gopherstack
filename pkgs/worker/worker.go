// Package worker is the standard primitive for the background work gopherstack
// services run: periodic "janitor" sweeps, one-shot delayed state transitions,
// and tracked goroutines.
//
// All of it is owned by [Group], a per-service supervisor constructed from the
// service/lifecycle context (never context.Background). A Group roots every task
// at that context, logs via the context logger, recovers panics (recording a
// "panic" task to the injected [Metrics]) so one failure never takes down a
// goroutine or the process, and joins everything deterministically on [Group.Stop].
// This removes the hand-rolled time.NewTicker / time.AfterFunc / go func copies
// that each risked a missing Stop, a leaked goroutine, or an unrecovered panic.
package worker
