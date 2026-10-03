package cloudtrail

import (
	"testing"
	"time"
)

// BenchmarkTrimEventsLocked_AtCapacity models steady-state load: the store
// sits at maxStoredEvents and every sweep must shed the newest excess.
func BenchmarkTrimEventsLocked_AtCapacity(b *testing.B) {
	be := NewInMemoryBackend("123456789012", "us-east-1")

	now := time.Now().UTC()

	be.events = make([]Event, maxStoredEvents, maxStoredEvents+trimEventsSweepEvery)
	for i := range be.events {
		be.events[i] = Event{EventTime: now, EventName: "PutObject"}
	}

	extra := make([]Event, trimEventsSweepEvery)
	for i := range extra {
		extra[i] = Event{EventTime: now, EventName: "PutObject"}
	}

	b.ReportAllocs()

	for b.Loop() {
		be.mu.Lock("bench")
		be.events = append(be.events, extra...)
		be.trimEventsLocked()
		be.mu.Unlock()
	}
}

func BenchmarkRecordEvent_AtCapacity(b *testing.B) {
	be := NewInMemoryBackend("123456789012", "us-east-1")
	be.SetS3Backend(benchS3{})

	now := time.Now().UTC()

	be.events = make([]Event, maxStoredEvents)
	for i := range be.events {
		be.events[i] = Event{EventTime: now, EventName: "PutObject"}
	}

	ev := Event{
		EventName:       "PutObject",
		EventSource:     "s3.amazonaws.com",
		CloudTrailEvent: `{"eventName":"PutObject","eventSource":"s3.amazonaws.com"}`,
	}

	b.ReportAllocs()

	for b.Loop() {
		be.RecordEvent(ev)
	}
}
