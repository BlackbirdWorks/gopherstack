package iotanalytics

import "time"

// SetLifecycleDelay sets how long channels, datastores and datasets report CREATING and
// dataset contents report CREATING before SUCCEEDED. Zero (the default) settles instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

func (b *InMemoryBackend) createStatus() string {
	if b.lifecycleDelay > 0 {
		return statusCreating
	}

	return statusActive
}

func (b *InMemoryBackend) createContentStatus() string {
	if b.lifecycleDelay > 0 {
		return statusCreating
	}

	return statusSucceeded
}

func (b *InMemoryBackend) readyAt() time.Time {
	if b.lifecycleDelay > 0 {
		return time.Now().Add(b.lifecycleDelay)
	}

	return time.Time{}
}

// observeStatus resolves a stored CREATING status; a zero readyAt (restored rows) is ready.
func observeStatus(status, done string, readyAt time.Time) string {
	if status == statusCreating && !time.Now().Before(readyAt) {
		return done
	}

	return status
}

func observeContent(c *DatasetContent) *DatasetContent {
	cp := *c
	cp.Status = observeStatus(c.Status, statusSucceeded, c.readyAt)

	return &cp
}
