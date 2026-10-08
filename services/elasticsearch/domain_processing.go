package elasticsearch

import "time"

const (
	dpsCreating  = "Creating"
	dpsModifying = "Modifying"
	dpsUpgrading = "UpgradingEngineVersion"
	dpsDeleting  = "Deleting"

	optionStateProcessing = "Processing"
)

func (b *InMemoryBackend) clock() time.Time {
	if b.now != nil {
		return b.now()
	}

	return time.Now()
}

// Now returns the backend clock, for handlers resolving processing windows.
func (b *InMemoryBackend) Now() time.Time {
	b.mu.RLock("Now")
	defer b.mu.RUnlock()

	return b.clock()
}

// SetProcessingDelay sets how long create, modify, upgrade and delete windows
// stay observable before settling; the default 0 settles immediately.
func (b *InMemoryBackend) SetProcessingDelay(d time.Duration) {
	b.mu.Lock("SetProcessingDelay")
	defer b.mu.Unlock()

	b.processingDelay = d
}

// SetClock installs a deterministic clock; nil restores time.Now.
func (b *InMemoryBackend) SetClock(fn func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = fn
}

// beginProcessing opens a processing window on d. Caller holds the write lock.
func (b *InMemoryBackend) beginProcessing(d *Domain, status string) {
	d.ProcessingStatus = status
	d.ProcessingUntil = b.clock().Add(b.processingDelay)
}

// domainProcessing resolves the Processing flag and DomainProcessingStatus of
// d at now.
func domainProcessing(d *Domain, now time.Time) (bool, string) {
	if d.ProcessingUntil.IsZero() || !now.Before(d.ProcessingUntil) {
		return false, statusActiveCap
	}

	return true, d.ProcessingStatus
}

// deleteWindowElapsed reports whether d was deleted and its window has passed.
func deleteWindowElapsed(d *Domain, now time.Time) bool {
	return d.Deleted && !now.Before(d.ProcessingUntil)
}

// optionState maps the processing window onto OptionStatus.State.
func optionState(d *Domain, now time.Time) string {
	if processing, _ := domainProcessing(d, now); processing {
		return optionStateProcessing
	}

	return statusActiveCap
}
