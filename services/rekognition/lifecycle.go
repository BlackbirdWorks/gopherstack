package rekognition

import "time"

const (
	statusStarting        = "STARTING"
	statusStopping        = "STOPPING"
	statusTrainingActive  = "TRAINING_IN_PROGRESS"
	statusTrainingDone    = "TRAINING_COMPLETED"
	statusCopyingActive   = "COPYING_IN_PROGRESS"
	statusCopyingDone     = "COPYING_COMPLETED"
	statusDatasetCreating = "CREATE_IN_PROGRESS"
	statusDatasetCreated  = "CREATE_COMPLETE"
	statusDatasetUpdating = "UPDATE_IN_PROGRESS"
	statusDatasetUpdated  = "UPDATE_COMPLETE"
)

// SetLifecycleDelay sets how long stream processors and project versions stay in a
// transitional state (STARTING, STOPPING, TRAINING_IN_PROGRESS, COPYING_IN_PROGRESS) and
// datasets in CREATE_IN_PROGRESS or UPDATE_IN_PROGRESS. Zero (the default) settles instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

func (b *InMemoryBackend) elapsed(since time.Time) bool {
	return b.lifecycleDelay <= 0 || time.Since(since) >= b.lifecycleDelay
}

// settleStreamProcessorLocked completes a pending STARTING or STOPPING transition.
func (b *InMemoryBackend) settleStreamProcessorLocked(p *storedStreamProcessor) {
	if !b.elapsed(p.statusSince) {
		return
	}

	switch p.Status {
	case statusStarting:
		p.Status = processorRunning
	case statusStopping:
		p.Status = processorStopped
	}
}

// settleProjectVersionLocked completes a pending training, copy, start or stop transition.
func (b *InMemoryBackend) settleProjectVersionLocked(v *storedProjectVersion) {
	switch v.Status {
	case statusTrainingActive:
		if b.elapsed(v.CreationTimestamp) {
			v.Status = statusTrainingDone
		}
	case statusCopyingActive:
		if b.elapsed(v.CreationTimestamp) {
			v.Status = statusCopyingDone
		}
	case statusStarting:
		if b.elapsed(v.statusSince) {
			v.Status = processorRunning
		}
	case statusStopping:
		if b.elapsed(v.statusSince) {
			v.Status = processorStopped
		}
	}
}

func (b *InMemoryBackend) settleDatasetLocked(ds *storedDataset) {
	if !b.elapsed(ds.LastUpdatedTimestamp) {
		return
	}

	switch ds.Status {
	case statusDatasetCreating:
		ds.Status = statusDatasetCreated
	case statusDatasetUpdating:
		ds.Status = statusDatasetUpdated
	}
}
