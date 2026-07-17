package codedeploy

import "fmt"

// maxBatchRevisions is the maximum number of revisions accepted by BatchGetApplicationRevisions.
const maxBatchRevisions = 25

// BatchGetApplicationRevisions validates that the application exists.
// It accepts up to maxBatchRevisions revisions per AWS spec.
func (b *InMemoryBackend) BatchGetApplicationRevisions(appName string, count int) (string, error) {
	b.mu.RLock("BatchGetApplicationRevisions")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return "", fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	if count > maxBatchRevisions {
		return "", fmt.Errorf("%w: at most %d revisions can be requested at once, got %d",
			ErrValidation, maxBatchRevisions, count)
	}

	return appName, nil
}
