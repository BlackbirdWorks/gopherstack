package autoscaling

import (
	"fmt"
	"sort"
)

// DescribeScalingActivities returns scaling activities for the given group.
func (b *InMemoryBackend) DescribeScalingActivities(groupName string) ([]ScalingActivity, error) {
	b.mu.RLock("DescribeScalingActivities")
	defer b.mu.RUnlock()

	if groupName != "" {
		if !b.groups.Has(groupName) {
			return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
		}

		acts := b.activities[groupName]
		result := make([]ScalingActivity, len(acts))
		copy(result, acts)

		return result, nil
	}

	result := make([]ScalingActivity, 0, len(b.activities))
	for _, acts := range b.activities {
		result = append(result, acts...)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ActivityID < result[j].ActivityID
	})

	return result, nil
}
