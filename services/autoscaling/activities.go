package autoscaling

import (
	"fmt"
	"sort"
)

// DescribeScalingActivities returns scaling activities for the given group.
// DescribeScalingActivities returns scaling activities for groupName (or
// account-wide when empty), optionally restricted to the given StatusCode
// values -- the "Status" Filter.Name api_op_DescribeScalingActivities.go
// documents ("This filter can only be used in combination with the
// AutoScalingGroupName parameter"). StartTimeLowerBound/StartTimeUpperBound
// are the other two documented Filter.Name values; this backend does not
// filter on them (see PARITY.md).
func (b *InMemoryBackend) DescribeScalingActivities(groupName string, statuses []string) ([]ScalingActivity, error) {
	b.mu.RLock("DescribeScalingActivities")
	defer b.mu.RUnlock()

	statusFilter := make(map[string]bool, len(statuses))
	for _, s := range statuses {
		statusFilter[s] = true
	}

	matches := func(a *ScalingActivity) bool {
		return len(statusFilter) == 0 || statusFilter[a.StatusCode]
	}

	if groupName != "" {
		if !b.groups.Has(groupName) {
			return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
		}

		acts := b.activities[groupName]
		result := make([]ScalingActivity, 0, len(acts))

		for i := range acts {
			if matches(&acts[i]) {
				result = append(result, acts[i])
			}
		}

		return result, nil
	}

	result := make([]ScalingActivity, 0, len(b.activities))
	for _, acts := range b.activities {
		for i := range acts {
			if matches(&acts[i]) {
				result = append(result, acts[i])
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ActivityID < result[j].ActivityID
	})

	return result, nil
}
