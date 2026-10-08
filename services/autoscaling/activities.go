package autoscaling

import (
	"fmt"
	"sort"
)

// maxDeletedActivities bounds the activity history kept for deleted groups.
const maxDeletedActivities = 5000

// retireActivities moves a deleted group's activities into the bounded
// deleted-group history. Caller holds b.mu.
func (b *InMemoryBackend) retireActivities(name string) {
	b.deletedActivities = append(b.deletedActivities, b.activities[name]...)
	if over := len(b.deletedActivities) - maxDeletedActivities; over > 0 {
		b.deletedActivities = b.deletedActivities[over:]
	}

	delete(b.activities, name)
}

// DescribeScalingActivities returns scaling activities for groupName (or
// account-wide when empty), optionally restricted to the given StatusCode
// values (the "Status" Filter.Name).
func (b *InMemoryBackend) DescribeScalingActivities(groupName string, statuses []string) ([]ScalingActivity, error) {
	return b.DescribeScalingActivitiesFor(groupName, statuses, false)
}

// DescribeScalingActivitiesFor is DescribeScalingActivities, optionally also
// returning activities of deleted groups (IncludeDeletedGroups).
func (b *InMemoryBackend) DescribeScalingActivitiesFor(
	groupName string, statuses []string, includeDeleted bool,
) ([]ScalingActivity, error) {
	b.mu.RLock("DescribeScalingActivities")
	defer b.mu.RUnlock()

	statusFilter := make(map[string]bool, len(statuses))
	for _, s := range statuses {
		statusFilter[s] = true
	}

	matches := func(a *ScalingActivity) bool {
		return len(statusFilter) == 0 || statusFilter[a.StatusCode]
	}

	result := make([]ScalingActivity, 0, len(b.activities))

	if groupName != "" {
		live := b.groups.Has(groupName)
		if !live && !includeDeleted {
			return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
		}

		result = appendMatching(result, b.activities[groupName], matches)

		if includeDeleted {
			for i := range b.deletedActivities {
				if b.deletedActivities[i].AutoScalingGroupName == groupName && matches(&b.deletedActivities[i]) {
					result = append(result, b.deletedActivities[i])
				}
			}
		}

		return result, nil
	}

	for _, acts := range b.activities {
		result = appendMatching(result, acts, matches)
	}

	if includeDeleted {
		result = appendMatching(result, b.deletedActivities, matches)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ActivityID < result[j].ActivityID
	})

	return result, nil
}

func appendMatching(dst, src []ScalingActivity, matches func(*ScalingActivity) bool) []ScalingActivity {
	for i := range src {
		if matches(&src[i]) {
			dst = append(dst, src[i])
		}
	}

	return dst
}
