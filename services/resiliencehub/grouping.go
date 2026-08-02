package resiliencehub

// This whole family (StartResourceGroupingRecommendationTask /
// DescribeResourceGroupingRecommendationTask /
// ListResourceGroupingRecommendations / Accept/RejectResourceGroupingRecommendations)
// is a fifth, distinct ML feature: AWS suggests how to cluster ungrouped
// resources into new AppComponents. This is an ML output with no public
// derivation rule -- same fabrication risk as the main recommendation
// families (see PARITY.md). This backend implements the FULL real state
// machine (a real task record, real Pending/InProgress/Success transitions,
// real accept/reject bookkeeping) but always completes with ZERO generated
// recommendations, so there is nothing to fabricate confidence scores for.

// StartResourceGroupingRecommendationTask starts a new grouping-
// recommendation task for appArn. Real state (a genuine task record with
// real async transitions), honestly empty content (zero recommendations
// ever get generated).
func (b *InMemoryBackend) StartResourceGroupingRecommendationTask(appArn string) (*GroupingTask, error) {
	b.mu.Lock("StartResourceGroupingRecommendationTask")
	defer b.mu.Unlock()

	if _, ok := b.resolveAppLocked(appArn); !ok {
		return nil, notFoundError(resourceApp, appArn)
	}

	t := &GroupingTask{GroupingID: newGroupingID(), AppArn: appArn, Status: AsyncStatusPending}
	b.groupingTasks.Put(t)
	b.scheduleGroupingTask(t.GroupingID)

	return t.clone(), nil
}

// scheduleGroupingTask transitions a grouping task Pending -> Success. No
// recommendations are ever generated (see PARITY.md).
func (b *InMemoryBackend) scheduleGroupingTask(groupingID string) {
	b.work.After("GroupingTask", asyncTransitionDelay, func() {
		b.mu.Lock("GroupingTask-async")
		defer b.mu.Unlock()

		t, ok := b.groupingTasks.Get(groupingID)
		if !ok || t.Status != AsyncStatusPending {
			return
		}

		t.Status = AsyncStatusSuccess
	})
}

// DescribeResourceGroupingRecommendationTask returns the task identified by
// (appArn, groupingID).
func (b *InMemoryBackend) DescribeResourceGroupingRecommendationTask(appArn, groupingID string) (*GroupingTask, error) {
	b.mu.RLock("DescribeResourceGroupingRecommendationTask")
	defer b.mu.RUnlock()

	t, ok := b.groupingTasks.Get(groupingID)
	if !ok || t.AppArn != appArn {
		return nil, notFoundError(resourceGroupingTask, groupingID)
	}

	return t.clone(), nil
}

// ListResourceGroupingRecommendations always returns an empty list -- this
// backend's grouping tasks never generate real recommendations.
func (b *InMemoryBackend) ListResourceGroupingRecommendations(appArn string) error {
	b.mu.RLock("ListResourceGroupingRecommendations")
	defer b.mu.RUnlock()

	if _, ok := b.resolveAppLocked(appArn); !ok {
		return notFoundError(resourceApp, appArn)
	}

	return nil
}

// AcceptResourceGroupingRecommendations accepts recommendations by ID. Since
// no grouping recommendation is ever generated, every submitted ID
// necessarily fails to resolve -- a real, honest outcome, not a fabricated
// success.
func (b *InMemoryBackend) AcceptResourceGroupingRecommendations(
	appArn string, ids []string,
) ([]failedGroupingRecommendationEntryWire, error) {
	b.mu.RLock("AcceptResourceGroupingRecommendations")
	defer b.mu.RUnlock()

	if _, ok := b.resolveAppLocked(appArn); !ok {
		return nil, notFoundError(resourceApp, appArn)
	}

	return failGroupingEntries(ids), nil
}

// RejectResourceGroupingRecommendations rejects recommendations by ID --
// same honest-failure rationale as AcceptResourceGroupingRecommendations.
func (b *InMemoryBackend) RejectResourceGroupingRecommendations(
	appArn string, ids []string,
) ([]failedGroupingRecommendationEntryWire, error) {
	b.mu.RLock("RejectResourceGroupingRecommendations")
	defer b.mu.RUnlock()

	if _, ok := b.resolveAppLocked(appArn); !ok {
		return nil, notFoundError(resourceApp, appArn)
	}

	return failGroupingEntries(ids), nil
}

func failGroupingEntries(ids []string) []failedGroupingRecommendationEntryWire {
	out := make([]failedGroupingRecommendationEntryWire, 0, len(ids))
	for _, id := range ids {
		out = append(out, failedGroupingRecommendationEntryWire{
			GroupingRecommendationID: id,
			ErrorMessage:             "grouping recommendation not found: no resource-grouping ML output exists for id " + id,
		})
	}

	return out
}
