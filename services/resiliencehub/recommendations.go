package resiliencehub

// The four recommendation families below (SOP/alarm/test/component) are the
// *outputs* of AWS's proprietary resiliency-analysis engine, mapping
// specific resource configurations to a curated library of standard-
// operating-procedure/alarm/test recommendations. None of this content is
// derivable from the SDK (see PARITY.md's honest-gap section) -- every
// List*Recommendations op below validates its assessmentArn against real
// backend state and then returns an honestly EMPTY list, never fabricated
// recommendation text.

// ListAlarmRecommendations always returns an empty list.
func (b *InMemoryBackend) ListAlarmRecommendations(assessmentArn string) error {
	b.mu.RLock("ListAlarmRecommendations")
	defer b.mu.RUnlock()

	if _, ok := b.resolveAssessmentLocked(assessmentArn); !ok {
		return notFoundError(resourceAssessment, assessmentArn)
	}

	return nil
}

// ListSopRecommendations always returns an empty list.
func (b *InMemoryBackend) ListSopRecommendations(assessmentArn string) error {
	b.mu.RLock("ListSopRecommendations")
	defer b.mu.RUnlock()

	if _, ok := b.resolveAssessmentLocked(assessmentArn); !ok {
		return notFoundError(resourceAssessment, assessmentArn)
	}

	return nil
}

// ListTestRecommendations always returns an empty list.
func (b *InMemoryBackend) ListTestRecommendations(assessmentArn string) error {
	b.mu.RLock("ListTestRecommendations")
	defer b.mu.RUnlock()

	if _, ok := b.resolveAssessmentLocked(assessmentArn); !ok {
		return notFoundError(resourceAssessment, assessmentArn)
	}

	return nil
}

// ListAppComponentRecommendations always returns an empty list.
func (b *InMemoryBackend) ListAppComponentRecommendations(assessmentArn string) error {
	b.mu.RLock("ListAppComponentRecommendations")
	defer b.mu.RUnlock()

	if _, ok := b.resolveAssessmentLocked(assessmentArn); !ok {
		return notFoundError(resourceAssessment, assessmentArn)
	}

	return nil
}

// BatchUpdateRecommendationStatus includes/excludes operational
// recommendations by ReferenceId. Since this backend never generates real
// recommendations (see the four List* ops above), every entry submitted
// necessarily fails to resolve -- an honest, real outcome (every entry lands
// in FailedEntries with a real "not found" reason), not a fabricated success.
func (b *InMemoryBackend) BatchUpdateRecommendationStatus(
	appArn string, entries []updateRecommendationStatusRequestEntryWire,
) (*App, []batchUpdateRecommendationStatusFailedEntryWire, error) {
	b.mu.RLock("BatchUpdateRecommendationStatus")
	defer b.mu.RUnlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, nil, notFoundError(resourceApp, appArn)
	}

	failed := make([]batchUpdateRecommendationStatusFailedEntryWire, 0, len(entries))
	for _, e := range entries {
		failed = append(failed, batchUpdateRecommendationStatusFailedEntryWire{
			EntryID:      e.EntryID,
			ErrorMessage: "recommendation not found: no recommendation engine output exists for referenceId " + e.ReferenceID,
		})
	}

	return a.clone(), failed, nil
}
