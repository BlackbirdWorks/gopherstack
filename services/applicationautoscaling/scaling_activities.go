package applicationautoscaling

import (
	"slices"
)

// DescribeScalingActivitiesFilter carries optional filters for DescribeScalingActivities.
type DescribeScalingActivitiesFilter struct {
	// ServiceNamespace limits results to this namespace when non-empty.
	ServiceNamespace string
	// ResourceID limits results to this resource when non-empty.
	ResourceID string
	// ScalableDimension limits results to this dimension when non-empty.
	ScalableDimension string
	// NextToken is the opaque pagination cursor returned by a prior call.
	NextToken string
	// MaxResults, when > 0, limits the number of returned items. Capped at maxDescribeResults.
	MaxResults int32
	// IncludeNotScaledActivities is accepted for wire completeness; see the
	// doc comment on [InMemoryBackend.DescribeScalingActivities].
	IncludeNotScaledActivities bool
}

// DescribeScalingActivities returns recorded scaling activities filtered by the
// optional fields in f, most recent first, with pagination.
// Returns ErrInvalidNextToken if f.NextToken fails to decode.
//
// f.IncludeNotScaledActivities is accepted for wire completeness but has no
// observable effect: gopherstack has no metric-evaluation loop capable of
// producing a "not scaled" activity (every recorded activity is a real
// capacity-changing event), so there is never anything for the flag to
// include or exclude -- this is a verified-vacuous gap, not an unimplemented
// filter (see PARITY.md).
func (b *InMemoryBackend) DescribeScalingActivities(
	f DescribeScalingActivitiesFilter,
) ([]*ScalingActivity, string, error) {
	b.mu.RLock("DescribeScalingActivities")
	defer b.mu.RUnlock()

	out := make([]*ScalingActivity, 0, len(b.scalingActivities))

	for _, a := range slices.Backward(b.scalingActivities) {
		if f.ServiceNamespace != "" && a.ServiceNamespace != f.ServiceNamespace {
			continue
		}

		if f.ResourceID != "" && a.ResourceID != f.ResourceID {
			continue
		}

		if f.ScalableDimension != "" && a.ScalableDimension != f.ScalableDimension {
			continue
		}

		cp := *a
		out = append(out, &cp)
	}

	return paginate(out, f.MaxResults, f.NextToken, func(a *ScalingActivity) string {
		return a.ActivityID
	})
}
