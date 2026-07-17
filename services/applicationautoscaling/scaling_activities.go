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
}

// DescribeScalingActivities returns recorded scaling activities filtered by the
// optional fields in f, most recent first, with pagination.
func (b *InMemoryBackend) DescribeScalingActivities(f DescribeScalingActivitiesFilter) ([]*ScalingActivity, string) {
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
