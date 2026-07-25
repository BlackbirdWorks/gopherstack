package applicationautoscaling

import (
	"fmt"
	"time"
)

// GetPredictiveScalingForecast returns simulated hourly forecast data for the requested
// policy over the given time window. It verifies the associated scaling policy exists.
func (b *InMemoryBackend) GetPredictiveScalingForecast(
	serviceNamespace, resourceID, scalableDimension, policyName string,
	startTime, endTime time.Time,
) (*CapacityForecastData, []LoadForecastData, time.Time, error) {
	if !endTime.After(startTime) {
		return nil, nil, time.Time{}, fmt.Errorf("%w: EndTime must be after StartTime", ErrValidation)
	}

	if endTime.Sub(startTime) > maxForecastWindow {
		return nil, nil, time.Time{}, fmt.Errorf(
			"%w: forecast window must not exceed 14 days",
			ErrValidation,
		)
	}

	b.mu.RLock("GetPredictiveScalingForecast")
	defer b.mu.RUnlock()

	key := policyNameKey(serviceNamespace, resourceID, scalableDimension, policyName)

	group := b.policiesByName.Get(key)
	if len(group) == 0 {
		// GetPredictiveScalingForecast's modeled error set is
		// {InternalServiceException, ValidationException} only --
		// ObjectNotFoundException is NOT modeled for this op (confirmed
		// against awsAwsjson11_deserializeOpErrorGetPredictiveScalingForecast
		// in the vendored SDK), unlike every other op keyed by policy/target
		// identity. A real client's typed-error matching on
		// ObjectNotFoundException would never fire here, so an unknown
		// policy is reported as ValidationException instead.
		return nil, nil, time.Time{}, fmt.Errorf(
			"%w: scaling policy %s not found for %s/%s/%s",
			ErrValidation, policyName, serviceNamespace, resourceID, scalableDimension,
		)
	}

	p := group[0]
	if p.PolicyType != policyTypePredictiveScaling {
		return nil, nil, time.Time{}, fmt.Errorf(
			"%w: GetPredictiveScalingForecast is only supported for PredictiveScaling policies; policy %s has type %s",
			ErrValidation, policyName, p.PolicyType,
		)
	}

	// Build hourly data points in [startTime, endTime).
	// Truncate always rounds down; if startTime is not on an exact hour boundary
	// the truncated value precedes startTime, so we advance by one hour.
	// When startTime is exactly on an hour boundary, truncation is a no-op and
	// the condition is false, keeping the boundary as the first point.
	start := startTime.Truncate(time.Hour)
	if start.Before(startTime) {
		start = start.Add(time.Hour)
	}

	// Preallocate with the exact known capacity to avoid slice growth.
	numPoints := max(0, int(endTime.Sub(start)/time.Hour))

	timestamps := make([]time.Time, 0, numPoints)

	for t := start; t.Before(endTime); t = t.Add(time.Hour) {
		timestamps = append(timestamps, t)
	}

	values := make([]float64, len(timestamps))
	for i := range values {
		values[i] = 10.0
	}

	capacity := &CapacityForecastData{
		Timestamps: timestamps,
		Values:     values,
	}

	load := []LoadForecastData{
		{
			Timestamps:          timestamps,
			Values:              values,
			MetricSpecification: fmt.Sprintf("%s/%s/%s", serviceNamespace, resourceID, scalableDimension),
		},
	}

	return capacity, load, time.Now().UTC(), nil
}
