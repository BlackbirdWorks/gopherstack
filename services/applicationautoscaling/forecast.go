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

	// Real AWS predictive scaling forecasts are produced by an ML model
	// trained on the resource's actual historical CloudWatch metric data.
	// gopherstack has no real metric history to forecast from, so returning
	// a fabricated curve here (previously a flat constant-10.0 line) would be
	// data a caller could mistake for a genuine forecast. Instead this
	// honestly returns no data points, which also matches real AWS's own
	// behavior for a predictive scaling policy that has not yet accumulated
	// enough history to produce a forecast (CapacityForecast/LoadForecast are
	// legitimately empty in that case). See PARITY.md gaps.
	capacity := &CapacityForecastData{
		Timestamps: []time.Time{},
		Values:     []float64{},
	}

	load := []LoadForecastData{
		{
			Timestamps:          []time.Time{},
			Values:              []float64{},
			MetricSpecification: fmt.Sprintf("%s/%s/%s", serviceNamespace, resourceID, scalableDimension),
		},
	}

	return capacity, load, time.Now().UTC(), nil
}
