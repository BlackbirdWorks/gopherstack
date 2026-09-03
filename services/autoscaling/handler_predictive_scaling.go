package autoscaling

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"time"
)

// forecastHorizonHours and forecastIntervalHours bound the synthetic forecast series
// generated below: AWS forecasts up to 2 days ahead in hourly points.
const (
	forecastHorizonHours  = 48
	forecastIntervalHours = 1
)

func (h *Handler) handleGetPredictiveScalingForecast(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	policyName := vals.Get("PolicyName")

	if err := h.Backend.GetPredictiveScalingForecast(groupName); err != nil {
		return nil, err
	}

	// AWS's actual forecast is produced by a statistical model trained on CloudWatch
	// history, which this emulator has no equivalent of. Rather than return an
	// all-empty (and required-field-violating) response, project a flat series at the
	// group's current DesiredCapacity so callers get well-shaped, non-empty,
	// real-derived data. See PARITY.md for the documented simplification.
	groups, err := h.Backend.DescribeAutoScalingGroups([]string{groupName}, nil)
	if err != nil {
		return nil, err
	}

	desired := groups[0].DesiredCapacity
	now := time.Now().UTC()

	timestamps := make([]xmlStringValue, 0, forecastHorizonHours)
	values := make([]xmlStringValue, 0, forecastHorizonHours)

	for i := range forecastHorizonHours {
		timestamps = append(timestamps, xmlStringValue{
			Value: now.Add(time.Duration(i*forecastIntervalHours) * time.Hour).Format(time.RFC3339),
		})
		values = append(values, xmlStringValue{Value: strconv.FormatInt(int64(desired), 10)})
	}

	series := xmlLoadForecast{
		Timestamps: xmlStringValueList{Members: timestamps},
		Values:     xmlStringValueList{Members: values},
	}

	forecasts := loadForecastsForPolicy(h.Backend, groupName, policyName, series)

	return &getPredictiveScalingForecastResponse{
		Xmlns: autoscalingXMLNS,
		Result: getPredictiveScalingForecastResult{
			LoadForecast: xmlLoadForecastList{Members: forecasts},
			CapacityForecast: xmlCapacityForecast{
				Timestamps: series.Timestamps,
				Values:     series.Values,
			},
			UpdateTime: now.Format(time.RFC3339),
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-get-predictive-scaling-forecast"},
	}, nil
}

// loadForecastsForPolicy fills each LoadForecast entry's required
// MetricSpecification (types.go:2670, LoadForecast) from the policy's own
// already-parsed PredictiveScalingConfiguration.MetricSpecifications (see
// bd gopherstack-2uti) -- real, stored data, not fabricated. AWS returns one
// LoadForecast per configured metric specification; falls back to a single
// unlabeled series only if the policy or its predictive-scaling config can't
// be found (e.g. a stale/renamed PolicyName), matching this handler's
// existing no-hard-validation behavior for AutoScalingGroupName.
func loadForecastsForPolicy(
	b StorageBackend, groupName, policyName string, series xmlLoadForecast,
) []xmlLoadForecast {
	policies, err := b.DescribePolicies(groupName, []string{policyName}, nil)
	if err != nil || len(policies) == 0 || policies[0].PredictiveScalingConfiguration == nil {
		return []xmlLoadForecast{series}
	}

	specs := policies[0].PredictiveScalingConfiguration.MetricSpecifications
	if len(specs) == 0 {
		return []xmlLoadForecast{series}
	}

	out := make([]xmlLoadForecast, 0, len(specs))

	for i := range specs {
		entry := series
		spec := toXMLPredictiveScalingMetricSpecification(&specs[i])
		entry.MetricSpecification = &spec
		out = append(out, entry)
	}

	return out
}

type xmlCapacityForecast struct {
	Timestamps xmlStringValueList `xml:"Timestamps"`
	Values     xmlStringValueList `xml:"Values"`
}

// xmlLoadForecast mirrors the real types.LoadForecast (types.go:2649):
// MetricSpecification, Timestamps and Values are all required.
// MetricSpecification is filled by loadForecastsForPolicy from the policy's own
// stored PredictiveScalingConfiguration; Timestamps/Values are the naive flat
// projection documented in PARITY.md.
type xmlLoadForecast struct {
	MetricSpecification *xmlPredictiveScalingMetricSpecification `xml:"MetricSpecification"`
	Timestamps          xmlStringValueList                       `xml:"Timestamps"`
	Values              xmlStringValueList                       `xml:"Values"`
}

type xmlLoadForecastList struct {
	Members []xmlLoadForecast `xml:"member"`
}

type getPredictiveScalingForecastResult struct {
	UpdateTime       string              `xml:"UpdateTime"`
	CapacityForecast xmlCapacityForecast `xml:"CapacityForecast"`
	LoadForecast     xmlLoadForecastList `xml:"LoadForecast"`
}

type getPredictiveScalingForecastResponse struct {
	XMLName          xml.Name                           `xml:"GetPredictiveScalingForecastResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           getPredictiveScalingForecastResult `xml:"GetPredictiveScalingForecastResult"`
}
