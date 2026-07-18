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

	if err := h.Backend.GetPredictiveScalingForecast(groupName); err != nil {
		return nil, err
	}

	// AWS's actual forecast is produced by a statistical model trained on CloudWatch
	// history, which this emulator has no equivalent of. Rather than return an
	// all-empty (and required-field-violating) response, project a flat series at the
	// group's current DesiredCapacity so callers get well-shaped, non-empty,
	// real-derived data. See PARITY.md for the documented simplification.
	groups, err := h.Backend.DescribeAutoScalingGroups([]string{groupName})
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

	return &getPredictiveScalingForecastResponse{
		Xmlns: autoscalingXMLNS,
		Result: getPredictiveScalingForecastResult{
			LoadForecast:     xmlLoadForecastList{Members: []xmlLoadForecast{series}},
			CapacityForecast: xmlCapacityForecast(series),
			UpdateTime:       now.Format(time.RFC3339),
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-get-predictive-scaling-forecast"},
	}, nil
}

type xmlCapacityForecast struct {
	Timestamps xmlStringValueList `xml:"Timestamps"`
	Values     xmlStringValueList `xml:"Values"`
}

// xmlLoadForecast mirrors the Timestamps/Values pair shared by CapacityForecast and
// each entry of LoadForecast. A full MetricSpecification projection is out of scope
// (see PARITY.md); the Timestamps/Values series is real, derived data.
type xmlLoadForecast struct {
	Timestamps xmlStringValueList `xml:"Timestamps"`
	Values     xmlStringValueList `xml:"Values"`
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
