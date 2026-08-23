package applicationautoscaling

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type getPredictiveScalingForecastInput struct {
	// StartTime/EndTime are AWS JSON-protocol timestamps (Unix epoch-seconds
	// JSON numbers, not ISO8601 strings) and are required by the real API, so
	// they are pointers here to distinguish "field absent" from "field zero".
	StartTime         *float64 `json:"StartTime"`
	EndTime           *float64 `json:"EndTime"`
	ServiceNamespace  string   `json:"ServiceNamespace"`
	ResourceID        string   `json:"ResourceId"`
	ScalableDimension string   `json:"ScalableDimension"`
	PolicyName        string   `json:"PolicyName"`
}

type capacityForecastOutput struct {
	Timestamps []float64 `json:"Timestamps"`
	Values     []float64 `json:"Values"`
}

type loadForecastOutput struct {
	MetricSpecification map[string]any `json:"MetricSpecification,omitempty"`
	Timestamps          []float64      `json:"Timestamps"`
	Values              []float64      `json:"Values"`
}

type getPredictiveScalingForecastOutput struct {
	CapacityForecast *capacityForecastOutput `json:"CapacityForecast"`
	LoadForecast     []loadForecastOutput    `json:"LoadForecast"`
	UpdateTime       float64                 `json:"UpdateTime"`
}

func (h *Handler) handleGetPredictiveScalingForecast(
	_ context.Context,
	in *getPredictiveScalingForecastInput,
) (*getPredictiveScalingForecastOutput, error) {
	if in.StartTime == nil {
		return nil, fmt.Errorf("%w: StartTime is required", ErrValidation)
	}

	if in.EndTime == nil {
		return nil, fmt.Errorf("%w: EndTime is required", ErrValidation)
	}

	startTime := *parseEpochSeconds(in.StartTime)
	endTime := *parseEpochSeconds(in.EndTime)

	capacity, load, updateTime, err := h.Backend.GetPredictiveScalingForecast(
		in.ServiceNamespace, in.ResourceID, in.ScalableDimension, in.PolicyName,
		startTime, endTime,
	)
	if err != nil {
		return nil, err
	}

	capTS := make([]float64, len(capacity.Timestamps))
	for i, ts := range capacity.Timestamps {
		capTS[i] = awstime.Epoch(ts)
	}

	loadOut := make([]loadForecastOutput, len(load))
	for i, lf := range load {
		ts := make([]float64, len(lf.Timestamps))
		for j, t := range lf.Timestamps {
			ts[j] = awstime.Epoch(t)
		}

		loadOut[i] = loadForecastOutput{
			Timestamps:          ts,
			Values:              lf.Values,
			MetricSpecification: lf.MetricSpecification,
		}
	}

	return &getPredictiveScalingForecastOutput{
		CapacityForecast: &capacityForecastOutput{
			Timestamps: capTS,
			Values:     capacity.Values,
		},
		LoadForecast: loadOut,
		UpdateTime:   awstime.Epoch(updateTime),
	}, nil
}
