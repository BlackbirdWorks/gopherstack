package eventbridge

import (
	"context"
	"encoding/json"
)

// pipeResponse is the handler-level DTO for Pipe objects. Timestamps are
// float64 Unix epoch seconds as required by the wire (a raw time.Time field
// would json.Marshal to an RFC3339 string instead) -- confirmed against
// aws-sdk-go-v2/service/pipes's deserializers.go, which parses both
// CreationTime and LastModifiedTime via smithytime.ParseEpochSeconds despite
// the SDK's stale "ISO-8601 format" doc comment on LastModifiedTime.
type pipeResponse struct {
	Arn              string  `json:"Arn"`
	CurrentState     string  `json:"CurrentState"`
	Description      string  `json:"Description,omitempty"`
	DesiredState     string  `json:"DesiredState"`
	EnrichmentArn    string  `json:"EnrichmentArn,omitempty"`
	Name             string  `json:"Name"`
	RoleArn          string  `json:"RoleArn"`
	SourceArn        string  `json:"SourceArn"`
	StateReason      string  `json:"StateReason,omitempty"`
	TargetArn        string  `json:"TargetArn"`
	CreationTime     float64 `json:"CreationTime,omitempty"`
	LastModifiedTime float64 `json:"LastModifiedTime,omitempty"`
}

func pipeToResponse(pipe *Pipe) *pipeResponse {
	if pipe == nil {
		return nil
	}

	resp := &pipeResponse{
		Arn:           pipe.Arn,
		CurrentState:  pipe.CurrentState,
		Description:   pipe.Description,
		DesiredState:  pipe.DesiredState,
		EnrichmentArn: pipe.EnrichmentArn,
		Name:          pipe.Name,
		RoleArn:       pipe.RoleArn,
		SourceArn:     pipe.SourceArn,
		StateReason:   pipe.StateReason,
		TargetArn:     pipe.TargetArn,
	}

	if !pipe.CreationTime.IsZero() {
		resp.CreationTime = timeToEpochSeconds(pipe.CreationTime)
	}

	if !pipe.LastModifiedTime.IsZero() {
		resp.LastModifiedTime = timeToEpochSeconds(pipe.LastModifiedTime)
	}

	return resp
}

func (h *Handler) pipesActions() map[string]actionFn {
	return map[string]actionFn{
		"CreatePipe": func(ctx context.Context, b []byte) (any, error) {
			var input CreatePipeInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			pipe, err := h.Backend.CreatePipe(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				Arn          string  `json:"Arn"`
				CurrentState string  `json:"CurrentState"`
				Name         string  `json:"Name"`
				CreationTime float64 `json:"CreationTime"`
			}{
				Arn:          pipe.Arn,
				CreationTime: timeToEpochSeconds(pipe.CreationTime),
				CurrentState: pipe.CurrentState,
				Name:         pipe.Name,
			}, nil
		},
		"DeletePipe": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return &struct{}{}, h.Backend.DeletePipe(ctx, input.Name)
		},
		"DescribePipe": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			pipe, err := h.Backend.DescribePipe(ctx, input.Name)
			if err != nil {
				return nil, err
			}

			return pipeToResponse(pipe), nil
		},
		"ListPipes": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			pipes, next, err := h.Backend.ListPipes(ctx, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}

			responses := make([]pipeResponse, len(pipes))
			for i := range pipes {
				responses[i] = *pipeToResponse(&pipes[i])
			}

			return &struct {
				NextToken string         `json:"NextToken,omitempty"`
				Pipes     []pipeResponse `json:"Pipes"`
			}{Pipes: responses, NextToken: next}, nil
		},
		"UpdatePipe": func(ctx context.Context, b []byte) (any, error) {
			var input UpdatePipeInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			pipe, err := h.Backend.UpdatePipe(ctx, input)
			if err != nil {
				return nil, err
			}

			return &struct {
				Arn              string  `json:"Arn"`
				CurrentState     string  `json:"CurrentState"`
				Name             string  `json:"Name"`
				LastModifiedTime float64 `json:"LastModifiedTime"`
			}{
				Arn:              pipe.Arn,
				CurrentState:     pipe.CurrentState,
				LastModifiedTime: timeToEpochSeconds(pipe.LastModifiedTime),
				Name:             pipe.Name,
			}, nil
		},
	}
}
