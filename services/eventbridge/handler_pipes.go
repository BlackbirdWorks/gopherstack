package eventbridge

import (
	"context"
	"encoding/json"
)

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

			return h.Backend.DescribePipe(ctx, input.Name)
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

			return &struct {
				NextToken string `json:"NextToken,omitempty"`
				Pipes     []Pipe `json:"Pipes"`
			}{Pipes: pipes, NextToken: next}, nil
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
