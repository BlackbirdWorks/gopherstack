package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type putDestinationInput struct {
	DestinationName string `json:"destinationName"`
	TargetArn       string `json:"targetArn"`
	RoleArn         string `json:"roleArn"`
}

func (h *Handler) handlePutDestination(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putDestinationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		dest, err := b.PutDestination(in.DestinationName, in.TargetArn, in.RoleArn)
		if err != nil {
			return nil, err
		}

		return map[string]any{"destination": destinationWireShape(dest)}, nil
	}

	return map[string]any{"destination": map[string]any{
		keyDestinationName: in.DestinationName,
		keyTargetArn:       in.TargetArn,
		keyRoleArn:         in.RoleArn,
		keyArn:             "",
	}}, nil
}

// destinationWireShape maps a CWLDestination to the full AWS wire shape
// (aws-sdk-go-v2 types.Destination: accessPolicy, arn, creationTime,
// destinationName, roleArn, targetArn), including accessPolicy and
// creationTime, which the hand-rolled response maps in this file previously
// omitted.
func destinationWireShape(d *CWLDestination) map[string]any {
	return map[string]any{
		keyDestinationName: d.DestinationName,
		keyTargetArn:       d.TargetArn,
		keyRoleArn:         d.RoleArn,
		keyArn:             d.Arn,
		keyAccessPolicy:    d.AccessPolicy,
		keyCreationTime:    d.CreatedAt.UnixMilli(),
	}
}

type putDestinationPolicyInput struct {
	DestinationName string `json:"destinationName"`
	AccessPolicy    string `json:"accessPolicy"`
}

func (h *Handler) handlePutDestinationPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putDestinationPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.PutDestinationPolicy(in.DestinationName, in.AccessPolicy); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

type describeDestinationsInput struct {
	DestinationNamePrefix string `json:"DestinationNamePrefix,omitempty"`
}

func (h *Handler) handleDescribeDestinations(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in describeDestinationsInput
	_ = json.Unmarshal(body, &in)

	if b := cwlBackend(h); b != nil {
		dests := b.DescribeDestinations(in.DestinationNamePrefix)
		out := make([]map[string]any, 0, len(dests))
		for i := range dests {
			out = append(out, destinationWireShape(&dests[i]))
		}

		return map[string]any{"destinations": out}, nil
	}

	return map[string]any{"destinations": []any{}}, nil
}

type deleteDestinationInput struct {
	DestinationName string `json:"destinationName"`
}

func (h *Handler) handleDeleteDestination(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteDestinationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteDestination(in.DestinationName); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}
