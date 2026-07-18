package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type createDeliveryInput struct {
	Tags                   map[string]string `json:"tags"`
	DeliverySourceName     string            `json:"deliverySourceName"`
	DeliveryDestinationArn string            `json:"deliveryDestinationArn"`
}

type createDeliveryOutput struct {
	Delivery *Delivery `json:"delivery,omitempty"`
}

// --- DescribeDeliveries ---.
type describeDeliveriesInput struct {
	NextToken string `json:"nextToken"`
	Limit     int    `json:"limit"`
}

type describeDeliveriesOutput struct {
	NextToken  string     `json:"nextToken,omitempty"`
	Deliveries []Delivery `json:"deliveries"`
}

// --- GetDelivery ---.
type getDeliveryInput struct {
	ID string `json:"id"`
}

type getDeliveryOutput struct {
	Delivery *Delivery `json:"delivery,omitempty"`
}

// --- DeleteDelivery ---.
type deleteDeliveryInput struct {
	ID string `json:"id"`
}

type deleteDeliveryOutput struct{}

func (h *Handler) handleCreateDelivery(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
	var input createDeliveryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	delivery, err := h.Backend.CreateDelivery(input.DeliverySourceName, input.DeliveryDestinationArn, input.Tags)
	if err != nil {
		return nil, err
	}

	return &createDeliveryOutput{Delivery: delivery}, nil
}

func (h *Handler) handleDescribeDeliveries(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input describeDeliveriesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	deliveries, next, err := h.Backend.DescribeDeliveries(input.Limit, input.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeDeliveriesOutput{Deliveries: deliveries, NextToken: next}, nil
}

func (h *Handler) handleGetDelivery(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
	var input getDeliveryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	d, err := h.Backend.GetDelivery(input.ID)
	if err != nil {
		return nil, err
	}

	return &getDeliveryOutput{Delivery: d}, nil
}

func (h *Handler) handleDeleteDelivery(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
	var input deleteDeliveryInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.DeleteDelivery(input.ID); err != nil {
		return nil, err
	}

	return &deleteDeliveryOutput{}, nil
}

type putDeliveryDestinationInputFull struct {
	Name         string            `json:"name"`
	OutputFormat string            `json:"outputFormat,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	Config       struct {
		DestinationResourceArn string `json:"destinationResourceArn"`
	} `json:"deliveryDestinationConfiguration"`
}

func (h *Handler) handlePutDeliveryDestination(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putDeliveryDestinationInputFull
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		dest, err := b.PutDeliveryDestination(in.Name, in.Config.DestinationResourceArn, in.OutputFormat, in.Tags)
		if err != nil {
			return nil, err
		}

		return map[string]any{keyDeliveryDestination: map[string]any{
			keyName:        dest.Name,
			keyArn:         dest.Arn,
			"outputFormat": dest.OutputFormat,
		}}, nil
	}

	return map[string]any{keyDeliveryDestination: map[string]any{}}, nil
}

type getDeliveryDestinationInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleGetDeliveryDestination(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getDeliveryDestinationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		dest, err := b.GetDeliveryDestination(in.Name)
		if err != nil {
			return nil, err
		}

		return map[string]any{keyDeliveryDestination: map[string]any{
			keyName:        dest.Name,
			keyArn:         dest.Arn,
			"outputFormat": dest.OutputFormat,
		}}, nil
	}

	return map[string]any{keyDeliveryDestination: map[string]any{}}, nil
}

func (h *Handler) handleDescribeDeliveryDestinations(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	if b := cwlBackend(h); b != nil {
		dests := b.DescribeDeliveryDestinations()
		out := make([]map[string]any, 0, len(dests))
		for _, d := range dests {
			out = append(out, map[string]any{keyName: d.Name, keyArn: d.Arn})
		}

		return map[string]any{"deliveryDestinations": out}, nil
	}

	return map[string]any{"deliveryDestinations": []any{}}, nil
}

type deleteDeliveryDestinationInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleDeleteDeliveryDestination(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteDeliveryDestinationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteDeliveryDestination(in.Name); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

type putDeliveryDestinationPolicyInput struct {
	DeliveryDestinationName   string `json:"deliveryDestinationName"`
	DeliveryDestinationPolicy string `json:"deliveryDestinationPolicy"`
}

func (h *Handler) handlePutDeliveryDestinationPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putDeliveryDestinationPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.PutDeliveryDestinationPolicy(in.DeliveryDestinationName, in.DeliveryDestinationPolicy); err != nil {
			return nil, err
		}
	}

	return map[string]any{"policy": map[string]any{
		"deliveryDestinationPolicy": in.DeliveryDestinationPolicy,
	}}, nil
}

type getDeliveryDestinationPolicyInput struct {
	DeliveryDestinationName string `json:"deliveryDestinationName"`
}

func (h *Handler) handleGetDeliveryDestinationPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getDeliveryDestinationPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	policy := ""
	if b := cwlBackend(h); b != nil {
		p, err := b.GetDeliveryDestinationPolicy(in.DeliveryDestinationName)
		if err != nil {
			return nil, err
		}
		policy = p
	}

	return map[string]any{"policy": map[string]any{"deliveryDestinationPolicy": policy}}, nil
}

type deleteDeliveryDestinationPolicyInput struct {
	DeliveryDestinationName string `json:"deliveryDestinationName"`
}

func (h *Handler) handleDeleteDeliveryDestinationPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteDeliveryDestinationPolicyInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteDeliveryDestinationPolicy(in.DeliveryDestinationName); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

type putDeliverySourceInput struct {
	Tags         map[string]string `json:"tags,omitempty"`
	Name         string            `json:"name"`
	LogType      string            `json:"logType,omitempty"`
	ResourceArns []string          `json:"resourceArns,omitempty"`
}

func (h *Handler) handlePutDeliverySource(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putDeliverySourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		src, err := b.PutDeliverySource(in.Name, in.LogType, in.ResourceArns, in.Tags)
		if err != nil {
			return nil, err
		}

		return map[string]any{keyDeliverySource: map[string]any{
			keyName: src.Name,
			keyArn:  src.Arn,
		}}, nil
	}

	return map[string]any{keyDeliverySource: map[string]any{}}, nil
}

type getDeliverySourceInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleGetDeliverySource(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getDeliverySourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		src, err := b.GetDeliverySource(in.Name)
		if err != nil {
			return nil, err
		}

		return map[string]any{keyDeliverySource: map[string]any{
			keyName: src.Name,
			keyArn:  src.Arn,
		}}, nil
	}

	return map[string]any{keyDeliverySource: map[string]any{}}, nil
}

func (h *Handler) handleDescribeDeliverySources(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	if b := cwlBackend(h); b != nil {
		srcs := b.DescribeDeliverySources()
		out := make([]map[string]any, 0, len(srcs))
		for _, s := range srcs {
			out = append(out, map[string]any{keyName: s.Name, keyArn: s.Arn})
		}

		return map[string]any{"deliverySources": out}, nil
	}

	return map[string]any{"deliverySources": []any{}}, nil
}

type deleteDeliverySourceInput struct {
	Name string `json:"name"`
}

func (h *Handler) handleDeleteDeliverySource(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteDeliverySourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteDeliverySource(in.Name); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

type updateDeliveryConfigurationInput struct {
	ID             string   `json:"id"`
	FieldDelimiter string   `json:"fieldDelimiter,omitempty"`
	RecordFields   []string `json:"recordFields,omitempty"`
}

func (h *Handler) handleUpdateDeliveryConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in updateDeliveryConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.UpdateDeliveryConfiguration(in.ID, in.FieldDelimiter, in.RecordFields); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}
