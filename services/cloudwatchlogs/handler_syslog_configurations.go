package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type putSyslogConfigurationInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
	VpcEndpointID      string `json:"vpcEndpointId,omitempty"`
}

func (h *Handler) handlePutSyslogConfiguration(ctx context.Context, body []byte) (any, error) {
	var in putSyslogConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	b := cwlBackend(h)
	if b == nil {
		return struct{}{}, nil
	}

	if _, err := b.PutSyslogConfiguration(ctx, in.LogGroupIdentifier, in.VpcEndpointID); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

type deleteSyslogConfigurationInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
	VpcEndpointID      string `json:"vpcEndpointId,omitempty"`
}

func (h *Handler) handleDeleteSyslogConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteSyslogConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteSyslogConfiguration(in.LogGroupIdentifier, in.VpcEndpointID); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

type listSyslogConfigurationsInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier,omitempty"`
	VpcEndpointID      string `json:"vpcEndpointId,omitempty"`
	NextToken          string `json:"nextToken,omitempty"`
	MaxResults         int32  `json:"maxResults,omitempty"`
}

func (h *Handler) handleListSyslogConfigurations(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in listSyslogConfigurationsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	b := cwlBackend(h)
	if b == nil {
		return map[string]any{"syslogConfigurations": []any{}}, nil
	}

	configs, nextToken := b.ListSyslogConfigurations(
		in.LogGroupIdentifier, in.VpcEndpointID, in.NextToken, int(in.MaxResults),
	)
	out := make([]map[string]any, 0, len(configs))

	for i := range configs {
		out = append(out, syslogConfigurationWireShape(&configs[i]))
	}

	resp := map[string]any{"syslogConfigurations": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, nil
}

// syslogConfigurationWireShape maps a SyslogConfiguration to the real
// types.SyslogConfiguration shape (createdAt/logGroupArn/sourceType/
// vpcEndpointId).
func syslogConfigurationWireShape(c *SyslogConfiguration) map[string]any {
	shape := map[string]any{
		"createdAt":   c.CreatedAt,
		"logGroupArn": c.LogGroupArn,
		"sourceType":  c.SourceType,
	}

	if c.VpcEndpointID != "" {
		shape["vpcEndpointId"] = c.VpcEndpointID
	}

	return shape
}
