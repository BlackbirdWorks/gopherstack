package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type createLogGroupInput struct {
	Tags          map[string]string `json:"tags,omitempty"`
	LogGroupName  string            `json:"logGroupName"`
	KmsKeyID      string            `json:"kmsKeyId,omitempty"`
	LogGroupClass string            `json:"logGroupClass,omitempty"`
}

type deleteLogGroupInput struct {
	LogGroupName string `json:"logGroupName"`
}

type describeLogGroupsInput struct {
	LogGroupNamePrefix string `json:"logGroupNamePrefix"`
	NextToken          string `json:"nextToken"`
	Limit              int    `json:"limit"`
}

type putRetentionPolicyInput struct {
	LogGroupName    string `json:"logGroupName"`
	RetentionInDays int32  `json:"retentionInDays"`
}

type deleteRetentionPolicyInput struct {
	LogGroupName string `json:"logGroupName"`
}

type associateKmsKeyInput struct {
	LogGroupName       string `json:"logGroupName"`
	ResourceIdentifier string `json:"resourceIdentifier"`
	KmsKeyID           string `json:"kmsKeyId"`
}

type associateKmsKeyOutput struct{}

type createLogGroupOutput struct{}

type deleteLogGroupOutput struct{}

type describeLogGroupsOutput struct {
	NextToken string     `json:"nextToken,omitempty"`
	LogGroups []LogGroup `json:"logGroups"`
}

type putRetentionPolicyOutput struct{}

type deleteRetentionPolicyOutput struct{}

// --- DisassociateKmsKey ---.
type disassociateKmsKeyInput struct {
	LogGroupName       string `json:"logGroupName"`
	ResourceIdentifier string `json:"resourceIdentifier"`
}

type disassociateKmsKeyOutput struct{}

// --- GetLogGroupFields ---.
type getLogGroupFieldsInput struct {
	Time               *int64 `json:"time"`
	LogGroupName       string `json:"logGroupName"`
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

type getLogGroupFieldsOutput struct {
	LogGroupFields []LogGroupField `json:"logGroupFields"`
}

// --- ListLogGroups ---.
type listLogGroupsInput struct {
	LogGroupNamePrefix string `json:"logGroupNamePrefix"`
	NextToken          string `json:"nextToken"`
	Limit              int    `json:"limit"`
}

func (h *Handler) logGroupActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateLogGroup": func(ctx context.Context, b []byte) (any, error) {
			var input createLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			group, err := h.Backend.CreateLogGroup(
				ctx,
				input.LogGroupName,
				input.LogGroupClass,
				input.KmsKeyID,
			)
			if err != nil {
				return nil, err
			}
			if len(input.Tags) > 0 {
				// Real clients read tags via ListTagsForResource(ARN), the
				// non-deprecated path -- ListTagsLogGroup(name) below is legacy.
				h.setTags(group.Arn, input.Tags)
				h.setTags(input.LogGroupName, input.Tags)
			}

			return &createLogGroupOutput{}, nil
		},
		"DeleteLogGroup": func(ctx context.Context, b []byte) (any, error) {
			var input deleteLogGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteLogGroup(ctx, input.LogGroupName); err != nil {
				return nil, err
			}

			return &deleteLogGroupOutput{}, nil
		},
		"DescribeLogGroups": func(ctx context.Context, b []byte) (any, error) {
			var input describeLogGroupsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			groups, next, err := h.Backend.DescribeLogGroups(
				ctx,
				input.LogGroupNamePrefix,
				input.NextToken,
				input.Limit,
			)
			if err != nil {
				return nil, err
			}

			return &describeLogGroupsOutput{LogGroups: groups, NextToken: next}, nil
		},
	}
}

func (h *Handler) retentionActions() map[string]actionFn {
	return map[string]actionFn{
		"PutRetentionPolicy": func(ctx context.Context, b []byte) (any, error) {
			var input putRetentionPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			days := input.RetentionInDays
			if err := h.Backend.SetRetentionPolicy(ctx, input.LogGroupName, &days); err != nil {
				return nil, err
			}

			return &putRetentionPolicyOutput{}, nil
		},
		"DeleteRetentionPolicy": func(ctx context.Context, b []byte) (any, error) {
			var input deleteRetentionPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.SetRetentionPolicy(ctx, input.LogGroupName, nil); err != nil {
				return nil, err
			}

			return &deleteRetentionPolicyOutput{}, nil
		},
	}
}

func (h *Handler) handleAssociateKmsKey(ctx context.Context, b []byte) (any, error) { //nolint:revive // existing issue.
	var input associateKmsKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.AssociateKmsKey(input.LogGroupName, input.ResourceIdentifier, input.KmsKeyID); err != nil {
		return nil, err
	}

	return &associateKmsKeyOutput{}, nil
}

func (h *Handler) handleDisassociateKmsKey(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input disassociateKmsKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.DisassociateKmsKey(input.LogGroupName, input.ResourceIdentifier); err != nil {
		return nil, err
	}

	return &disassociateKmsKeyOutput{}, nil
}

func (h *Handler) handleGetLogGroupFields(ctx context.Context, b []byte) (any, error) {
	var input getLogGroupFieldsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	name := input.LogGroupName
	if name == "" {
		name = normalizeLogGroupIdentifier(input.LogGroupIdentifier)
	}

	fields, err := h.Backend.GetLogGroupFields(ctx, name, input.Time)
	if err != nil {
		return nil, err
	}

	return &getLogGroupFieldsOutput{LogGroupFields: fields}, nil
}

func (h *Handler) handleListLogGroups(ctx context.Context, b []byte) (any, error) {
	var input listLogGroupsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	groups, next, err := h.Backend.ListLogGroups(ctx, input.LogGroupNamePrefix, input.NextToken, input.Limit)
	if err != nil {
		return nil, err
	}

	return &describeLogGroupsOutput{LogGroups: groups, NextToken: next}, nil
}

type putLogGroupDeletionProtectionInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
	DeletionProtected  bool   `json:"deletionProtected"`
}

func (h *Handler) handlePutLogGroupDeletionProtection(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putLogGroupDeletionProtectionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.SetLogGroupDeletionProtection(in.LogGroupIdentifier, in.DeletionProtected); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

// handleListAggregateLogGroupSummaries returns aggregate summaries derived from
// the real log groups and their stored events for the current region.
func (h *Handler) handleListAggregateLogGroupSummaries(
	ctx context.Context,
	_ []byte,
) (any, error) {
	b := cwlBackend(h)
	if b == nil {
		return map[string]any{"logGroupSummaries": []any{}}, nil
	}

	summaries := b.ListAggregateLogGroupSummaries(ctx)
	if summaries == nil {
		summaries = []AggregateLogGroupSummary{}
	}

	return map[string]any{"logGroupSummaries": summaries}, nil
}
