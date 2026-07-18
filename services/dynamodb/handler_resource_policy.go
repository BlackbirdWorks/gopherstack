// Package dynamodb implements the AWS DynamoDB mock service.
// handler_resource_policy.go implements the wire-JSON handlers for
// Get/Put/DeleteResourcePolicy. Routing (dispatchExtraOps) stays in
// handler.go; these are the leaf implementations it calls into. Backend
// logic lives in resource_policy.go.
package dynamodb

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkDDB "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type resourcePolicyInput struct {
	ResourceArn string `json:"ResourceArn"`
	Policy      string `json:"Policy,omitempty"`
}

type resourcePolicyOutput struct {
	Policy     string `json:"Policy,omitempty"`
	RevisionID string `json:"RevisionId,omitempty"`
}

type deleteResourcePolicyInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type deleteResourcePolicyOutput struct {
	RevisionID string `json:"RevisionId,omitempty"`
}

func (h *DynamoDBHandler) handleDeleteResourcePolicy(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req deleteResourcePolicyInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	_, err := h.Backend.DeleteResourcePolicy(ctx, &sdkDDB.DeleteResourcePolicyInput{
		ResourceArn: &req.ResourceArn,
	})
	if err != nil {
		return nil, err
	}

	return &deleteResourcePolicyOutput{}, nil
}

func (h *DynamoDBHandler) handleGetResourcePolicy(ctx context.Context, body []byte) (any, error) {
	var req resourcePolicyInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.GetResourcePolicy(ctx, &sdkDDB.GetResourcePolicyInput{
		ResourceArn: &req.ResourceArn,
	})
	if err != nil {
		return nil, err
	}

	resp := &resourcePolicyOutput{}
	if out != nil {
		resp.Policy = aws.ToString(out.Policy)
		resp.RevisionID = aws.ToString(out.RevisionId)
	}

	return resp, nil
}

func (h *DynamoDBHandler) handlePutResourcePolicy(ctx context.Context, body []byte) (any, error) {
	var req resourcePolicyInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.PutResourcePolicy(ctx, &sdkDDB.PutResourcePolicyInput{
		ResourceArn: &req.ResourceArn,
		Policy:      &req.Policy,
	})
	if err != nil {
		return nil, err
	}

	resp := &resourcePolicyOutput{}
	if out != nil {
		resp.RevisionID = aws.ToString(out.RevisionId)
	}

	return resp, nil
}
