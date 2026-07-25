package glue

import (
	"context"
)

// deleteResourcePolicyInput holds input for DeleteResourcePolicy.
type deleteResourcePolicyInput struct {
	ResourceArn string `json:"ResourceArn,omitempty"`
	PolicyHash  string `json:"PolicyHashCondition,omitempty"`
}

func (h *Handler) handleDeleteResourcePolicy(
	_ context.Context,
	in *deleteResourcePolicyInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteResourcePolicy(in.ResourceArn, in.PolicyHash)
}

// getResourcePoliciesInput holds input for GetResourcePolicies.
type getResourcePoliciesInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

// gluePolicyOut is a single policy entry in GetResourcePolicies' response list.
type gluePolicyOut struct {
	PolicyInJSON string  `json:"PolicyInJson,omitempty"`
	PolicyHash   string  `json:"PolicyHash,omitempty"`
	CreateTime   float64 `json:"CreateTime,omitempty"`
	UpdateTime   float64 `json:"UpdateTime,omitempty"`
}

// getResourcePoliciesOutput holds the result for GetResourcePolicies.
type getResourcePoliciesOutput struct {
	NextToken                       string          `json:"NextToken,omitempty"`
	GetResourcePoliciesResponseList []gluePolicyOut `json:"GetResourcePoliciesResponseList"`
}

func (h *Handler) handleGetResourcePolicies(
	_ context.Context,
	_ *getResourcePoliciesInput,
) (*getResourcePoliciesOutput, error) {
	entries := h.Backend.ListResourcePolicies()
	list := make([]gluePolicyOut, 0, len(entries))

	for _, e := range entries {
		list = append(list, gluePolicyOut{
			PolicyInJSON: e.Policy,
			PolicyHash:   e.Hash,
			CreateTime:   e.CreateTime,
			UpdateTime:   e.UpdateTime,
		})
	}

	return &getResourcePoliciesOutput{GetResourcePoliciesResponseList: list}, nil
}

// getResourcePolicyInput holds input for GetResourcePolicy.
type getResourcePolicyInput struct {
	ResourceArn string `json:"ResourceArn,omitempty"`
}

// getResourcePolicyOutput holds the result for GetResourcePolicy.
type getResourcePolicyOutput struct {
	PolicyInJSON string `json:"PolicyInJson"`
	PolicyHash   string `json:"PolicyHash,omitempty"`
}

func (h *Handler) handleGetResourcePolicy(
	_ context.Context,
	in *getResourcePolicyInput,
) (*getResourcePolicyOutput, error) {
	policy, hash, err := h.Backend.GetResourcePolicy(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &getResourcePolicyOutput{PolicyInJSON: policy, PolicyHash: hash}, nil
}

// putResourcePolicyInput holds input for PutResourcePolicy.
type putResourcePolicyInput struct {
	PolicyInJSON          string `json:"PolicyInJson"`
	ResourceArn           string `json:"ResourceArn,omitempty"`
	PolicyExistsCondition string `json:"PolicyExistsCondition,omitempty"`
	PolicyHashCondition   string `json:"PolicyHashCondition,omitempty"`
	EnableHybrid          string `json:"EnableHybrid,omitempty"`
}

// putResourcePolicyOutput holds the result for PutResourcePolicy.
type putResourcePolicyOutput struct {
	PolicyHash string `json:"PolicyHash"`
}

func (h *Handler) handlePutResourcePolicy(
	_ context.Context,
	in *putResourcePolicyInput,
) (*putResourcePolicyOutput, error) {
	hash, err := h.Backend.PutResourcePolicy(
		in.PolicyInJSON,
		in.ResourceArn,
		in.PolicyExistsCondition,
		in.PolicyHashCondition,
		in.EnableHybrid,
	)
	if err != nil {
		return nil, err
	}

	return &putResourcePolicyOutput{PolicyHash: hash}, nil
}
