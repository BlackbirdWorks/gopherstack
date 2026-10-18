package networkfirewall

import (
	"context"
	"encoding/json"
)

// PutResourcePolicyInput is the PutResourcePolicy request.
type PutResourcePolicyInput struct {
	ResourceArn string `json:"ResourceArn"`
	Policy      string `json:"Policy"`
}

// EmptyOutput is an empty JSON object response.
type EmptyOutput struct{}

// DescribeResourcePolicyInput is the DescribeResourcePolicy request.
type DescribeResourcePolicyInput struct {
	ResourceArn string `json:"ResourceArn"`
}

// DescribeResourcePolicyOutput is the DescribeResourcePolicy response.
type DescribeResourcePolicyOutput struct {
	Policy string `json:"Policy"`
}

// DeleteResourcePolicyInput is the DeleteResourcePolicy request.
type DeleteResourcePolicyInput = DescribeResourcePolicyInput

func validatePolicyDocument(doc string) error {
	var parsed struct {
		Statement json.RawMessage `json:"Statement"`
	}

	if err := json.Unmarshal([]byte(doc), &parsed); err != nil || len(parsed.Statement) == 0 {
		return invalidResourcePolicy("The policy must be a JSON document with a Statement")
	}

	return nil
}

// shareableLocked reports whether arn names a policy or rule group, the only shareable resources.
func (b *InMemoryBackend) shareableLocked(resourceARN string) bool {
	return b.policies.Has(resourceARN) || b.ruleGroups.Has(resourceARN)
}

// PutResourcePolicy attaches a sharing policy to a firewall policy or rule group.
func (b *InMemoryBackend) PutResourcePolicy(_ context.Context, in *PutResourcePolicyInput) (*EmptyOutput, error) {
	if in.ResourceArn == "" || in.Policy == "" {
		return nil, invalid("ResourceArn and Policy are required")
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	if !b.shareableLocked(in.ResourceArn) {
		return nil, notFound("resource", in.ResourceArn)
	}

	if err := validatePolicyDocument(in.Policy); err != nil {
		return nil, err
	}

	b.resourcePolicies.Put(&resourcePolicyRecord{ResourceARN: in.ResourceArn, Policy: in.Policy})

	return &EmptyOutput{}, nil
}

// DescribeResourcePolicy returns the policy attached to a resource.
func (b *InMemoryBackend) DescribeResourcePolicy(
	_ context.Context, in *DescribeResourcePolicyInput,
) (*DescribeResourcePolicyOutput, error) {
	b.mu.RLock("DescribeResourcePolicy")
	defer b.mu.RUnlock()

	rec, ok := b.resourcePolicies.Get(in.ResourceArn)
	if !ok {
		return nil, notFound("resource policy", in.ResourceArn)
	}

	return &DescribeResourcePolicyOutput{Policy: rec.Policy}, nil
}

// DeleteResourcePolicy removes the policy attached to a resource.
func (b *InMemoryBackend) DeleteResourcePolicy(_ context.Context, in *DeleteResourcePolicyInput) (*EmptyOutput, error) {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if !b.resourcePolicies.Delete(in.ResourceArn) {
		return nil, notFound("resource policy", in.ResourceArn)
	}

	return &EmptyOutput{}, nil
}
