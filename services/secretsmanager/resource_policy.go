package secretsmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
)

// GetResourcePolicy retrieves the resource-based policy for a secret.
func (b *InMemoryBackend) GetResourcePolicy(
	ctx context.Context, input *GetResourcePolicyInput,
) (*GetResourcePolicyOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock(opGetResourcePolicy)
	defer b.mu.RUnlock()

	name := b.resolveName(region, input.SecretID)

	secret, ok := b.secretGet(region, name)
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	policy := b.resourcePoliciesStoreRO(region)[name]

	return &GetResourcePolicyOutput{
		ARN:            secret.ARN,
		Name:           secret.Name,
		ResourcePolicy: policy,
	}, nil
}

// PutResourcePolicy stores a resource-based policy for a secret.
func (b *InMemoryBackend) PutResourcePolicy(
	ctx context.Context, input *PutResourcePolicyInput,
) (*PutResourcePolicyOutput, error) {
	if input.ResourcePolicy == "" {
		return nil, fmt.Errorf("%w: ResourcePolicy must not be empty", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	name := b.resolveName(region, input.SecretID)

	secret, ok := b.secretGet(region, name)
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	if errs := validateResourcePolicyDocument(input.ResourcePolicy); len(errs) > 0 {
		return nil, ErrMalformedPolicyDocument
	}

	if input.BlockPublicPolicy != nil && *input.BlockPublicPolicy && policyAllowsPublicAccess(input.ResourcePolicy) {
		return nil, ErrPublicPolicyException
	}

	b.resourcePoliciesStore(region)[name] = input.ResourcePolicy

	return &PutResourcePolicyOutput{
		ARN:  secret.ARN,
		Name: secret.Name,
	}, nil
}

// DeleteResourcePolicy removes the resource-based policy from a secret.
func (b *InMemoryBackend) DeleteResourcePolicy(
	ctx context.Context, input *DeleteResourcePolicyInput,
) (*DeleteResourcePolicyOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	name := b.resolveName(region, input.SecretID)

	secret, ok := b.secretGet(region, name)
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	delete(b.resourcePoliciesStore(region), name)

	return &DeleteResourcePolicyOutput{
		ARN:  secret.ARN,
		Name: secret.Name,
	}, nil
}

// ValidateResourcePolicy validates a resource-based policy document for a secret.
// It performs basic structural validation and returns any detected issues.
func (b *InMemoryBackend) ValidateResourcePolicy(
	ctx context.Context,
	input *ValidateResourcePolicyInput,
) (*ValidateResourcePolicyOutput, error) {
	if input.ResourcePolicy == "" {
		return nil, fmt.Errorf("%w: ResourcePolicy must not be empty", ErrInvalidParameter)
	}

	// If a secret ID is provided, verify the secret exists.
	if input.SecretID != "" {
		region := getRegion(ctx, b.region)

		b.mu.RLock(opValidateResourcePolicy)
		defer b.mu.RUnlock()

		name := b.resolveName(region, input.SecretID)
		if !b.secretHas(region, name) {
			return nil, ErrSecretNotFound
		}
	}

	errs := validateResourcePolicyDocument(input.ResourcePolicy)
	if len(errs) == 0 && policyAllowsPublicAccess(input.ResourcePolicy) {
		errs = append(errs, PolicyValidationException{
			CheckName:    "PublicPolicyCheck",
			ErrorMessage: "The resource policy allows broad access to the secret through a wildcard principal.",
		})
	}

	return &ValidateResourcePolicyOutput{
		PolicyValidationPassed: len(errs) == 0,
		ValidationErrors:       errs,
	}, nil
}

func validateResourcePolicyDocument(policyStr string) []PolicyValidationException {
	var policy map[string]json.RawMessage
	if err := json.Unmarshal([]byte(policyStr), &policy); err != nil {
		return []PolicyValidationException{
			{
				CheckName:    "SyntaxCheck",
				ErrorMessage: "Policy document is not valid JSON: " + err.Error(),
			},
		}
	}

	var errs []PolicyValidationException

	if _, hasVersion := policy["Version"]; !hasVersion {
		errs = append(errs, PolicyValidationException{
			CheckName:    "VersionCheck",
			ErrorMessage: "Policy document must include a Version element.",
		})
	}

	if _, hasStatement := policy["Statement"]; !hasStatement {
		errs = append(errs, PolicyValidationException{
			CheckName:    "StatementCheck",
			ErrorMessage: "Policy document must include a Statement element.",
		})
	}

	return errs
}

type policyStatement struct {
	Effect    string          `json:"Effect"`
	Principal json.RawMessage `json:"Principal"`
	Condition json.RawMessage `json:"Condition"`
}

// policyAllowsPublicAccess reports whether an unconditioned Allow statement
// grants a wildcard principal.
func policyAllowsPublicAccess(policyStr string) bool {
	var doc struct {
		Statement json.RawMessage `json:"Statement"`
	}
	if json.Unmarshal([]byte(policyStr), &doc) != nil {
		return false
	}

	var stmts []policyStatement
	if json.Unmarshal(doc.Statement, &stmts) != nil {
		var one policyStatement
		if json.Unmarshal(doc.Statement, &one) != nil {
			return false
		}

		stmts = []policyStatement{one}
	}

	for _, st := range stmts {
		if st.Effect == "Allow" && len(st.Condition) == 0 && principalIsWildcard(st.Principal) {
			return true
		}
	}

	return false
}

func principalIsWildcard(raw json.RawMessage) bool {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s == "*"
	}

	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return false
	}

	aws, ok := m["AWS"]
	if !ok {
		return false
	}

	if json.Unmarshal(aws, &s) == nil {
		return s == "*"
	}

	var list []string

	return json.Unmarshal(aws, &list) == nil && slices.Contains(list, "*")
}
