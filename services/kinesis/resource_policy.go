package kinesis

import "context"

// PutResourcePolicy stores a resource-based policy for the given stream or consumer ARN.
func (b *InMemoryBackend) PutResourcePolicy(ctx context.Context, input *PutResourcePolicyInput) error {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	b.policiesStore(region)[input.ResourceARN] = input.Policy

	return nil
}

// GetResourcePolicy retrieves the resource-based policy for the given stream or consumer ARN.
func (b *InMemoryBackend) GetResourcePolicy(
	ctx context.Context,
	input *GetResourcePolicyInput,
) (*GetResourcePolicyOutput, error) {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	policy, ok := b.resourcePolicies[region][input.ResourceARN]
	if !ok {
		return nil, ErrResourcePolicyNotFound
	}

	return &GetResourcePolicyOutput{Policy: policy}, nil
}

// DeleteResourcePolicy removes the resource-based policy for the given stream or consumer ARN.
func (b *InMemoryBackend) DeleteResourcePolicy(ctx context.Context, input *DeleteResourcePolicyInput) error {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	regionPolicies := b.resourcePolicies[region]
	if _, ok := regionPolicies[input.ResourceARN]; !ok {
		return ErrResourcePolicyNotFound
	}

	delete(regionPolicies, input.ResourceARN)

	return nil
}
