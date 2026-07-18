package mediaconvert

import "fmt"

// GetPolicy returns the current account policy, or nil if none has been set.
func (b *InMemoryBackend) GetPolicy() (*Policy, error) {
	b.mu.RLock("GetPolicy")
	defer b.mu.RUnlock()

	if b.policy == nil {
		return nil, fmt.Errorf("%w: no policy configured", ErrNotFound)
	}
	cp := *b.policy

	return &cp, nil
}

// PutPolicy sets the account policy.
func (b *InMemoryBackend) PutPolicy(httpInputs, httpsInputs, s3Inputs string) *Policy {
	b.mu.Lock("PutPolicy")
	defer b.mu.Unlock()

	b.policy = &Policy{
		HTTPInputs:  httpInputs,
		HTTPSInputs: httpsInputs,
		S3Inputs:    s3Inputs,
	}
	cp := *b.policy

	return &cp
}

// DeletePolicy removes the account policy.
func (b *InMemoryBackend) DeletePolicy() error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	if b.policy == nil {
		return fmt.Errorf("%w: no policy configured", ErrNotFound)
	}
	b.policy = nil

	return nil
}
