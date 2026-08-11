package vpclattice

// ------- Auth Policy operations -------

// resolvePolicyResourceARN normalizes resourceID -- which real AWS documents
// as accepting either "The ID or ARN of the service network or service" for
// PutAuthPolicy/PutResourcePolicy -- to that resource's canonical ARN.
// DeleteService/DeleteServiceNetwork's cascade delete always removes
// authPolicies/resourcePolicies entries by ARN (see deleteServiceDependents,
// DeleteServiceNetwork); keying a Put by whatever raw identifier form the
// caller happened to use would let the cascade delete miss the entry
// entirely, orphaning it in the map forever once the parent resource is
// gone. Falls back to returning identifier unchanged when it doesn't resolve
// to a known service or service network, matching Put's existing lenient
// behavior of not validating resource existence.
func (b *InMemoryBackend) resolvePolicyResourceARN(identifier string) string {
	if svcID, svcOK := b.resolveServiceID(identifier); svcOK {
		if svc, found := b.services.Get(svcID); found {
			return svc.ARN
		}
	}

	if snID, snOK := b.resolveServiceNetworkID(identifier); snOK {
		if sn, found := b.serviceNetworks.Get(snID); found {
			return sn.ARN
		}
	}

	return identifier
}

// PutAuthPolicy sets an auth policy on a resource.
func (b *InMemoryBackend) PutAuthPolicy(resourceID, policy string) (*AuthPolicy, error) {
	b.mu.Lock("PutAuthPolicy")
	defer b.mu.Unlock()

	b.authPolicies[b.resolvePolicyResourceARN(resourceID)] = policy

	return &AuthPolicy{Policy: policy, State: authPolicyStateActive}, nil
}

// GetAuthPolicy returns the auth policy for a resource.
func (b *InMemoryBackend) GetAuthPolicy(resourceID string) (*AuthPolicy, error) {
	b.mu.RLock("GetAuthPolicy")
	defer b.mu.RUnlock()

	policy, ok := b.authPolicies[b.resolvePolicyResourceARN(resourceID)]
	if !ok {
		return nil, ErrNotFound
	}

	return &AuthPolicy{Policy: policy, State: authPolicyStateActive}, nil
}

// DeleteAuthPolicy deletes the auth policy for a resource.
func (b *InMemoryBackend) DeleteAuthPolicy(resourceID string) error {
	b.mu.Lock("DeleteAuthPolicy")
	defer b.mu.Unlock()

	delete(b.authPolicies, b.resolvePolicyResourceARN(resourceID))

	return nil
}
