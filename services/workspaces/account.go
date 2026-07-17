package workspaces

// DescribeAccount returns account configuration.
func (b *InMemoryBackend) DescribeAccount() storedAccountConfig {
	b.mu.RLock("DescribeAccount")
	defer b.mu.RUnlock()

	cfg := b.accountConfig
	if cfg.DedicatedTenancySupport == "" {
		cfg.DedicatedTenancySupport = "ENABLED"
	}

	return cfg
}

// ModifyAccount updates account configuration.
func (b *InMemoryBackend) ModifyAccount(
	dedicatedTenancyCidr, dedicatedTenancySupport string,
) error {
	b.mu.Lock("ModifyAccount")
	defer b.mu.Unlock()

	if dedicatedTenancyCidr != "" {
		b.accountConfig.ManagementCidrRange = dedicatedTenancyCidr
	}

	if dedicatedTenancySupport != "" {
		b.accountConfig.DedicatedTenancySupport = dedicatedTenancySupport
	}

	return nil
}
