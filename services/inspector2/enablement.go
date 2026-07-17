package inspector2

// Resource types and scan-mode defaults for Enable/Disable and Configuration.
const (
	resourceTypeEC2        = "EC2"
	resourceTypeECR        = "ECR"
	resourceTypeLambda     = "LAMBDA"
	resourceTypeLambdaCode = "LAMBDA_CODE"

	ec2ScanModeEC2SSMAgentBased = "EC2_SSM_AGENT_BASED"
	ecrRescanDurationLifetime   = "LIFETIME"
)

// defaultConfiguration returns the Configuration a fresh backend (or a reset
// one) starts with.
func defaultConfiguration() Configuration {
	return Configuration{
		Ec2ScanMode:       ec2ScanModeEC2SSMAgentBased,
		EcrRescanDuration: ecrRescanDurationLifetime,
	}
}

// knownResourceTypes returns the full set of Inspector2 resource types, used
// when Enable/Disable is called with an empty list.
func knownResourceTypes() []string {
	return []string{resourceTypeEC2, resourceTypeECR, resourceTypeLambda, resourceTypeLambdaCode}
}

// Enable enables Inspector2 scanning for the given resource types.
// If resourceTypes is empty, all known resource types are enabled.
func (b *InMemoryBackend) Enable(resourceTypes []string) error {
	b.mu.Lock("Enable")
	defer b.mu.Unlock()

	if len(resourceTypes) == 0 {
		resourceTypes = knownResourceTypes()
	}

	for _, rt := range resourceTypes {
		b.enabledTypes[rt] = true
	}

	return nil
}

// Disable disables Inspector2 scanning for the given resource types.
// If resourceTypes is empty, all known resource types are disabled.
func (b *InMemoryBackend) Disable(resourceTypes []string) error {
	b.mu.Lock("Disable")
	defer b.mu.Unlock()

	if len(resourceTypes) == 0 {
		resourceTypes = knownResourceTypes()
	}

	for _, rt := range resourceTypes {
		b.enabledTypes[rt] = false
	}

	return nil
}

// IsEnabled returns whether Inspector2 is enabled for any resource type.
func (b *InMemoryBackend) IsEnabled() bool {
	b.mu.RLock("IsEnabled")
	defer b.mu.RUnlock()

	for _, v := range b.enabledTypes {
		if v {
			return true
		}
	}

	return false
}

// GetStatus returns account status information with per-resource-type detail.
func (b *InMemoryBackend) GetStatus() *AccountStatusResponse {
	b.mu.RLock("GetStatus")
	defer b.mu.RUnlock()

	typeStatus := func(rt string) string {
		if b.enabledTypes[rt] {
			return statusEnabled
		}

		return statusDisabled
	}

	overall := statusDisabled

	for _, v := range b.enabledTypes {
		if v {
			overall = statusEnabled

			break
		}
	}

	return &AccountStatusResponse{
		AccountID:    b.accountID,
		Status:       overall,
		Ec2Status:    typeStatus(resourceTypeEC2),
		EcrStatus:    typeStatus(resourceTypeECR),
		LambdaStatus: typeStatus(resourceTypeLambda),
	}
}

// GetConfiguration returns the current configuration.
func (b *InMemoryBackend) GetConfiguration() *Configuration {
	b.mu.RLock("GetConfiguration")
	defer b.mu.RUnlock()

	cfg := b.config

	return &cfg
}

// UpdateConfiguration updates the scan configuration.
func (b *InMemoryBackend) UpdateConfiguration(ec2ScanMode, ecrRescanDuration string) error {
	b.mu.Lock("UpdateConfiguration")
	defer b.mu.Unlock()

	if ec2ScanMode != "" {
		b.config.Ec2ScanMode = ec2ScanMode
	}

	if ecrRescanDuration != "" {
		b.config.EcrRescanDuration = ecrRescanDuration
	}

	return nil
}

// ListAccountPermissions returns account-level Inspector2 permissions (stub).
func (b *InMemoryBackend) ListAccountPermissions(_ string) ([]*AccountPermission, error) {
	return []*AccountPermission{}, nil
}
