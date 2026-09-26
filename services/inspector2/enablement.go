package inspector2

// Resource types and scan-mode defaults for Enable/Disable and Configuration.
const (
	resourceTypeEC2            = "EC2"
	resourceTypeECR            = "ECR"
	resourceTypeLambda         = "LAMBDA"
	resourceTypeLambdaCode     = "LAMBDA_CODE"
	resourceTypeCodeRepository = "CODE_REPOSITORY"

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
	return []string{
		resourceTypeEC2, resourceTypeECR, resourceTypeLambda,
		resourceTypeLambdaCode, resourceTypeCodeRepository,
	}
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
		AccountID:            b.accountID,
		Status:               overall,
		Ec2Status:            typeStatus(resourceTypeEC2),
		EcrStatus:            typeStatus(resourceTypeECR),
		LambdaStatus:         typeStatus(resourceTypeLambda),
		LambdaCodeStatus:     typeStatus(resourceTypeLambdaCode),
		CodeRepositoryStatus: typeStatus(resourceTypeCodeRepository),
	}
}

// effectiveMemberConfig returns accountID's effective configuration: any
// individually-configured scan type overrides the delegated admin's own
// Configuration, and any scan type never configured (or reset to inherit,
// see UpdateConfiguration) falls back to it -- api_op_UpdateConfiguration.go:
// "this operation updates the delegated administrator's configuration and
// propagates it to member accounts that have not been individually
// configured." Callers must hold at least a read lock.
func (b *InMemoryBackend) effectiveMemberConfig(accountID string) Configuration {
	cfg := b.config

	if mc, ok := b.memberConfigs.Get(accountID); ok {
		if mc.Ec2ScanMode != "" {
			cfg.Ec2ScanMode = mc.Ec2ScanMode
		}

		if mc.EcrRescanDuration != "" {
			cfg.EcrRescanDuration = mc.EcrRescanDuration
		}
	}

	return cfg
}

// GetConfiguration returns accountID's configuration. An empty accountID
// (or the backend's own account) returns the delegated admin's own
// Configuration; any other accountId must name a known member (real AWS:
// "you must be the delegated administrator for the specified member
// account") and gets its effective (override-or-inherited) configuration.
func (b *InMemoryBackend) GetConfiguration(accountID string) (*Configuration, error) {
	b.mu.RLock("GetConfiguration")
	defer b.mu.RUnlock()

	if accountID == "" || accountID == b.accountID {
		cfg := b.config

		return &cfg, nil
	}

	if _, ok := b.members.Get(accountID); !ok {
		return nil, ErrMemberNotFound
	}

	cfg := b.effectiveMemberConfig(accountID)

	return &cfg, nil
}

// UpdateConfiguration updates accountID's scan configuration. An empty
// accountID updates the delegated admin's own Configuration (and implicitly
// propagates to members with no individual override, via
// effectiveMemberConfig's fallback). A non-empty accountID must name a known
// member; resetEc2ToInherit/resetEcrToInherit (from
// UpdateConfigurationInheritance) clear that member's override for the named
// scan type, restoring inheritance from the admin's Configuration.
func (b *InMemoryBackend) UpdateConfiguration(
	accountID, ec2ScanMode, ecrRescanDuration string,
	resetEc2ToInherit, resetEcrToInherit bool,
) error {
	b.mu.Lock("UpdateConfiguration")
	defer b.mu.Unlock()

	if accountID == "" || accountID == b.accountID {
		if ec2ScanMode != "" {
			b.config.Ec2ScanMode = ec2ScanMode
		}

		if ecrRescanDuration != "" {
			b.config.EcrRescanDuration = ecrRescanDuration
		}

		return nil
	}

	if _, ok := b.members.Get(accountID); !ok {
		return ErrMemberNotFound
	}

	mc := MemberConfiguration{AccountID: accountID}
	if existing, ok := b.memberConfigs.Get(accountID); ok {
		mc = *existing
	}

	switch {
	case resetEc2ToInherit:
		mc.Ec2ScanMode = ""
	case ec2ScanMode != "":
		mc.Ec2ScanMode = ec2ScanMode
	}

	switch {
	case resetEcrToInherit:
		mc.EcrRescanDuration = ""
	case ecrRescanDuration != "":
		mc.EcrRescanDuration = ecrRescanDuration
	}

	b.memberConfigs.Put(&mc)

	return nil
}

// accountPermissionOperations lists every real Operation enum value.
func accountPermissionOperations() []string {
	return []string{
		"ENABLE_SCANNING",
		"DISABLE_SCANNING",
		"ENABLE_REPOSITORY",
		"DISABLE_REPOSITORY",
	}
}

// accountPermissionServices lists every real Service enum value.
func accountPermissionServices() []string {
	return []string{resourceTypeEC2, resourceTypeECR, resourceTypeLambda}
}

// ListAccountPermissions returns the account's Inspector2 configuration
// permissions. gopherstack's mock account has no IAM engine to evaluate
// against, so -- rather than the prior hardwired-empty stub, which silently
// dropped every request -- it reports the full real Operation x Service
// permission matrix (the account can perform every configuration
// operation), narrowed by the optional service filter, matching real
// AccountPermission wire shape (operation/service).
func (b *InMemoryBackend) ListAccountPermissions(service string) ([]*AccountPermission, error) {
	services := accountPermissionServices()
	if service != "" {
		services = []string{service}
	}

	perms := make([]*AccountPermission, 0, len(services)*len(accountPermissionOperations()))

	for _, svc := range services {
		for _, op := range accountPermissionOperations() {
			perms = append(perms, &AccountPermission{Operation: op, Service: svc})
		}
	}

	return perms, nil
}
