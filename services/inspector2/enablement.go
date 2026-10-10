package inspector2

import "time"

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
	b.setEnabled(nil, resourceTypes, true)

	return nil
}

// Disable disables Inspector2 scanning for the given resource types.
// If resourceTypes is empty, all known resource types are disabled.
func (b *InMemoryBackend) Disable(resourceTypes []string) error {
	b.setEnabled(nil, resourceTypes, false)

	return nil
}

// AccountOutcome splits requested accounts into those updated and those that cannot be addressed.
type AccountOutcome struct {
	Updated []string
	Unknown []string
}

// StatusLookup holds the statuses found for requested accounts and the IDs that could not be addressed.
type StatusLookup struct {
	Found   []*AccountStatusResponse
	Unknown []string
}

// EnableAccounts enables resourceTypes for each account and returns the accounts that could not be addressed.
func (b *InMemoryBackend) EnableAccounts(accountIDs, resourceTypes []string) AccountOutcome {
	return b.setEnabled(accountIDs, resourceTypes, true)
}

// DisableAccounts disables resourceTypes for each account and returns the accounts that could not be addressed.
func (b *InMemoryBackend) DisableAccounts(accountIDs, resourceTypes []string) AccountOutcome {
	return b.setEnabled(accountIDs, resourceTypes, false)
}

// setEnabled addresses the backend's own account or an associated member; an empty accountIDs means the own account.
func (b *InMemoryBackend) setEnabled(accountIDs, resourceTypes []string, enabled bool) AccountOutcome {
	b.mu.Lock("SetEnabled")
	defer b.mu.Unlock()

	if len(resourceTypes) == 0 {
		resourceTypes = knownResourceTypes()
	}

	if len(accountIDs) == 0 {
		accountIDs = []string{b.accountID}
	}

	var out AccountOutcome

	for _, id := range accountIDs {
		target, ok := b.enabledMapLocked(id, true)
		if !ok {
			out.Unknown = append(out.Unknown, id)

			continue
		}

		for _, rt := range resourceTypes {
			if target[rt] != enabled {
				b.transitions[transitionKey(id, rt)] = b.now()
			}

			target[rt] = enabled
		}

		out.Updated = append(out.Updated, id)
	}

	return out
}

// enabledMapLocked returns accountID's resource-type enablement map; false when the account is neither the
// backend's own nor an associated member. Callers must hold b.mu.
func (b *InMemoryBackend) enabledMapLocked(accountID string, create bool) (map[string]bool, bool) {
	if accountID == b.accountID {
		return b.enabledTypes, true
	}

	if _, ok := b.members.Get(accountID); !ok {
		return nil, false
	}

	m, ok := b.memberEnabled[accountID]
	if !ok && create {
		m = make(map[string]bool)
		b.memberEnabled[accountID] = m
	}

	return m, true
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

// GetStatus returns the backend's own account status with per-resource-type detail.
func (b *InMemoryBackend) GetStatus() *AccountStatusResponse {
	b.mu.RLock("GetStatus")
	defer b.mu.RUnlock()

	return b.statusLocked(b.accountID, b.enabledTypes)
}

// GetAccountStatuses returns the status of each requested account (the own account when none are given) and the
// IDs that are neither the own account nor an associated member.
func (b *InMemoryBackend) GetAccountStatuses(accountIDs []string) StatusLookup {
	b.mu.RLock("GetAccountStatuses")
	defer b.mu.RUnlock()

	if len(accountIDs) == 0 {
		accountIDs = []string{b.accountID}
	}

	var out StatusLookup

	for _, id := range accountIDs {
		enabled, ok := b.enabledMapLocked(id, false)
		if !ok {
			out.Unknown = append(out.Unknown, id)

			continue
		}

		out.Found = append(out.Found, b.statusLocked(id, enabled))
	}

	return out
}

// SetLifecycleDelay sets how long Enable and Disable report ENABLING and DISABLING before settling; the default 0
// settles instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// SetClock overrides the backend clock; for deterministic tests.
func (b *InMemoryBackend) SetClock(clock func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = clock
}

func (b *InMemoryBackend) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}

	return time.Now()
}

func transitionKey(accountID, resourceType string) string { return accountID + "|" + resourceType }

// resourceStatusLocked reports one resource type's status, ENABLING or DISABLING while its dwell window is open.
func (b *InMemoryBackend) resourceStatusLocked(accountID, rt string, enabled bool) string {
	inWindow := false

	if at, ok := b.transitions[transitionKey(accountID, rt)]; ok && b.lifecycleDelay > 0 {
		inWindow = b.now().Sub(at) < b.lifecycleDelay
	}

	switch {
	case enabled && inWindow:
		return statusEnabling
	case enabled:
		return statusEnabled
	case inWindow:
		return statusDisabling
	default:
		return statusDisabled
	}
}

func (b *InMemoryBackend) statusLocked(accountID string, enabled map[string]bool) *AccountStatusResponse {
	statuses := make(map[string]string, len(knownResourceTypes()))
	for _, rt := range knownResourceTypes() {
		statuses[rt] = b.resourceStatusLocked(accountID, rt, enabled[rt])
	}

	return &AccountStatusResponse{
		AccountID:            accountID,
		Status:               overallStatus(statuses),
		Ec2Status:            statuses[resourceTypeEC2],
		EcrStatus:            statuses[resourceTypeECR],
		LambdaStatus:         statuses[resourceTypeLambda],
		LambdaCodeStatus:     statuses[resourceTypeLambdaCode],
		CodeRepositoryStatus: statuses[resourceTypeCodeRepository],
	}
}

// overallStatus rolls per-type statuses up: ENABLING outranks ENABLED, which outranks DISABLING.
func overallStatus(statuses map[string]string) string {
	for _, want := range []string{statusEnabling, statusEnabled, statusDisabling} {
		for _, st := range statuses {
			if st == want {
				return want
			}
		}
	}

	return statusDisabled
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
