package inspector2

// ec2DeepInspectionStatusActivated/Deactivated are the real
// Ec2DeepInspectionStatus enum values (inspector2@v1.54.1 types/enums.go:
// ACTIVATED|DEACTIVATED|PENDING|FAILED) -- distinct from this package's
// generic statusEnabled/statusDisabled ("ENABLED"/"DISABLED"), which do not
// appear in this enum at all.
const (
	ec2DeepInspectionStatusActivated   = "ACTIVATED"
	ec2DeepInspectionStatusDeactivated = "DEACTIVATED"
)

// defaultEc2DeepInspectionConfig returns the Ec2DeepInspectionConfig a fresh
// backend (or a reset one) starts with.
func defaultEc2DeepInspectionConfig() Ec2DeepInspectionConfig {
	return Ec2DeepInspectionConfig{
		Status:       ec2DeepInspectionStatusDeactivated,
		PackagePaths: []string{},
	}
}

// GetEc2DeepInspectionConfiguration returns EC2 deep inspection config.
func (b *InMemoryBackend) GetEc2DeepInspectionConfiguration() Ec2DeepInspectionConfig {
	b.mu.RLock("GetEc2DeepInspectionConfiguration")
	defer b.mu.RUnlock()

	cp := b.ec2DeepConfig
	cp.PackagePaths = append([]string(nil), b.ec2DeepConfig.PackagePaths...)

	return cp
}

// UpdateEc2DeepInspectionConfiguration updates EC2 deep inspection config.
func (b *InMemoryBackend) UpdateEc2DeepInspectionConfiguration(paths []string) error {
	b.mu.Lock("UpdateEc2DeepInspectionConfiguration")
	defer b.mu.Unlock()

	b.ec2DeepConfig.PackagePaths = append([]string(nil), paths...)
	b.ec2DeepConfig.Status = ec2DeepInspectionStatusActivated

	return nil
}

// UpdateOrgEc2DeepInspectionConfiguration updates org-level EC2 deep inspection config.
func (b *InMemoryBackend) UpdateOrgEc2DeepInspectionConfiguration(paths []string) error {
	b.mu.Lock("UpdateOrgEc2DeepInspectionConfiguration")
	defer b.mu.Unlock()

	b.orgEc2Config.CustomPaths = append([]string(nil), paths...)

	return nil
}

// BatchGetMemberEc2DeepInspectionStatus returns EC2 deep inspection status for member accounts.
func (b *InMemoryBackend) BatchGetMemberEc2DeepInspectionStatus(accountIDs []string) []*MemberEc2DeepInspectionStatus {
	b.mu.RLock("BatchGetMemberEc2DeepInspectionStatus")
	defer b.mu.RUnlock()

	result := make([]*MemberEc2DeepInspectionStatus, 0, len(accountIDs))

	for _, id := range accountIDs {
		if s, ok := b.memberEc2Status.Get(id); ok {
			cp := *s
			result = append(result, &cp)
		} else {
			result = append(result, &MemberEc2DeepInspectionStatus{
				AccountID:    id,
				PackagePaths: []string{},
				Status:       ec2DeepInspectionStatusDeactivated,
			})
		}
	}

	return result
}

// BatchUpdateMemberEc2DeepInspectionStatus updates EC2 deep inspection
// status for member accounts. Previously hardcoded every account's stored
// Status to enabled regardless of the caller's ActivateDeepInspection value
// -- a real client deactivating an account via this op would see it
// reported activated again on the very next Get/BatchGet call.
func (b *InMemoryBackend) BatchUpdateMemberEc2DeepInspectionStatus(
	updates []*MemberEc2DeepInspectionStatus,
) []*MemberEc2DeepInspectionStatus {
	b.mu.Lock("BatchUpdateMemberEc2DeepInspectionStatus")
	defer b.mu.Unlock()

	result := make([]*MemberEc2DeepInspectionStatus, 0, len(updates))

	for _, u := range updates {
		paths := append([]string(nil), u.PackagePaths...)
		s := &MemberEc2DeepInspectionStatus{
			AccountID:    u.AccountID,
			PackagePaths: paths,
			Status:       u.Status,
		}
		b.memberEc2Status.Put(s)
		cp := *s
		result = append(result, &cp)
	}

	return result
}
