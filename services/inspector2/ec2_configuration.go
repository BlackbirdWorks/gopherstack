package inspector2

// defaultEc2DeepInspectionConfig returns the Ec2DeepInspectionConfig a fresh
// backend (or a reset one) starts with.
func defaultEc2DeepInspectionConfig() Ec2DeepInspectionConfig {
	return Ec2DeepInspectionConfig{
		Status:       statusDisabled,
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
	b.ec2DeepConfig.Status = statusEnabled

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
				Status:       statusDisabled,
			})
		}
	}

	return result
}

// BatchUpdateMemberEc2DeepInspectionStatus updates EC2 deep inspection status for member accounts.
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
			Status:       statusEnabled,
		}
		b.memberEc2Status.Put(s)
		cp := *s
		result = append(result, &cp)
	}

	return result
}
