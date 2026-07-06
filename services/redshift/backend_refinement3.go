package redshift

import (
	"fmt"
	"time"
)

// ---- Snapshot Copy Grants ----

// CreateSnapshotCopyGrant creates a KMS key grant used for cross-region snapshot copy.
func (b *InMemoryBackend) CreateSnapshotCopyGrant(
	name, kmsKeyID string,
	tagMap map[string]string,
) (*SnapshotCopyGrant, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SnapshotCopyGrantName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSnapshotCopyGrant")
	defer b.mu.Unlock()

	if _, exists := b.snapshotCopyGrants.Get(name); exists {
		return nil, fmt.Errorf("%w: grant %s already exists", ErrSnapshotCopyGrantAlreadyExists, name)
	}

	grant := &SnapshotCopyGrant{
		SnapshotCopyGrantName: name,
		KMSKeyID:              kmsKeyID,
		Tags:                  tagMap,
	}
	b.snapshotCopyGrants.Put(grant)

	cp := *grant

	return &cp, nil
}

// DeleteSnapshotCopyGrant deletes the named snapshot copy grant.
func (b *InMemoryBackend) DeleteSnapshotCopyGrant(name string) error {
	if name == "" {
		return fmt.Errorf("%w: SnapshotCopyGrantName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSnapshotCopyGrant")
	defer b.mu.Unlock()

	if _, exists := b.snapshotCopyGrants.Get(name); !exists {
		return fmt.Errorf("%w: grant %s not found", ErrSnapshotCopyGrantNotFound, name)
	}

	b.snapshotCopyGrants.Delete(name)

	return nil
}

// DescribeSnapshotCopyGrants returns snapshot copy grants, optionally filtered by name.
func (b *InMemoryBackend) DescribeSnapshotCopyGrants(name string) ([]SnapshotCopyGrant, error) {
	b.mu.RLock("DescribeSnapshotCopyGrants")
	defer b.mu.RUnlock()

	if name != "" {
		g, exists := b.snapshotCopyGrants.Get(name)
		if !exists {
			return nil, fmt.Errorf("%w: grant %s not found", ErrSnapshotCopyGrantNotFound, name)
		}

		cp := *g

		return []SnapshotCopyGrant{cp}, nil
	}

	result := make([]SnapshotCopyGrant, 0, b.snapshotCopyGrants.Len())

	for _, g := range b.snapshotCopyGrants.All() {
		result = append(result, *g)
	}

	return result, nil
}

// ---- Snapshot Copy (cluster-level) ----

// EnableSnapshotCopy enables cross-region snapshot copy for a cluster.
func (b *InMemoryBackend) EnableSnapshotCopy(
	clusterID, destinationRegion, grantName string,
	retentionPeriod int,
) (*Cluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if destinationRegion == "" {
		return nil, fmt.Errorf("%w: DestinationRegion is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableSnapshotCopy")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(clusterID)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	if _, already := b.snapshotCopyConfigs[clusterID]; already {
		return nil, fmt.Errorf(
			"%w: snapshot copy already enabled for cluster %s",
			ErrSnapshotCopyAlreadyEnabled,
			clusterID,
		)
	}

	if retentionPeriod <= 0 {
		retentionPeriod = 7
	}

	b.snapshotCopyConfigs[clusterID] = &SnapshotCopyConfig{
		DestinationRegion:     destinationRegion,
		SnapshotCopyGrantName: grantName,
		RetentionPeriod:       retentionPeriod,
	}

	cp := cloneCluster(cluster)

	return &cp, nil
}

// DisableSnapshotCopy disables cross-region snapshot copy for a cluster.
func (b *InMemoryBackend) DisableSnapshotCopy(clusterID string) (*Cluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableSnapshotCopy")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(clusterID)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	if _, enabled := b.snapshotCopyConfigs[clusterID]; !enabled {
		return nil, fmt.Errorf("%w: snapshot copy not enabled for cluster %s", ErrSnapshotCopyNotEnabled, clusterID)
	}

	delete(b.snapshotCopyConfigs, clusterID)

	cp := cloneCluster(cluster)

	return &cp, nil
}

// ModifySnapshotCopyRetentionPeriod modifies the retention period for cross-region snapshot copy.
func (b *InMemoryBackend) ModifySnapshotCopyRetentionPeriod(clusterID string, retentionPeriod int) (*Cluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifySnapshotCopyRetentionPeriod")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(clusterID)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	cfg, enabled := b.snapshotCopyConfigs[clusterID]
	if !enabled {
		return nil, fmt.Errorf("%w: snapshot copy not enabled for cluster %s", ErrSnapshotCopyNotEnabled, clusterID)
	}

	cfg.RetentionPeriod = retentionPeriod
	cp := cloneCluster(cluster)

	return &cp, nil
}

// ---- Snapshot Schedules ----

// CreateSnapshotSchedule creates a new snapshot schedule.
func (b *InMemoryBackend) CreateSnapshotSchedule(
	scheduleID, description string,
	definitions []string,
	tagMap map[string]string,
) (*SnapshotSchedule, error) {
	if scheduleID == "" {
		return nil, fmt.Errorf("%w: ScheduleIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSnapshotSchedule")
	defer b.mu.Unlock()

	if _, exists := b.snapshotSchedules.Get(scheduleID); exists {
		return nil, fmt.Errorf("%w: schedule %s already exists", ErrSnapshotScheduleAlreadyExists, scheduleID)
	}

	defCopy := make([]string, len(definitions))
	copy(defCopy, definitions)

	sched := &SnapshotSchedule{
		ScheduleIdentifier:  scheduleID,
		Description:         description,
		ScheduleDefinitions: defCopy,
		Tags:                tagMap,
	}
	b.snapshotSchedules.Put(sched)

	cp := cloneSnapshotSchedule(sched)

	return cp, nil
}

// DeleteSnapshotSchedule deletes the named snapshot schedule.
func (b *InMemoryBackend) DeleteSnapshotSchedule(scheduleID string) error {
	if scheduleID == "" {
		return fmt.Errorf("%w: ScheduleIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSnapshotSchedule")
	defer b.mu.Unlock()

	if _, exists := b.snapshotSchedules.Get(scheduleID); !exists {
		return fmt.Errorf("%w: schedule %s not found", ErrSnapshotScheduleNotFound, scheduleID)
	}

	b.snapshotSchedules.Delete(scheduleID)

	return nil
}

// DescribeSnapshotSchedules returns snapshot schedules, optionally filtered by identifier.
func (b *InMemoryBackend) DescribeSnapshotSchedules(scheduleID string) ([]SnapshotSchedule, error) {
	b.mu.RLock("DescribeSnapshotSchedules")
	defer b.mu.RUnlock()

	if scheduleID != "" {
		s, exists := b.snapshotSchedules.Get(scheduleID)
		if !exists {
			return nil, fmt.Errorf("%w: schedule %s not found", ErrSnapshotScheduleNotFound, scheduleID)
		}

		return []SnapshotSchedule{*cloneSnapshotSchedule(s)}, nil
	}

	result := make([]SnapshotSchedule, 0, b.snapshotSchedules.Len())

	for _, s := range b.snapshotSchedules.All() {
		result = append(result, *cloneSnapshotSchedule(s))
	}

	return result, nil
}

// ModifySnapshotSchedule updates the schedule definitions for an existing snapshot schedule.
func (b *InMemoryBackend) ModifySnapshotSchedule(scheduleID string, definitions []string) (*SnapshotSchedule, error) {
	if scheduleID == "" {
		return nil, fmt.Errorf("%w: ScheduleIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifySnapshotSchedule")
	defer b.mu.Unlock()

	sched, exists := b.snapshotSchedules.Get(scheduleID)
	if !exists {
		return nil, fmt.Errorf("%w: schedule %s not found", ErrSnapshotScheduleNotFound, scheduleID)
	}

	defCopy := make([]string, len(definitions))
	copy(defCopy, definitions)
	sched.ScheduleDefinitions = defCopy

	return cloneSnapshotSchedule(sched), nil
}

// ModifyClusterSnapshotSchedule associates or disassociates a snapshot schedule with a cluster.
func (b *InMemoryBackend) ModifyClusterSnapshotSchedule(clusterID, scheduleID string, disassociate bool) error {
	if clusterID == "" {
		return fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyClusterSnapshotSchedule")
	defer b.mu.Unlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	if !disassociate && scheduleID != "" {
		if _, exists := b.snapshotSchedules.Get(scheduleID); !exists {
			return fmt.Errorf("%w: schedule %s not found", ErrSnapshotScheduleNotFound, scheduleID)
		}
	}

	return nil
}

// ---- Usage Limits ----

// CreateUsageLimit creates a new usage limit for a cluster feature.
func (b *InMemoryBackend) CreateUsageLimit(
	clusterID, featureType, limitType, breachAction string,
	amount int64,
	tagMap map[string]string,
) (*UsageLimit, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateUsageLimit")
	defer b.mu.Unlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	id := fmt.Sprintf("ul-%d", time.Now().UnixNano())

	ul := &UsageLimit{
		UsageLimitID:      id,
		ClusterIdentifier: clusterID,
		FeatureType:       featureType,
		LimitType:         limitType,
		BreachAction:      breachAction,
		Amount:            amount,
		Tags:              tagMap,
	}
	b.usageLimits.Put(ul)

	cp := *ul

	return &cp, nil
}

// DeleteUsageLimit deletes the usage limit with the given ID.
func (b *InMemoryBackend) DeleteUsageLimit(usageLimitID string) error {
	if usageLimitID == "" {
		return fmt.Errorf("%w: UsageLimitId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteUsageLimit")
	defer b.mu.Unlock()

	if _, exists := b.usageLimits.Get(usageLimitID); !exists {
		return fmt.Errorf("%w: usage limit %s not found", ErrUsageLimitNotFound, usageLimitID)
	}

	b.usageLimits.Delete(usageLimitID)

	return nil
}

// DescribeUsageLimits returns usage limits, optionally filtered by cluster and feature type.
func (b *InMemoryBackend) DescribeUsageLimits(clusterID, featureType string) ([]UsageLimit, error) {
	b.mu.RLock("DescribeUsageLimits")
	defer b.mu.RUnlock()

	result := make([]UsageLimit, 0, b.usageLimits.Len())

	for _, ul := range b.usageLimits.All() {
		if clusterID != "" && ul.ClusterIdentifier != clusterID {
			continue
		}

		if featureType != "" && ul.FeatureType != featureType {
			continue
		}

		cp := *ul
		result = append(result, cp)
	}

	return result, nil
}

// ModifyUsageLimit modifies the amount and/or breach action of a usage limit.
func (b *InMemoryBackend) ModifyUsageLimit(usageLimitID, breachAction string, amount int64) (*UsageLimit, error) {
	if usageLimitID == "" {
		return nil, fmt.Errorf("%w: UsageLimitId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyUsageLimit")
	defer b.mu.Unlock()

	ul, exists := b.usageLimits.Get(usageLimitID)
	if !exists {
		return nil, fmt.Errorf("%w: usage limit %s not found", ErrUsageLimitNotFound, usageLimitID)
	}

	if amount > 0 {
		ul.Amount = amount
	}

	if breachAction != "" {
		ul.BreachAction = breachAction
	}

	cp := *ul

	return &cp, nil
}

// ---- Authentication Profiles ----

// CreateAuthenticationProfile creates a new authentication profile.
func (b *InMemoryBackend) CreateAuthenticationProfile(name, content string) (*AuthenticationProfile, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: AuthenticationProfileName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateAuthenticationProfile")
	defer b.mu.Unlock()

	if _, exists := b.authProfiles.Get(name); exists {
		return nil, fmt.Errorf("%w: profile %s already exists", ErrAuthProfileAlreadyExists, name)
	}

	ap := &AuthenticationProfile{
		AuthenticationProfileName:    name,
		AuthenticationProfileContent: content,
	}
	b.authProfiles.Put(ap)

	cp := *ap

	return &cp, nil
}

// DeleteAuthenticationProfile deletes the named authentication profile.
func (b *InMemoryBackend) DeleteAuthenticationProfile(name string) error {
	if name == "" {
		return fmt.Errorf("%w: AuthenticationProfileName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteAuthenticationProfile")
	defer b.mu.Unlock()

	if _, exists := b.authProfiles.Get(name); !exists {
		return fmt.Errorf("%w: profile %s not found", ErrAuthProfileNotFound, name)
	}

	b.authProfiles.Delete(name)

	return nil
}

// DescribeAuthenticationProfiles returns authentication profiles, optionally filtered by name.
func (b *InMemoryBackend) DescribeAuthenticationProfiles(name string) ([]AuthenticationProfile, error) {
	b.mu.RLock("DescribeAuthenticationProfiles")
	defer b.mu.RUnlock()

	if name != "" {
		ap, exists := b.authProfiles.Get(name)
		if !exists {
			return nil, fmt.Errorf("%w: profile %s not found", ErrAuthProfileNotFound, name)
		}

		cp := *ap

		return []AuthenticationProfile{cp}, nil
	}

	result := make([]AuthenticationProfile, 0, b.authProfiles.Len())

	for _, ap := range b.authProfiles.All() {
		cp := *ap
		result = append(result, cp)
	}

	return result, nil
}

// ModifyAuthenticationProfile updates the content of an authentication profile.
func (b *InMemoryBackend) ModifyAuthenticationProfile(name, content string) (*AuthenticationProfile, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: AuthenticationProfileName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyAuthenticationProfile")
	defer b.mu.Unlock()

	ap, exists := b.authProfiles.Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: profile %s not found", ErrAuthProfileNotFound, name)
	}

	ap.AuthenticationProfileContent = content
	cp := *ap

	return &cp, nil
}

// ---- Resource Policy ----

// GetResourcePolicy returns the resource policy for the given resource ARN.
func (b *InMemoryBackend) GetResourcePolicy(resourceArn string) (*ResourcePolicy, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	rp, exists := b.resourcePolicies.Get(resourceArn)
	if !exists {
		return nil, fmt.Errorf("%w: resource policy for %s not found", ErrResourcePolicyNotFound, resourceArn)
	}

	cp := *rp

	return &cp, nil
}

// PutResourcePolicy creates or replaces a resource policy for the given ARN.
func (b *InMemoryBackend) PutResourcePolicy(resourceArn, policy string) (*ResourcePolicy, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	rp := &ResourcePolicy{
		ResourceArn: resourceArn,
		Policy:      policy,
	}
	b.resourcePolicies.Put(rp)

	cp := *rp

	return &cp, nil
}

// DeleteResourcePolicy deletes the resource policy for the given ARN.
func (b *InMemoryBackend) DeleteResourcePolicy(resourceArn string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if _, exists := b.resourcePolicies.Get(resourceArn); !exists {
		return fmt.Errorf("%w: resource policy for %s not found", ErrResourcePolicyNotFound, resourceArn)
	}

	b.resourcePolicies.Delete(resourceArn)

	return nil
}

// ---- GetClusterCredentialsWithIAM ----

// GetClusterCredentialsWithIAM returns temporary cluster credentials including an IAM role.
func (b *InMemoryBackend) GetClusterCredentialsWithIAM(clusterID, _ string) (*ClusterCredentials, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetClusterCredentialsWithIAM")
	defer b.mu.RUnlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	return &ClusterCredentials{
		DBUser:     "IAMUser:" + clusterID,
		DBPassword: "Tmp1_iam" + clusterID,
		Expiration: time.Now().Add(time.Hour),
	}, nil
}

// ---- FailoverPrimaryCompute ----

// FailoverPrimaryCompute simulates a primary compute failover by updating the cluster status.
func (b *InMemoryBackend) FailoverPrimaryCompute(clusterID string) (*Cluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("FailoverPrimaryCompute")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(clusterID)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	cp := cloneCluster(cluster)

	return &cp, nil
}

// ---- DescribeTableRestoreStatus ----

// DescribeTableRestoreStatus returns table restore status records for a cluster.
func (b *InMemoryBackend) DescribeTableRestoreStatus(clusterID string) ([]TableRestoreStatus, error) {
	b.mu.RLock("DescribeTableRestoreStatus")
	defer b.mu.RUnlock()

	result := make([]TableRestoreStatus, 0, b.tableRestores.Len())

	for _, tr := range b.tableRestores.All() {
		if clusterID != "" && tr.ClusterIdentifier != clusterID {
			continue
		}

		cp := *tr
		result = append(result, cp)
	}

	return result, nil
}

// ---- clone helpers ----

func cloneSnapshotSchedule(s *SnapshotSchedule) *SnapshotSchedule {
	cp := *s
	cp.ScheduleDefinitions = make([]string, len(s.ScheduleDefinitions))
	copy(cp.ScheduleDefinitions, s.ScheduleDefinitions)

	return &cp
}
