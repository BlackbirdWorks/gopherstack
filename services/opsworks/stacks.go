package opsworks

import (
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

// CreateStack creates a new OpsWorks stack. Name, Region,
// DefaultInstanceProfileArn, and ServiceRoleArn are all "This member is
// required" on the real CreateStackInput (confirmed against
// aws-sdk-go-v2/service/opsworks@v1.31.0's api_op_CreateStack.go) -- a
// well-behaved SDK client validates this locally before ever sending the
// request, but a raw/non-SDK caller can still reach this handler with one
// missing, so the backend must reject it too. opts carries a subset of the
// remaining optional members (see CreateStackOptions's doc comment for what
// is still unmodeled).
func (b *InMemoryBackend) CreateStack(
	name, region, defaultInstanceProfileArn, serviceRoleArn string, opts CreateStackOptions,
) (*Stack, error) {
	if name == "" || region == "" || defaultInstanceProfileArn == "" || serviceRoleArn == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateStack")
	defer b.mu.Unlock()

	id := uuid.NewString()
	now := time.Now().UTC()
	stackArn := b.stackARN(id)

	s := &storedStack{
		CreatedAt:                 now,
		ConfigurationManager:      opts.ConfigurationManager,
		ChefConfiguration:         opts.ChefConfiguration,
		UseOpsworksSecurityGroups: opts.UseOpsworksSecurityGroups,
		Tags:                      make(map[string]string),
		Attributes:                opts.Attributes,
		StackID:                   id,
		Arn:                       stackArn,
		Name:                      name,
		Region:                    region,
		DefaultInstanceProfileArn: defaultInstanceProfileArn,
		ServiceRoleArn:            serviceRoleArn,
		VpcID:                     opts.VpcID,
		AgentVersion:              opts.AgentVersion,
		CustomJSON:                opts.CustomJSON,
		DefaultAvailabilityZone:   opts.DefaultAvailabilityZone,
		DefaultOs:                 opts.DefaultOs,
		DefaultRootDeviceType:     opts.DefaultRootDeviceType,
		DefaultSSHKeyName:         opts.DefaultSSHKeyName,
		DefaultSubnetID:           opts.DefaultSubnetID,
		HostnameTheme:             opts.HostnameTheme,
	}
	b.stacks.Put(s)

	return s.toStack(), nil
}

// CloneStack creates a new stack that is a copy of the source stack.
// ServiceRoleArn is "This member is required" on the real CloneStackInput
// (confirmed against aws-sdk-go-v2/service/opsworks@v1.31.0's
// api_op_CloneStack.go) -- unlike the other stack-attribute overrides in
// opts, it is never inherited from the source. Every field in opts left at
// its zero value inherits the source stack's value instead, matching the
// real API's per-field doc comments (e.g. DefaultOs: "The default option is
// the parent stack's operating system").
func (b *InMemoryBackend) CloneStack(
	sourceStackID, name, region, serviceRoleArn string, opts CloneStackOptions,
) (*Stack, error) {
	if serviceRoleArn == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CloneStack")
	defer b.mu.Unlock()

	src, ok := b.stacks.Get(sourceStackID)
	if !ok {
		return nil, ErrStackNotFound
	}

	cloneName := name
	if cloneName == "" {
		cloneName = src.Name + "-clone"
	}

	cloneRegion := region
	if cloneRegion == "" {
		cloneRegion = src.Region
	}

	id := uuid.NewString()
	now := time.Now().UTC()

	s := &storedStack{
		CreatedAt:            now,
		ConfigurationManager: cloneStackConfigManager(opts.ConfigurationManager, src.ConfigurationManager),
		ChefConfiguration:    cloneStackChefConfig(opts.ChefConfiguration, src.ChefConfiguration),
		UseOpsworksSecurityGroups: cloneStackBoolOverride(
			opts.UseOpsworksSecurityGroups,
			src.UseOpsworksSecurityGroups,
		),
		Tags:    make(map[string]string),
		StackID: id,
		Arn:     b.stackARN(id),
		Name:    cloneName,
		Region:  cloneRegion,
		DefaultInstanceProfileArn: cloneStackStringOverride(
			opts.DefaultInstanceProfileArn,
			src.DefaultInstanceProfileArn,
		),
		ServiceRoleArn:          serviceRoleArn,
		VpcID:                   cloneStackStringOverride(opts.VpcID, src.VpcID),
		AgentVersion:            cloneStackStringOverride(opts.AgentVersion, src.AgentVersion),
		CustomJSON:              cloneStackStringOverride(opts.CustomJSON, src.CustomJSON),
		DefaultAvailabilityZone: cloneStackStringOverride(opts.DefaultAvailabilityZone, src.DefaultAvailabilityZone),
		DefaultOs:               cloneStackStringOverride(opts.DefaultOs, src.DefaultOs),
		DefaultRootDeviceType:   cloneStackStringOverride(opts.DefaultRootDeviceType, src.DefaultRootDeviceType),
		DefaultSSHKeyName:       cloneStackStringOverride(opts.DefaultSSHKeyName, src.DefaultSSHKeyName),
		DefaultSubnetID:         cloneStackStringOverride(opts.DefaultSubnetID, src.DefaultSubnetID),
		HostnameTheme:           cloneStackStringOverride(opts.HostnameTheme, src.HostnameTheme),
	}
	b.stacks.Put(s)

	return s.toStack(), nil
}

func cloneStackStringOverride(override, sourceValue string) string {
	if override != "" {
		return override
	}

	return sourceValue
}

func cloneStackBoolOverride(override, sourceValue *bool) *bool {
	if override != nil {
		return override
	}

	return sourceValue
}

func cloneStackConfigManager(override, sourceValue *StackConfigurationManager) *StackConfigurationManager {
	if override != nil {
		return override
	}

	return sourceValue
}

func cloneStackChefConfig(override, sourceValue *ChefConfiguration) *ChefConfiguration {
	if override != nil {
		return override
	}

	return sourceValue
}

// DescribeStacks returns stacks, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeStacks(stackIDs []string) ([]*Stack, error) {
	b.mu.RLock("DescribeStacks")
	defer b.mu.RUnlock()

	if len(stackIDs) > 0 {
		result := make([]*Stack, 0, len(stackIDs))
		for _, id := range stackIDs {
			s, ok := b.stacks.Get(id)
			if !ok {
				return nil, ErrStackNotFound
			}
			result = append(result, s.toStack())
		}

		return result, nil
	}

	all := b.stacks.All()
	result := make([]*Stack, 0, len(all))
	for _, s := range all {
		result = append(result, s.toStack())
	}

	return result, nil
}

// UpdateStack updates a stack's name and attributes. Every opts field left
// at its zero value leaves the current stored value unchanged, matching a
// real PATCH-style update where an omitted member means "no change".
func (b *InMemoryBackend) UpdateStack(stackID, name string, opts UpdateStackOptions) error {
	b.mu.Lock("UpdateStack")
	defer b.mu.Unlock()

	s, ok := b.stacks.Get(stackID)
	if !ok {
		return ErrStackNotFound
	}

	if name != "" {
		s.Name = name
	}

	applyUpdateStackOptions(s, opts)

	return nil
}

func applyUpdateStackOptions(s *storedStack, opts UpdateStackOptions) {
	if opts.ConfigurationManager != nil {
		s.ConfigurationManager = opts.ConfigurationManager
	}
	if opts.ChefConfiguration != nil {
		s.ChefConfiguration = opts.ChefConfiguration
	}
	if opts.UseOpsworksSecurityGroups != nil {
		s.UseOpsworksSecurityGroups = opts.UseOpsworksSecurityGroups
	}
	if opts.Attributes != nil {
		s.Attributes = opts.Attributes
	}
	if opts.AgentVersion != "" {
		s.AgentVersion = opts.AgentVersion
	}
	if opts.CustomJSON != "" {
		s.CustomJSON = opts.CustomJSON
	}
	if opts.DefaultAvailabilityZone != "" {
		s.DefaultAvailabilityZone = opts.DefaultAvailabilityZone
	}
	if opts.DefaultInstanceProfileArn != "" {
		s.DefaultInstanceProfileArn = opts.DefaultInstanceProfileArn
	}
	if opts.DefaultOs != "" {
		s.DefaultOs = opts.DefaultOs
	}
	if opts.DefaultRootDeviceType != "" {
		s.DefaultRootDeviceType = opts.DefaultRootDeviceType
	}
	if opts.DefaultSSHKeyName != "" {
		s.DefaultSSHKeyName = opts.DefaultSSHKeyName
	}
	if opts.DefaultSubnetID != "" {
		s.DefaultSubnetID = opts.DefaultSubnetID
	}
	if opts.HostnameTheme != "" {
		s.HostnameTheme = opts.HostnameTheme
	}
	if opts.ServiceRoleArn != "" {
		s.ServiceRoleArn = opts.ServiceRoleArn
	}
}

// deleteStackResources removes layers, instances, apps, and deployments for a stack (caller holds lock).
func (b *InMemoryBackend) deleteStackResources(stackID string) {
	for _, l := range slices.Clone(b.layersByStack.Get(stackID)) {
		b.layers.Delete(l.LayerID)
	}
	for _, i := range slices.Clone(b.instancesByStack.Get(stackID)) {
		b.instances.Delete(i.InstanceID)
	}
	for _, a := range slices.Clone(b.appsByStack.Get(stackID)) {
		b.apps.Delete(a.AppID)
	}
	for _, d := range slices.Clone(b.deploymentsByStack.Get(stackID)) {
		b.deployments.Delete(d.DeploymentID)
	}
}

// deleteStackAssociations removes permissions, volumes, RDS instances, and ECS clusters for a stack
// (caller holds lock).
func (b *InMemoryBackend) deleteStackAssociations(stackID string) {
	for _, p := range slices.Clone(b.permissionsByStack.Get(stackID)) {
		b.permissions.Delete(permissionKey(p.StackID, p.IamUserArn))
	}
	for _, v := range slices.Clone(b.volumesByStack.Get(stackID)) {
		b.volumes.Delete(v.VolumeID)
	}
	for _, r := range slices.Clone(b.rdsDBInstancesByStack.Get(stackID)) {
		b.rdsDBInstances.Delete(r.RdsDBInstanceArn)
	}
	for _, e := range slices.Clone(b.ecsClustersByStack.Get(stackID)) {
		b.ecsClusters.Delete(e.EcsClusterArn)
	}
}

// deleteStackChildren removes all resources belonging to a stack (caller holds lock).
func (b *InMemoryBackend) deleteStackChildren(stackID string) {
	b.deleteStackResources(stackID)
	b.deleteStackAssociations(stackID)
}

// DeleteStack deletes a stack. AWS requires all instances, layers, and apps
// be deleted or deregistered first (api_op_DeleteStack.go: "You must first
// delete all instances, layers, and apps or deregister registered
// instances.").
func (b *InMemoryBackend) DeleteStack(stackID string) error {
	b.mu.Lock("DeleteStack")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return ErrStackNotFound
	}

	if len(b.instancesByStack.Get(stackID)) > 0 ||
		len(b.layersByStack.Get(stackID)) > 0 ||
		len(b.appsByStack.Get(stackID)) > 0 {
		return ErrValidation
	}

	b.deleteStackChildren(stackID)

	stackArn := b.stackARN(stackID)
	delete(b.tags, stackArn)
	b.stacks.Delete(stackID)

	return nil
}

// StartStack transitions all stopped instances in a stack to online. Real AWS
// OpsWorks moves an instance through several transient states (requested,
// pending, booting, running_setup) before it reaches online; this backend has
// no time-based scheduler (see CreateDeployment's synchronous-completion
// convention), so it commits directly to the terminal state rather than
// leaving the instance parked in a transient status that nothing ever
// advances -- a stuck "starting" status previously left DescribeInstances
// pollers spinning forever.
func (b *InMemoryBackend) StartStack(stackID string) error {
	b.mu.Lock("StartStack")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return ErrStackNotFound
	}

	for _, i := range b.instancesByStack.Get(stackID) {
		if i.Status == instanceStatusStopped {
			i.Status = instanceStatusOnline
		}
	}

	return nil
}

// StopStack transitions all online instances in a stack to stopped. See
// StartStack's doc comment for why this commits directly to the terminal
// state instead of parking instances in the transient "stopping" status.
func (b *InMemoryBackend) StopStack(stackID string) error {
	b.mu.Lock("StopStack")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return ErrStackNotFound
	}

	for _, i := range b.instancesByStack.Get(stackID) {
		if i.Status == instanceStatusOnline {
			i.Status = instanceStatusStopped
		}
	}

	return nil
}

// GetHostnameSuggestion returns a suggested hostname for a new instance on
// the given layer. The real GetHostnameSuggestionInput has no StackId member
// -- only LayerId -- so this must key off the layer, not a stack.
func (b *InMemoryBackend) GetHostnameSuggestion(layerID string) (string, error) {
	b.mu.RLock("GetHostnameSuggestion")
	defer b.mu.RUnlock()

	if !b.layers.Has(layerID) {
		return "", ErrLayerNotFound
	}

	suffix := uuid.NewString()[:8]

	return fmt.Sprintf("gopherstack-%s", suffix), nil
}

// DescribeStackSummary returns summary counts for a stack.
func (b *InMemoryBackend) DescribeStackSummary(stackID string) (*StackSummary, error) {
	b.mu.RLock("DescribeStackSummary")
	defer b.mu.RUnlock()

	s, ok := b.stacks.Get(stackID)
	if !ok {
		return nil, ErrStackNotFound
	}

	counts := &InstancesCount{}
	for _, i := range b.instancesByStack.Get(stackID) {
		switch i.Status {
		case instanceStatusOnline:
			counts.Online++
		case instanceStatusStopped:
			counts.Stopped++
		}
	}

	var layerCount, appCount, deploymentCount int32
	for range b.layersByStack.Get(stackID) {
		layerCount++
	}
	for range b.appsByStack.Get(stackID) {
		appCount++
	}
	for range b.deploymentsByStack.Get(stackID) {
		deploymentCount++
	}

	return &StackSummary{
		StackID:          stackID,
		Arn:              s.Arn,
		Name:             s.Name,
		InstancesCount:   counts,
		LayersCount:      layerCount,
		AppsCount:        appCount,
		DeploymentsCount: deploymentCount,
	}, nil
}

// DescribeStackProvisioningParameters returns provisioning parameters for a
// stack. The real DescribeStackProvisioningParametersOutput has
// AgentInstallerUrl and Parameters as two SEPARATE members (confirmed
// against aws-sdk-go-v2/service/opsworks@v1.31.0's
// api_op_DescribeStackProvisioningParameters.go) -- no StackArn, so this
// returns just the two, not the stack's ARN.
//
// Parameters is returned empty rather than fabricated: AWS's real Parameters
// map holds internal agent-bootstrap config (e.g. agent_installer_base_url,
// instance_service_endpoint, ops_works_region, charlie_public_key), none of
// which this backend tracks. AgentInstallerUrl itself is NOT one of those
// keys -- a previous version of this method put "AgentInstallerUrl" inside
// Parameters too, duplicating the dedicated top-level field under a
// fabricated key.
func (b *InMemoryBackend) DescribeStackProvisioningParameters(stackID string) (string, map[string]string, error) {
	b.mu.RLock("DescribeStackProvisioningParameters")
	defer b.mu.RUnlock()

	if !b.stacks.Has(stackID) {
		return "", nil, ErrStackNotFound
	}

	agentInstallerURL := fmt.Sprintf(
		"https://opsworks-instance-agent.s3.amazonaws.com/latest/install/%s",
		b.region,
	)

	return agentInstallerURL, map[string]string{}, nil
}
