package mgn

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// GetReplicationConfiguration/UpdateReplicationConfiguration are per-SourceServer
// and flattened (PARITY.md wire-trap #2, no types.ReplicationConfiguration struct
// exists), separate from the account-level, reusable Template family. Same "no
// exposed template -> per-server application mechanism" gap as launchconfig.go.

// GetReplicationConfiguration returns sourceServerID's per-server
// ReplicationConfiguration.
func (b *InMemoryBackend) GetReplicationConfiguration(sourceServerID string) (*ReplicationConfiguration, error) {
	b.mu.RLock("GetReplicationConfiguration")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	rc, ok := b.replicationConfigs.Get(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	return rc.clone(), nil
}

// UpdateReplicationConfigurationInput mirrors
// UpdateReplicationConfigurationInput -- every field but SourceServerID is
// optional.
type UpdateReplicationConfigurationInput struct {
	DefaultLargeStagingDiskType         *string
	EbsEncryptionKeyArn                 *string
	UseFipsEndpoint                     *bool
	UseDedicatedReplicationServer       *bool
	AssociateDefaultSecurityGroup       *bool
	BandwidthThrottling                 *int64
	CreatePublicIP                      *bool
	DataPlaneRouting                    *string
	StagingAreaTags                     map[string]string
	StorageConfiguration                *StorageConfiguration
	EbsEncryption                       *string
	InternetProtocol                    *string
	Name                                *string
	ReplicationServerInstanceType       *string
	StagingAreaSubnetID                 *string
	StoreSnapshotOnLocalZone            *bool
	ReplicationServersSecurityGroupsIDs []string
	ReplicatedDisks                     []ReplicationConfigurationReplicatedDisk
}

// UpdateReplicationConfiguration applies a partial update to sourceServerID's
// per-server ReplicationConfiguration.
func (b *InMemoryBackend) UpdateReplicationConfiguration(
	sourceServerID string,
	in UpdateReplicationConfigurationInput,
) (*ReplicationConfiguration, error) {
	b.mu.Lock("UpdateReplicationConfiguration")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	rc, ok := b.replicationConfigs.Get(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	applyReplicationConfigUpdate(rc, in)

	return rc.clone(), nil
}

// applyReplicationConfigUpdate applies in's set fields onto rc, split across
// two helpers purely to keep each function's branch count low (decomposition,
// not suppression -- see .claude/memories/parity-principles.md's ban on
// cyclop/funlen/gocyclo/gocognit suppressions).
func applyReplicationConfigUpdate(rc *ReplicationConfiguration, in UpdateReplicationConfigurationInput) {
	applyReplicationConfigUpdateShapes(rc, in)
	applyReplicationConfigUpdateScalars(rc, in)
}

func applyReplicationConfigUpdateShapes(rc *ReplicationConfiguration, in UpdateReplicationConfigurationInput) {
	if in.StorageConfiguration != nil {
		rc.StorageConfiguration = in.StorageConfiguration
	}

	if in.StagingAreaTags != nil {
		rc.StagingAreaTags = in.StagingAreaTags
	}

	if in.ReplicatedDisks != nil {
		rc.ReplicatedDisks = in.ReplicatedDisks
	}

	if in.ReplicationServersSecurityGroupsIDs != nil {
		rc.ReplicationServersSecurityGroupsIDs = in.ReplicationServersSecurityGroupsIDs
	}

	if in.Name != nil {
		rc.Name = *in.Name
	}

	if in.ReplicationServerInstanceType != nil {
		rc.ReplicationServerInstanceType = *in.ReplicationServerInstanceType
	}

	if in.StagingAreaSubnetID != nil {
		rc.StagingAreaSubnetID = *in.StagingAreaSubnetID
	}
}

// applyReplicationConfigUpdateScalars applies in's scalar fields onto rc.
//
// resource kinds (per-server vs. account-level reusable template, PARITY.md family D) that happen
// to share most scalar field names -- see applyReplicationTemplateUpdateScalars's sibling below.
//
//nolint:dupl // ReplicationConfiguration and ReplicationConfigurationTemplate are genuinely distinct
func applyReplicationConfigUpdateScalars(rc *ReplicationConfiguration, in UpdateReplicationConfigurationInput) {
	if in.AssociateDefaultSecurityGroup != nil {
		rc.AssociateDefaultSecurityGroup = *in.AssociateDefaultSecurityGroup
	}

	if in.BandwidthThrottling != nil {
		rc.BandwidthThrottling = *in.BandwidthThrottling
	}

	if in.CreatePublicIP != nil {
		rc.CreatePublicIP = *in.CreatePublicIP
	}

	if in.DataPlaneRouting != nil {
		rc.DataPlaneRouting = *in.DataPlaneRouting
	}

	if in.DefaultLargeStagingDiskType != nil {
		rc.DefaultLargeStagingDiskType = *in.DefaultLargeStagingDiskType
	}

	if in.EbsEncryption != nil {
		rc.EbsEncryption = *in.EbsEncryption
	}

	if in.EbsEncryptionKeyArn != nil {
		rc.EbsEncryptionKeyArn = *in.EbsEncryptionKeyArn
	}

	if in.InternetProtocol != nil {
		rc.InternetProtocol = *in.InternetProtocol
	}

	if in.StoreSnapshotOnLocalZone != nil {
		rc.StoreSnapshotOnLocalZone = *in.StoreSnapshotOnLocalZone
	}

	if in.UseDedicatedReplicationServer != nil {
		rc.UseDedicatedReplicationServer = *in.UseDedicatedReplicationServer
	}

	if in.UseFipsEndpoint != nil {
		rc.UseFipsEndpoint = *in.UseFipsEndpoint
	}
}

// CreateReplicationConfigurationTemplateInput mirrors
// CreateReplicationConfigurationTemplateInput -- 11 fields are required
// (confirmed by direct SDK read; PARITY.md's note on the asymmetry with
// LaunchConfigurationTemplate, where nothing is required).
type CreateReplicationConfigurationTemplateInput struct {
	StorageConfiguration                *StorageConfiguration
	Tags                                map[string]string
	StagingAreaTags                     map[string]string
	InternetProtocol                    string
	StagingAreaSubnetID                 string
	DefaultLargeStagingDiskType         string
	EbsEncryption                       string
	EbsEncryptionKeyArn                 string
	DataPlaneRouting                    string
	ReplicationServerInstanceType       string
	ReplicationServersSecurityGroupsIDs []string
	BandwidthThrottling                 int64
	AssociateDefaultSecurityGroup       bool
	CreatePublicIP                      bool
	StoreSnapshotOnLocalZone            bool
	UseDedicatedReplicationServer       bool
	UseFipsEndpoint                     bool
}

// CreateReplicationConfigurationTemplate creates a new, account-level,
// reusable ReplicationConfigurationTemplate.
func (b *InMemoryBackend) CreateReplicationConfigurationTemplate(
	in CreateReplicationConfigurationTemplateInput,
) (*ReplicationConfigurationTemplate, error) {
	b.mu.Lock("CreateReplicationConfigurationTemplate")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	if in.ReplicationServerInstanceType == "" {
		return nil, validationError("replicationServerInstanceType is required")
	}

	if in.StagingAreaSubnetID == "" {
		return nil, validationError("stagingAreaSubnetID is required")
	}

	if len(in.ReplicationServersSecurityGroupsIDs) == 0 {
		return nil, validationError("replicationServersSecurityGroupsIDs is required")
	}

	id := newReplicationTemplateID()
	t := tags.New("mgn.replicationtemplate." + id + ".tags")
	t.Merge(in.Tags)

	tmpl := &ReplicationConfigurationTemplate{
		ReplicationConfigurationTemplateID:  id,
		Arn:                                 b.replicationTemplateARN(id),
		Tags:                                t,
		StorageConfiguration:                in.StorageConfiguration,
		StagingAreaTags:                     cloneStrMap(in.StagingAreaTags),
		ReplicationServersSecurityGroupsIDs: append([]string(nil), in.ReplicationServersSecurityGroupsIDs...),
		DataPlaneRouting:                    in.DataPlaneRouting,
		DefaultLargeStagingDiskType:         in.DefaultLargeStagingDiskType,
		EbsEncryption:                       in.EbsEncryption,
		EbsEncryptionKeyArn:                 in.EbsEncryptionKeyArn,
		InternetProtocol:                    in.InternetProtocol,
		ReplicationServerInstanceType:       in.ReplicationServerInstanceType,
		StagingAreaSubnetID:                 in.StagingAreaSubnetID,
		AssociateDefaultSecurityGroup:       in.AssociateDefaultSecurityGroup,
		CreatePublicIP:                      in.CreatePublicIP,
		StoreSnapshotOnLocalZone:            in.StoreSnapshotOnLocalZone,
		UseDedicatedReplicationServer:       in.UseDedicatedReplicationServer,
		UseFipsEndpoint:                     in.UseFipsEndpoint,
		BandwidthThrottling:                 in.BandwidthThrottling,
	}
	b.replicationTemplates.Put(tmpl)

	return tmpl.clone(), nil
}

// DeleteReplicationConfigurationTemplate deletes a
// ReplicationConfigurationTemplate.
func (b *InMemoryBackend) DeleteReplicationConfigurationTemplate(id string) error {
	b.mu.Lock("DeleteReplicationConfigurationTemplate")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	tmpl, ok := b.replicationTemplates.Get(id)
	if !ok {
		return notFoundError(resourceReplicationTemplate, id)
	}

	if tmpl.Tags != nil {
		tmpl.Tags.Close()
	}

	b.replicationTemplates.Delete(id)

	return nil
}

// DescribeReplicationConfigurationTemplates returns a page of
// ReplicationConfigurationTemplates, optionally filtered by ids.
func (b *InMemoryBackend) DescribeReplicationConfigurationTemplates(
	ids []string,
	token string,
	limit int,
) (page.Page[*ReplicationConfigurationTemplate], error) {
	b.mu.RLock("DescribeReplicationConfigurationTemplates")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*ReplicationConfigurationTemplate]{}, err
	}

	all := b.replicationTemplates.Snapshot()
	filtered := make([]*ReplicationConfigurationTemplate, 0, len(all))

	for _, t := range all {
		if len(ids) == 0 || containsStr(ids, t.ReplicationConfigurationTemplateID) {
			filtered = append(filtered, t.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit), nil
}

// UpdateReplicationConfigurationTemplateInput mirrors
// UpdateReplicationConfigurationTemplateInput -- everything but
// ReplicationConfigurationTemplateID is optional.
type UpdateReplicationConfigurationTemplateInput struct {
	ReplicationServerInstanceType       *string
	StagingAreaSubnetID                 *string
	BandwidthThrottling                 *int64
	DataPlaneRouting                    *string
	DefaultLargeStagingDiskType         *string
	EbsEncryption                       *string
	StagingAreaTags                     map[string]string
	EbsEncryptionKeyArn                 *string
	StorageConfiguration                *StorageConfiguration
	InternetProtocol                    *string
	AssociateDefaultSecurityGroup       *bool
	CreatePublicIP                      *bool
	StoreSnapshotOnLocalZone            *bool
	UseDedicatedReplicationServer       *bool
	UseFipsEndpoint                     *bool
	ReplicationServersSecurityGroupsIDs []string
}

// UpdateReplicationConfigurationTemplate applies a partial update to id.
func (b *InMemoryBackend) UpdateReplicationConfigurationTemplate(
	id string,
	in UpdateReplicationConfigurationTemplateInput,
) (*ReplicationConfigurationTemplate, error) {
	b.mu.Lock("UpdateReplicationConfigurationTemplate")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	tmpl, ok := b.replicationTemplates.Get(id)
	if !ok {
		return nil, notFoundError(resourceReplicationTemplate, id)
	}

	applyReplicationTemplateUpdateShapes(tmpl, in)
	applyReplicationTemplateUpdateScalars(tmpl, in)

	return tmpl.clone(), nil
}

// applyReplicationTemplateUpdateShapes/Scalars apply in's set fields onto
// tmpl, split purely to keep each function's branch count low
// (decomposition, not suppression -- see
// .claude/memories/parity-principles.md's ban on cyclop/funlen/gocyclo/
// gocognit suppressions).
func applyReplicationTemplateUpdateShapes(
	tmpl *ReplicationConfigurationTemplate,
	in UpdateReplicationConfigurationTemplateInput,
) {
	if in.StorageConfiguration != nil {
		tmpl.StorageConfiguration = in.StorageConfiguration
	}

	if in.StagingAreaTags != nil {
		tmpl.StagingAreaTags = cloneStrMap(in.StagingAreaTags)
	}

	if in.ReplicationServersSecurityGroupsIDs != nil {
		tmpl.ReplicationServersSecurityGroupsIDs = append([]string(nil), in.ReplicationServersSecurityGroupsIDs...)
	}

	if in.ReplicationServerInstanceType != nil {
		tmpl.ReplicationServerInstanceType = *in.ReplicationServerInstanceType
	}

	if in.StagingAreaSubnetID != nil {
		tmpl.StagingAreaSubnetID = *in.StagingAreaSubnetID
	}
}

//nolint:dupl // see applyReplicationConfigUpdateScalars's sibling doc comment above
func applyReplicationTemplateUpdateScalars(
	tmpl *ReplicationConfigurationTemplate,
	in UpdateReplicationConfigurationTemplateInput,
) {
	if in.DataPlaneRouting != nil {
		tmpl.DataPlaneRouting = *in.DataPlaneRouting
	}

	if in.DefaultLargeStagingDiskType != nil {
		tmpl.DefaultLargeStagingDiskType = *in.DefaultLargeStagingDiskType
	}

	if in.EbsEncryption != nil {
		tmpl.EbsEncryption = *in.EbsEncryption
	}

	if in.EbsEncryptionKeyArn != nil {
		tmpl.EbsEncryptionKeyArn = *in.EbsEncryptionKeyArn
	}

	if in.InternetProtocol != nil {
		tmpl.InternetProtocol = *in.InternetProtocol
	}

	if in.AssociateDefaultSecurityGroup != nil {
		tmpl.AssociateDefaultSecurityGroup = *in.AssociateDefaultSecurityGroup
	}

	if in.CreatePublicIP != nil {
		tmpl.CreatePublicIP = *in.CreatePublicIP
	}

	if in.StoreSnapshotOnLocalZone != nil {
		tmpl.StoreSnapshotOnLocalZone = *in.StoreSnapshotOnLocalZone
	}

	if in.UseDedicatedReplicationServer != nil {
		tmpl.UseDedicatedReplicationServer = *in.UseDedicatedReplicationServer
	}

	if in.UseFipsEndpoint != nil {
		tmpl.UseFipsEndpoint = *in.UseFipsEndpoint
	}

	if in.BandwidthThrottling != nil {
		tmpl.BandwidthThrottling = *in.BandwidthThrottling
	}
}
