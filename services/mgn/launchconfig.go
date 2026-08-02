package mgn

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file backs family C (6 ops): GetLaunchConfiguration,
// UpdateLaunchConfiguration (per-SourceServer, flattened -- PARITY.md
// wire-trap #2, no types.LaunchConfiguration struct exists) plus
// CreateLaunchConfigurationTemplate, DeleteLaunchConfigurationTemplate,
// DescribeLaunchConfigurationTemplates, UpdateLaunchConfigurationTemplate
// (the separate, account-level, reusable Template family).
//
// # How template -> per-server configuration application happens
//
// Not exposed by any op in this SDK (PARITY.md's "genuine, unresolved gap").
// This backend's documented convention: a per-server LaunchConfiguration is
// auto-created with fixed defaults alongside its SourceServer
// (SeedSourceServer, in sourceservers.go) and never automatically inherits
// settings from any LaunchConfigurationTemplate -- an implementer/caller
// must explicitly UpdateLaunchConfiguration to copy template values across
// if desired. This is a documented, invented convention, not derived from
// AWS behavior.

// GetLaunchConfiguration returns sourceServerID's per-server
// LaunchConfiguration.
func (b *InMemoryBackend) GetLaunchConfiguration(sourceServerID string) (*LaunchConfiguration, error) {
	b.mu.RLock("GetLaunchConfiguration")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	lc, ok := b.launchConfigs.Get(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	return lc.clone(), nil
}

// UpdateLaunchConfigurationInput carries the partial-update fields
// UpdateLaunchConfiguration accepts, mirrored 1:1 from
// UpdateLaunchConfigurationInput -- every field but SourceServerID is
// optional, so callers pass a pointer/hasX flag per field via
// wire_convert.go's applyLaunchConfigUpdate rather than a single struct
// literal, matching this style throughout the family files.
type UpdateLaunchConfigurationInput struct {
	BootMode                            *string
	CopyPrivateIP                       *bool
	CopyTags                            *bool
	EnableMapAutoTagging                *bool
	LaunchDisposition                   *string
	Licensing                           *Licensing
	MapAutoTaggingMpeID                 *string
	Name                                *string
	PostLaunchActions                   *PostLaunchActions
	TargetInstanceTypeRightSizingMethod *string
}

// UpdateLaunchConfiguration applies a partial update to sourceServerID's
// per-server LaunchConfiguration.
func (b *InMemoryBackend) UpdateLaunchConfiguration(
	sourceServerID string,
	in UpdateLaunchConfigurationInput,
) (*LaunchConfiguration, error) {
	b.mu.Lock("UpdateLaunchConfiguration")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	lc, ok := b.launchConfigs.Get(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	applyLaunchConfigUpdate(lc, in)

	return lc.clone(), nil
}

func applyLaunchConfigUpdate(lc *LaunchConfiguration, in UpdateLaunchConfigurationInput) {
	if in.BootMode != nil {
		lc.BootMode = *in.BootMode
	}

	if in.CopyPrivateIP != nil {
		lc.CopyPrivateIP = *in.CopyPrivateIP
	}

	if in.CopyTags != nil {
		lc.CopyTags = *in.CopyTags
	}

	if in.EnableMapAutoTagging != nil {
		lc.EnableMapAutoTagging = *in.EnableMapAutoTagging
	}

	if in.LaunchDisposition != nil {
		lc.LaunchDisposition = *in.LaunchDisposition
	}

	if in.Licensing != nil {
		lc.Licensing = in.Licensing
	}

	if in.MapAutoTaggingMpeID != nil {
		lc.MapAutoTaggingMpeID = *in.MapAutoTaggingMpeID
	}

	if in.Name != nil {
		lc.Name = *in.Name
	}

	if in.PostLaunchActions != nil {
		lc.PostLaunchActions = in.PostLaunchActions
	}

	if in.TargetInstanceTypeRightSizingMethod != nil {
		lc.TargetInstanceTypeRightSizingMethod = *in.TargetInstanceTypeRightSizingMethod
	}
}

// CreateLaunchConfigurationTemplateInput mirrors
// CreateLaunchConfigurationTemplateInput -- nothing is required (confirmed
// by direct SDK read, PARITY.md's note on the required-vs-optional
// asymmetry with ReplicationConfigurationTemplate).
type CreateLaunchConfigurationTemplateInput struct {
	Licensing                           *Licensing
	PostLaunchActions                   *PostLaunchActions
	LargeVolumeConf                     *LaunchTemplateDiskConf
	SmallVolumeConf                     *LaunchTemplateDiskConf
	Tags                                map[string]string
	BootMode                            string
	Ec2LaunchTemplateID                 string
	LaunchDisposition                   string
	MapAutoTaggingMpeID                 string
	ParametersEncryptionKey             string
	TargetInstanceTypeRightSizingMethod string
	SmallVolumeMaxSize                  int64
	AssociatePublicIPAddress            bool
	CopyPrivateIP                       bool
	CopyTags                            bool
	EnableMapAutoTagging                bool
	EnableParametersEncryption          bool
}

// CreateLaunchConfigurationTemplate creates a new, account-level, reusable
// LaunchConfigurationTemplate.
func (b *InMemoryBackend) CreateLaunchConfigurationTemplate(
	in CreateLaunchConfigurationTemplateInput,
) (*LaunchConfigurationTemplate, error) {
	b.mu.Lock("CreateLaunchConfigurationTemplate")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	id := newLaunchTemplateID()
	t := tags.New("mgn.launchtemplate." + id + ".tags")
	t.Merge(in.Tags)

	tmpl := &LaunchConfigurationTemplate{
		LaunchConfigurationTemplateID:       id,
		Arn:                                 b.launchTemplateARN(id),
		Tags:                                t,
		Licensing:                           in.Licensing,
		PostLaunchActions:                   in.PostLaunchActions,
		LargeVolumeConf:                     in.LargeVolumeConf,
		SmallVolumeConf:                     in.SmallVolumeConf,
		BootMode:                            in.BootMode,
		Ec2LaunchTemplateID:                 in.Ec2LaunchTemplateID,
		LaunchDisposition:                   in.LaunchDisposition,
		MapAutoTaggingMpeID:                 in.MapAutoTaggingMpeID,
		ParametersEncryptionKey:             in.ParametersEncryptionKey,
		TargetInstanceTypeRightSizingMethod: in.TargetInstanceTypeRightSizingMethod,
		SmallVolumeMaxSize:                  in.SmallVolumeMaxSize,
		AssociatePublicIPAddress:            in.AssociatePublicIPAddress,
		CopyPrivateIP:                       in.CopyPrivateIP,
		CopyTags:                            in.CopyTags,
		EnableMapAutoTagging:                in.EnableMapAutoTagging,
		EnableParametersEncryption:          in.EnableParametersEncryption,
	}
	b.launchTemplates.Put(tmpl)

	return tmpl.clone(), nil
}

// DeleteLaunchConfigurationTemplate deletes a LaunchConfigurationTemplate.
func (b *InMemoryBackend) DeleteLaunchConfigurationTemplate(id string) error {
	b.mu.Lock("DeleteLaunchConfigurationTemplate")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	tmpl, ok := b.launchTemplates.Get(id)
	if !ok {
		return notFoundError(resourceLaunchTemplate, id)
	}

	if tmpl.Tags != nil {
		tmpl.Tags.Close()
	}

	b.launchTemplates.Delete(id)

	for _, a := range b.templateActionsByTemplate.Get(id) {
		b.templateActions.Delete(actionKey(a.LaunchConfigurationTemplateID, a.ActionID))
	}

	return nil
}

// DescribeLaunchConfigurationTemplates returns a page of
// LaunchConfigurationTemplates, optionally filtered by ids.
func (b *InMemoryBackend) DescribeLaunchConfigurationTemplates(
	ids []string,
	token string,
	limit int,
) (page.Page[*LaunchConfigurationTemplate], error) {
	b.mu.RLock("DescribeLaunchConfigurationTemplates")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*LaunchConfigurationTemplate]{}, err
	}

	all := b.launchTemplates.Snapshot()
	filtered := make([]*LaunchConfigurationTemplate, 0, len(all))

	for _, t := range all {
		if len(ids) == 0 || containsStr(ids, t.LaunchConfigurationTemplateID) {
			filtered = append(filtered, t.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit), nil
}

// UpdateLaunchConfigurationTemplateInput mirrors
// UpdateLaunchConfigurationTemplateInput -- everything but
// LaunchConfigurationTemplateID is optional.
type UpdateLaunchConfigurationTemplateInput struct {
	Licensing                           *Licensing
	PostLaunchActions                   *PostLaunchActions
	LargeVolumeConf                     *LaunchTemplateDiskConf
	SmallVolumeConf                     *LaunchTemplateDiskConf
	BootMode                            *string
	Ec2LaunchTemplateID                 *string
	LaunchDisposition                   *string
	MapAutoTaggingMpeID                 *string
	TargetInstanceTypeRightSizingMethod *string
	SmallVolumeMaxSize                  *int64
	AssociatePublicIPAddress            *bool
	CopyPrivateIP                       *bool
	CopyTags                            *bool
	EnableMapAutoTagging                *bool
}

// UpdateLaunchConfigurationTemplate applies a partial update to id.
func (b *InMemoryBackend) UpdateLaunchConfigurationTemplate(
	id string,
	in UpdateLaunchConfigurationTemplateInput,
) (*LaunchConfigurationTemplate, error) {
	b.mu.Lock("UpdateLaunchConfigurationTemplate")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	tmpl, ok := b.launchTemplates.Get(id)
	if !ok {
		return nil, notFoundError(resourceLaunchTemplate, id)
	}

	applyLaunchTemplateUpdateShapes(tmpl, in)
	applyLaunchTemplateUpdateScalars(tmpl, in)

	return tmpl.clone(), nil
}

// applyLaunchTemplateUpdateShapes/Scalars apply in's set fields onto tmpl,
// split purely to keep each function's branch count low (decomposition,
// not suppression -- see .claude/memories/parity-principles.md's ban on
// cyclop/funlen/gocyclo/gocognit suppressions).
func applyLaunchTemplateUpdateShapes(tmpl *LaunchConfigurationTemplate, in UpdateLaunchConfigurationTemplateInput) {
	if in.Licensing != nil {
		tmpl.Licensing = in.Licensing
	}

	if in.PostLaunchActions != nil {
		tmpl.PostLaunchActions = in.PostLaunchActions
	}

	if in.LargeVolumeConf != nil {
		tmpl.LargeVolumeConf = in.LargeVolumeConf
	}

	if in.SmallVolumeConf != nil {
		tmpl.SmallVolumeConf = in.SmallVolumeConf
	}

	if in.BootMode != nil {
		tmpl.BootMode = *in.BootMode
	}

	if in.Ec2LaunchTemplateID != nil {
		tmpl.Ec2LaunchTemplateID = *in.Ec2LaunchTemplateID
	}
}

func applyLaunchTemplateUpdateScalars(tmpl *LaunchConfigurationTemplate, in UpdateLaunchConfigurationTemplateInput) {
	if in.LaunchDisposition != nil {
		tmpl.LaunchDisposition = *in.LaunchDisposition
	}

	if in.MapAutoTaggingMpeID != nil {
		tmpl.MapAutoTaggingMpeID = *in.MapAutoTaggingMpeID
	}

	if in.TargetInstanceTypeRightSizingMethod != nil {
		tmpl.TargetInstanceTypeRightSizingMethod = *in.TargetInstanceTypeRightSizingMethod
	}

	if in.SmallVolumeMaxSize != nil {
		tmpl.SmallVolumeMaxSize = *in.SmallVolumeMaxSize
	}

	if in.AssociatePublicIPAddress != nil {
		tmpl.AssociatePublicIPAddress = *in.AssociatePublicIPAddress
	}

	if in.CopyPrivateIP != nil {
		tmpl.CopyPrivateIP = *in.CopyPrivateIP
	}

	if in.CopyTags != nil {
		tmpl.CopyTags = *in.CopyTags
	}

	if in.EnableMapAutoTagging != nil {
		tmpl.EnableMapAutoTagging = *in.EnableMapAutoTagging
	}
}
