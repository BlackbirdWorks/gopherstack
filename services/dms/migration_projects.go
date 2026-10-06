package dms

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// DataProviderDescriptorInput is the request-side shape of a data provider
// descriptor entry within CreateMigrationProject's Source/TargetDataProviderDescriptors.
type DataProviderDescriptorInput struct {
	DataProviderIdentifier      string
	SecretsManagerAccessRoleArn string
	SecretsManagerSecretId      string //nolint:revive,staticcheck // matches the AWS wire field name.
}

// resolveDataProviderDescriptors resolves each entry's DataProviderIdentifier
// (name or ARN) against the DataProvider store, preserving the caller's
// Secrets Manager pass-through fields. Must hold b.mu.
func (b *InMemoryBackend) resolveDataProviderDescriptors(
	ctx context.Context, entries []DataProviderDescriptorInput,
) ([]DataProviderDescriptor, error) {
	out := make([]DataProviderDescriptor, 0, len(entries))

	for _, e := range entries {
		if e.DataProviderIdentifier == "" {
			return nil, fmt.Errorf("%w: DataProviderIdentifier is required", ErrValidation)
		}

		dp := b.findDataProvider(ctx, e.DataProviderIdentifier)
		if dp == nil {
			return nil, fmt.Errorf("%w: data provider %s not found", ErrNotFound, e.DataProviderIdentifier)
		}

		out = append(out, DataProviderDescriptor{
			DataProviderArn:             dp.DataProviderArn,
			DataProviderName:            dp.DataProviderName,
			SecretsManagerAccessRoleArn: e.SecretsManagerAccessRoleArn,
			SecretsManagerSecretId:      e.SecretsManagerSecretId,
		})
	}

	return out, nil
}

// CreateMigrationProject creates a migration project.
func (b *InMemoryBackend) CreateMigrationProject(
	ctx context.Context,
	p CreateMigrationProjectParams,
) (*MigrationProject, error) {
	b.mu.Lock("CreateMigrationProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	name := p.Name

	if b.migrationProjects.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: migration project %s already exists", ErrAlreadyExists, name)
	}

	if p.InstanceProfileIdentifier == "" {
		return nil, fmt.Errorf("%w: InstanceProfileIdentifier is required", ErrValidation)
	}

	ip := b.findInstanceProfile(ctx, p.InstanceProfileIdentifier)
	if ip == nil {
		return nil, fmt.Errorf("%w: instance profile %s not found", ErrNotFound, p.InstanceProfileIdentifier)
	}

	if p.SourceDescriptors == nil {
		return nil, fmt.Errorf("%w: SourceDataProviderDescriptors is required", ErrValidation)
	}

	if p.TargetDescriptors == nil {
		return nil, fmt.Errorf("%w: TargetDataProviderDescriptors is required", ErrValidation)
	}

	sourceResolved, err := b.resolveDataProviderDescriptors(ctx, p.SourceDescriptors)
	if err != nil {
		return nil, err
	}

	targetResolved, err := b.resolveDataProviderDescriptors(ctx, p.TargetDescriptors)
	if err != nil {
		return nil, err
	}

	projectARN := arn.Build("dms", region, b.accountID, "migration-project:"+uuid.NewString())
	t := tags.New("dms.migration-project." + name + ".tags")
	if len(p.Tags) > 0 {
		t.Merge(p.Tags)
	}
	mp := &MigrationProject{
		MigrationProjectName:                  name,
		MigrationProjectArn:                   projectARN,
		MigrationProjectIdentifier:            name,
		Description:                           p.Description,
		AccountID:                             b.accountID,
		Region:                                region,
		InstanceProfileArn:                    ip.InstanceProfileArn,
		InstanceProfileName:                   ip.InstanceProfileName,
		SourceDataProviderDescriptors:         sourceResolved,
		TargetDataProviderDescriptors:         targetResolved,
		SchemaConversionApplicationAttributes: p.SchemaConversionApplicationAttributes,
		TransformationRules:                   p.TransformationRules,
		Tags:                                  t,
	}
	b.migrationProjects.Put(mp)
	cp := *mp

	return &cp, nil
}

// DeleteMigrationProject deletes a migration project by name or ARN.
func (b *InMemoryBackend) DeleteMigrationProject(ctx context.Context, nameOrArn string) error {
	b.mu.Lock("DeleteMigrationProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if mp, ok := b.migrationProjects.Get(regionKey(region, nameOrArn)); ok {
		mp.Tags.Close()
		b.migrationProjects.Delete(regionKey(region, nameOrArn))

		return nil
	}

	if mp, ok := lookupUnique(b.migrationProjectsByARN, regionKey(region, nameOrArn)); ok {
		mp.Tags.Close()
		b.migrationProjects.Delete(regionKey(region, mp.MigrationProjectName))

		return nil
	}

	return fmt.Errorf("%w: migration project %s not found", ErrNotFound, nameOrArn)
}

// DescribeMigrationProjects returns all migration projects.
func (b *InMemoryBackend) DescribeMigrationProjects(ctx context.Context) ([]*MigrationProject, error) {
	b.mu.RLock("DescribeMigrationProjects")
	defer b.mu.RUnlock()

	items := b.migrationProjectsByRegion.Get(getRegion(ctx, b.region))
	list := make([]*MigrationProject, 0, len(items))
	for _, mp := range items {
		cp := *mp
		list = append(list, &cp)
	}

	return list, nil
}

// findMigrationProject locates a project by name or ARN (must hold a lock).
func (b *InMemoryBackend) findMigrationProject(ctx context.Context, nameOrArn string) *MigrationProject {
	region := getRegion(ctx, b.region)
	if mp, ok := b.migrationProjects.Get(regionKey(region, nameOrArn)); ok {
		return mp
	}

	if mp, ok := lookupUnique(b.migrationProjectsByARN, regionKey(region, nameOrArn)); ok {
		return mp
	}

	return nil
}

// ModifyMigrationProject applies the supplied members to an existing project;
// omitted members keep their stored values.
func (b *InMemoryBackend) ModifyMigrationProject(
	ctx context.Context,
	p ModifyMigrationProjectParams,
) (*MigrationProject, error) {
	b.mu.Lock("ModifyMigrationProject")
	defer b.mu.Unlock()

	mp := b.findMigrationProject(ctx, p.NameOrArn)
	if mp == nil {
		return nil, fmt.Errorf("%w: migration project %s not found", ErrNotFound, p.NameOrArn)
	}

	changes, err := b.resolveProjectChanges(ctx, p)
	if err != nil {
		return nil, err
	}

	if p.MigrationProjectName != nil {
		if err = rekey(b.migrationProjects, getRegion(ctx, b.region), mp.MigrationProjectName,
			*p.MigrationProjectName, "migration project", mp, func(n string) {
				mp.MigrationProjectName, mp.MigrationProjectIdentifier = n, n
			}); err != nil {
			return nil, err
		}
	}

	applyMigrationProjectChanges(mp, p, changes.ip, changes.source, changes.target)
	cp := *mp

	return &cp, nil
}

type projectChanges struct {
	ip             *InstanceProfile
	source, target []DataProviderDescriptor
}

// resolveProjectChanges looks up the instance profile and data providers a
// modify request names, before anything is mutated. Must hold b.mu.
func (b *InMemoryBackend) resolveProjectChanges(
	ctx context.Context, p ModifyMigrationProjectParams,
) (projectChanges, error) {
	var (
		out projectChanges
		err error
	)

	if p.InstanceProfileIdentifier != nil && *p.InstanceProfileIdentifier != "" {
		out.ip = b.findInstanceProfile(ctx, *p.InstanceProfileIdentifier)
		if out.ip == nil {
			return out, fmt.Errorf("%w: instance profile %s not found", ErrNotFound, *p.InstanceProfileIdentifier)
		}
	}

	if p.SourceDescriptors != nil {
		if out.source, err = b.resolveDataProviderDescriptors(ctx, p.SourceDescriptors); err != nil {
			return out, err
		}
	}

	if p.TargetDescriptors != nil {
		out.target, err = b.resolveDataProviderDescriptors(ctx, p.TargetDescriptors)
	}

	return out, err
}

func applyMigrationProjectChanges(
	mp *MigrationProject,
	p ModifyMigrationProjectParams,
	ip *InstanceProfile,
	source, target []DataProviderDescriptor,
) {
	if p.Description != nil {
		mp.Description = *p.Description
	}

	if ip != nil {
		mp.InstanceProfileArn = ip.InstanceProfileArn
		mp.InstanceProfileName = ip.InstanceProfileName
	}

	if p.SourceDescriptors != nil {
		mp.SourceDataProviderDescriptors = source
	}

	if p.TargetDescriptors != nil {
		mp.TargetDataProviderDescriptors = target
	}

	if p.SchemaConversionApplicationAttributes != nil {
		mp.SchemaConversionApplicationAttributes = p.SchemaConversionApplicationAttributes
	}

	if p.TransformationRules != nil {
		mp.TransformationRules = *p.TransformationRules
	}
}
