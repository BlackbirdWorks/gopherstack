package dms

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/google/uuid"
)

// CreateMigrationProject creates a migration project.
func (b *InMemoryBackend) CreateMigrationProject(
	ctx context.Context,
	name, description string,
	kv map[string]string,
) (*MigrationProject, error) {
	b.mu.Lock("CreateMigrationProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.migrationProjects.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: migration project %s already exists", ErrAlreadyExists, name)
	}

	projectARN := arn.Build("dms", region, b.accountID, "migration-project:"+uuid.NewString())
	t := tags.New("dms.migration-project." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	mp := &MigrationProject{
		MigrationProjectName:       name,
		MigrationProjectArn:        projectARN,
		MigrationProjectIdentifier: name,
		Description:                description,
		AccountID:                  b.accountID,
		Region:                     region,
		Tags:                       t,
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

// ModifyMigrationProject updates the description of an existing migration project.
func (b *InMemoryBackend) ModifyMigrationProject(
	ctx context.Context,
	nameOrArn, description string,
) (*MigrationProject, error) {
	b.mu.Lock("ModifyMigrationProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if mp, ok := b.migrationProjects.Get(regionKey(region, nameOrArn)); ok {
		mp.Description = description
		cp := *mp

		return &cp, nil
	}

	if mp, ok := lookupUnique(b.migrationProjectsByARN, regionKey(region, nameOrArn)); ok {
		mp.Description = description
		cp := *mp

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: migration project %s not found", ErrNotFound, nameOrArn)
}
