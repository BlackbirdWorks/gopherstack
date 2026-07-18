package dms

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/google/uuid"
)

func isValidMigrationType(s string) bool {
	return s == "full-load" || s == "cdc" || s == "full-load-and-cdc"
}

// CreateDataMigration creates a new data migration.
func (b *InMemoryBackend) CreateDataMigration(
	ctx context.Context,
	name, migrationProjectArn, migrationType, serviceAccessRoleArn, selectionRules string,
	numberOfJobs int32,
	enableCloudwatchLogs bool,
	kv map[string]string,
) (*DataMigration, error) {
	b.mu.Lock("CreateDataMigration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.dataMigrations.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: data migration %s already exists", ErrAlreadyExists, name)
	}

	if !isValidMigrationType(migrationType) {
		return nil, fmt.Errorf(
			"%w: invalid DataMigrationType %q; valid: full-load, cdc, full-load-and-cdc",
			ErrValidation,
			migrationType,
		)
	}

	migrationARN := arn.Build("dms", region, b.accountID, "data-migration:"+uuid.NewString())
	t := tags.New("dms.data-migration." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	if numberOfJobs == 0 {
		numberOfJobs = 1
	}

	dm := &DataMigration{
		DataMigrationName:    name,
		DataMigrationArn:     migrationARN,
		MigrationProjectArn:  migrationProjectArn,
		DataMigrationType:    migrationType,
		ServiceAccessRoleArn: serviceAccessRoleArn,
		SelectionRules:       selectionRules,
		NumberOfJobs:         numberOfJobs,
		EnableCloudwatchLogs: enableCloudwatchLogs,
		DataMigrationStatus:  statusReady,
		AccountID:            b.accountID,
		Region:               region,
		CreationTime:         time.Now().UTC(),
		Tags:                 t,
	}
	b.dataMigrations.Put(dm)
	cp := *dm

	return &cp, nil
}

// AddDataMigrationInternal seeds a data migration directly without HTTP.
func (b *InMemoryBackend) AddDataMigrationInternal(name, migrationType string) {
	b.mu.Lock("AddDataMigrationInternal")
	defer b.mu.Unlock()
	migrationARN := arn.Build("dms", b.region, b.accountID, "data-migration:"+uuid.NewString())
	t := tags.New("dms.data-migration." + name + ".tags")
	dm := &DataMigration{
		DataMigrationName:   name,
		DataMigrationArn:    migrationARN,
		DataMigrationType:   migrationType,
		DataMigrationStatus: statusReady,
		NumberOfJobs:        1,
		AccountID:           b.accountID,
		Region:              b.region,
		CreationTime:        time.Now().UTC(),
		Tags:                t,
	}
	b.dataMigrations.Put(dm)
}

// DeleteDataMigration deletes a data migration by name or ARN.
func (b *InMemoryBackend) DeleteDataMigration(ctx context.Context, nameOrArn string) (*DataMigration, error) {
	b.mu.Lock("DeleteDataMigration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if dm, ok := b.dataMigrations.Get(regionKey(region, nameOrArn)); ok {
		cp := *dm
		dm.Tags.Close()
		b.dataMigrations.Delete(regionKey(region, nameOrArn))

		return &cp, nil
	}

	if dm, ok := lookupUnique(b.dataMigrationsByARN, regionKey(region, nameOrArn)); ok {
		cp := *dm
		dm.Tags.Close()
		b.dataMigrations.Delete(regionKey(region, dm.DataMigrationName))

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: data migration %s not found", ErrNotFound, nameOrArn)
}

// ModifyDataMigration updates a data migration.
func (b *InMemoryBackend) ModifyDataMigration(
	ctx context.Context,
	nameOrArn, migrationType, serviceAccessRoleArn string,
	numberOfJobs *int32,
) (*DataMigration, error) {
	b.mu.Lock("ModifyDataMigration")
	defer b.mu.Unlock()

	dm := b.findDataMigration(ctx, nameOrArn)
	if dm == nil {
		return nil, fmt.Errorf("%w: data migration %s not found", ErrNotFound, nameOrArn)
	}

	if migrationType != "" {
		dm.DataMigrationType = migrationType
	}

	if serviceAccessRoleArn != "" {
		dm.ServiceAccessRoleArn = serviceAccessRoleArn
	}

	if numberOfJobs != nil {
		dm.NumberOfJobs = *numberOfJobs
	}

	cp := *dm

	return &cp, nil
}

// findDataMigration locates a data migration by name or ARN within the request
// region (must hold a lock).
func (b *InMemoryBackend) findDataMigration(ctx context.Context, nameOrArn string) *DataMigration {
	region := getRegion(ctx, b.region)
	if dm, ok := b.dataMigrations.Get(regionKey(region, nameOrArn)); ok {
		return dm
	}

	if dm, ok := lookupUnique(b.dataMigrationsByARN, regionKey(region, nameOrArn)); ok {
		return dm
	}

	return nil
}

// StartDataMigration transitions a data migration to running status.
func (b *InMemoryBackend) StartDataMigration(ctx context.Context, nameOrArn string) (*DataMigration, error) {
	b.mu.Lock("StartDataMigration")
	defer b.mu.Unlock()

	dm := b.findDataMigration(ctx, nameOrArn)
	if dm == nil {
		return nil, fmt.Errorf("%w: data migration %s not found", ErrNotFound, nameOrArn)
	}

	dm.DataMigrationStatus = statusRunning
	cp := *dm

	return &cp, nil
}

// StopDataMigration transitions a data migration to stopped status.
func (b *InMemoryBackend) StopDataMigration(ctx context.Context, nameOrArn string) (*DataMigration, error) {
	b.mu.Lock("StopDataMigration")
	defer b.mu.Unlock()

	dm := b.findDataMigration(ctx, nameOrArn)
	if dm == nil {
		return nil, fmt.Errorf("%w: data migration %s not found", ErrNotFound, nameOrArn)
	}

	dm.DataMigrationStatus = statusStopped
	cp := *dm

	return &cp, nil
}

// DescribeDataMigrations returns all data migrations (optionally filtered by name/arn).
func (b *InMemoryBackend) DescribeDataMigrations(ctx context.Context, nameOrArn string) ([]*DataMigration, error) {
	b.mu.RLock("DescribeDataMigrations")
	defer b.mu.RUnlock()

	if nameOrArn != "" {
		dm := b.findDataMigration(ctx, nameOrArn)
		if dm == nil {
			return []*DataMigration{}, nil
		}

		cp := *dm

		return []*DataMigration{&cp}, nil
	}

	items := b.dataMigrationsByRegion.Get(getRegion(ctx, b.region))
	list := make([]*DataMigration, 0, len(items))
	for _, dm := range items {
		cp := *dm
		list = append(list, &cp)
	}

	return list, nil
}
