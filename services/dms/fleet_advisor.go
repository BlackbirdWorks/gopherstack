package dms

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateFleetAdvisorCollector creates a new Fleet Advisor collector.
func (b *InMemoryBackend) CreateFleetAdvisorCollector(
	ctx context.Context,
	collectorName, description, serviceAccessRoleArn, s3BucketName string,
) (*FleetAdvisorCollector, error) {
	b.mu.Lock("CreateFleetAdvisorCollector")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.fleetAdvisorCollectors.Has(regionKey(region, collectorName)) {
		return nil, fmt.Errorf(
			"%w: Fleet Advisor collector %s already exists",
			ErrAlreadyExists,
			collectorName,
		)
	}

	collectorID := uuid.NewString()
	t := tags.New("dms.fleet-advisor-collector." + collectorName + ".tags")
	col := &FleetAdvisorCollector{
		CollectorName:         collectorName,
		CollectorReferencedID: collectorID,
		CollectorVersion:      "1.0.0",
		Description:           description,
		ServiceAccessRoleArn:  serviceAccessRoleArn,
		S3BucketName:          s3BucketName,
		CollectorHealthCheck:  "HEALTHY",
		AccountID:             b.accountID,
		Region:                region,
		CreatedDate:           time.Now().UTC(),
		Tags:                  t,
	}
	b.fleetAdvisorCollectors.Put(col)

	// Seed two discovered databases per collector to emulate Fleet Advisor discovery.
	for _, seed := range []struct{ name, engine, ip string }{
		{collectorName + "-mysql-db", engineNameMySQL, "10.0.1.10"},
		{collectorName + "-pg-db", "postgresql", "10.0.1.11"},
	} {
		dbID := uuid.NewString()
		b.fleetAdvisorDatabases.Put(&FleetAdvisorDatabase{
			DatabaseID:            dbID,
			DatabaseName:          seed.name,
			IPAddress:             seed.ip,
			EngineName:            seed.engine,
			CollectorReferencedID: collectorID,
			Region:                region,
		})
	}

	cp := *col

	return &cp, nil
}

// DescribeFleetAdvisorDatabases returns databases discovered by Fleet Advisor collectors.
func (b *InMemoryBackend) DescribeFleetAdvisorDatabases(ctx context.Context) ([]*FleetAdvisorDatabase, error) {
	b.mu.RLock("DescribeFleetAdvisorDatabases")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	items := b.fleetAdvisorDatabasesByRegion.Get(region)
	result := make([]*FleetAdvisorDatabase, 0, len(items))

	for _, db := range items {
		cp := *db
		result = append(result, &cp)
	}

	return result, nil
}

// DeleteFleetAdvisorDatabases removes Fleet Advisor databases by ID and returns the deleted IDs.
func (b *InMemoryBackend) DeleteFleetAdvisorDatabases(ctx context.Context, ids []string) ([]string, error) {
	b.mu.Lock("DeleteFleetAdvisorDatabases")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	deleted := make([]string, 0, len(ids))

	for _, id := range ids {
		if b.fleetAdvisorDatabases.Delete(regionKey(region, id)) {
			deleted = append(deleted, id)
		}
	}

	return deleted, nil
}

// AddFleetAdvisorCollectorInternal seeds a Fleet Advisor collector directly without HTTP.
func (b *InMemoryBackend) AddFleetAdvisorCollectorInternal(name string) {
	b.mu.Lock("AddFleetAdvisorCollectorInternal")
	defer b.mu.Unlock()
	collectorID := uuid.NewString()
	t := tags.New("dms.fleet-advisor-collector." + name + ".tags")
	col := &FleetAdvisorCollector{
		CollectorName:         name,
		CollectorReferencedID: collectorID,
		CollectorVersion:      "1.0.0",
		CollectorHealthCheck:  "HEALTHY",
		AccountID:             b.accountID,
		Region:                b.region,
		CreatedDate:           time.Now().UTC(),
		Tags:                  t,
	}
	b.fleetAdvisorCollectors.Put(col)
}

// DeleteFleetAdvisorCollector deletes a fleet advisor collector by name or ID.
func (b *InMemoryBackend) DeleteFleetAdvisorCollector(ctx context.Context, nameOrID string) error {
	b.mu.Lock("DeleteFleetAdvisorCollector")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if col, ok := b.fleetAdvisorCollectors.Get(regionKey(region, nameOrID)); ok {
		col.Tags.Close()
		b.fleetAdvisorCollectors.Delete(regionKey(region, nameOrID))

		return nil
	}

	if col, ok := lookupUnique(b.fleetAdvisorCollectorsByID, regionKey(region, nameOrID)); ok {
		col.Tags.Close()
		b.fleetAdvisorCollectors.Delete(regionKey(region, col.CollectorName))

		return nil
	}

	return fmt.Errorf("%w: fleet advisor collector %s not found", ErrCollectorNotFound, nameOrID)
}

// DescribeFleetAdvisorCollectors returns all fleet advisor collectors.
func (b *InMemoryBackend) DescribeFleetAdvisorCollectors(ctx context.Context) ([]*FleetAdvisorCollector, error) {
	b.mu.RLock("DescribeFleetAdvisorCollectors")
	defer b.mu.RUnlock()

	items := b.fleetAdvisorCollectorsByRegion.Get(getRegion(ctx, b.region))
	list := make([]*FleetAdvisorCollector, 0, len(items))
	for _, col := range items {
		cp := *col
		list = append(list, &cp)
	}

	return list, nil
}
