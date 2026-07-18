package rds

import (
	"fmt"
	"slices"
)

// CreateCustomDBEngineVersion creates a custom DB engine version.
func (b *InMemoryBackend) CreateCustomDBEngineVersion(
	engine, engineVersion, description string,
) (*CustomDBEngineVersion, error) {
	if engine == "" {
		return nil, fmt.Errorf("%w: Engine is required", ErrInvalidParameter)
	}
	if engineVersion == "" {
		return nil, fmt.Errorf("%w: EngineVersion is required", ErrInvalidParameter)
	}

	key := engine + ":" + engineVersion
	b.mu.Lock("CreateCustomDBEngineVersion")
	defer b.mu.Unlock()

	if _, exists := b.customEngineVersions.Get(key); exists {
		return nil, fmt.Errorf(
			"%w: custom engine version %s/%s already exists",
			ErrInstanceAlreadyExists,
			engine,
			engineVersion,
		)
	}

	cev := &CustomDBEngineVersion{
		Engine:        engine,
		EngineVersion: engineVersion,
		Status:        instanceStatusAvailable,
		Description:   description,
	}
	b.customEngineVersions.Put(cev)
	cp := *cev

	return &cp, nil
}

// DeleteCustomDBEngineVersion deletes a custom DB engine version.
func (b *InMemoryBackend) DeleteCustomDBEngineVersion(engine, engineVersion string) (*CustomDBEngineVersion, error) {
	key := engine + ":" + engineVersion
	b.mu.Lock("DeleteCustomDBEngineVersion")
	defer b.mu.Unlock()

	cev, exists := b.customEngineVersions.Get(key)
	if !exists {
		return nil, fmt.Errorf("%w: custom engine version %s/%s not found", ErrInstanceNotFound, engine, engineVersion)
	}

	cp := *cev
	cp.Status = instanceStatusDeleting
	b.customEngineVersions.Delete(key)

	return &cp, nil
}

// ModifyCustomDBEngineVersion modifies a custom DB engine version.
func (b *InMemoryBackend) ModifyCustomDBEngineVersion(
	engine, engineVersion, description, status string,
) (*CustomDBEngineVersion, error) {
	key := engine + ":" + engineVersion
	b.mu.Lock("ModifyCustomDBEngineVersion")
	defer b.mu.Unlock()

	cev, exists := b.customEngineVersions.Get(key)
	if !exists {
		return nil, fmt.Errorf("%w: custom engine version %s/%s not found", ErrInstanceNotFound, engine, engineVersion)
	}

	if description != "" {
		cev.Description = description
	}
	if status != "" {
		cev.Status = status
	}

	cp := *cev

	return &cp, nil
}

// DescribeDBEngineVersions returns available engine versions, filtered by engine and/or version.
func (b *InMemoryBackend) DescribeDBEngineVersions(engine, engineVersion string) []DBEngineVersion {
	all := []DBEngineVersion{
		{Engine: enginePostgres, EngineVersion: "14.10", DBEngineDescription: "PostgreSQL 14.10"},
		{Engine: enginePostgres, EngineVersion: "15.5", DBEngineDescription: "PostgreSQL 15.5"},
		{Engine: engineMySQL, EngineVersion: "8.0.35", DBEngineDescription: "MySQL 8.0.35"},
		{Engine: engineMariaDB, EngineVersion: "10.6.14", DBEngineDescription: "MariaDB 10.6.14"},
		{Engine: engineAuroraMySQL, EngineVersion: "3.04.0", DBEngineDescription: "Aurora MySQL 3.04.0"},
		{Engine: engineAuroraPostgresql, EngineVersion: "14.9", DBEngineDescription: "Aurora PostgreSQL 14.9"},
		{Engine: engineAuroraPostgresql, EngineVersion: "15.4", DBEngineDescription: "Aurora PostgreSQL 15.4"},
	}
	if engine == "" && engineVersion == "" {
		return all
	}
	result := make([]DBEngineVersion, 0, len(all))
	for _, v := range all {
		if engine != "" && v.Engine != engine {
			continue
		}
		if engineVersion != "" && v.EngineVersion != engineVersion {
			continue
		}
		result = append(result, v)
	}

	return result
}

// DescribeOrderableDBInstanceOptions returns orderable instance options for the given engine.
func (b *InMemoryBackend) DescribeOrderableDBInstanceOptions(engine, engineVersion string) []OrderableDBInstanceOption {
	classes := []string{defaultInstanceClass, "db.t3.small", "db.t3.medium", "db.r5.large", "db.r5.xlarge"}
	if engine == "" {
		engine = "postgres"
	}
	versions := b.DescribeDBEngineVersions(engine, engineVersion)
	if len(versions) == 0 {
		versions = []DBEngineVersion{{Engine: engine, EngineVersion: engineVersion}}
	}
	result := make([]OrderableDBInstanceOption, 0, len(classes)*len(versions))
	for _, v := range versions {
		for _, class := range classes {
			result = append(result, OrderableDBInstanceOption{
				Engine:          v.Engine,
				EngineVersion:   v.EngineVersion,
				DBInstanceClass: class,
				MultiAZCapable:  true,
			})
		}
	}

	return result
}

const (
	engineLifecycleSupportOpenSource         = "open-source-rds-extended-support"
	engineLifecycleSupportOpenSourceDisabled = "open-source-rds-extended-support-disabled"

	storageTypeAuroraIOOptimized = "aurora-iopt1"
	storageTypeAurora            = "aurora"
	storageTypeIO1               = "io1"
	storageTypeGP2               = "gp2"
	storageTypeGP3               = "gp3"
)

// DescribeCustomDBEngineVersions returns all custom engine versions, filtered by engine
// and/or engineVersion if non-empty.
func (b *InMemoryBackend) DescribeCustomDBEngineVersions(engine, engineVersion string) []CustomDBEngineVersion {
	b.mu.RLock("DescribeCustomDBEngineVersions")
	defer b.mu.RUnlock()

	result := make([]CustomDBEngineVersion, 0, b.customEngineVersions.Len())

	for _, cev := range b.customEngineVersions.All() {
		if engine != "" && cev.Engine != engine {
			continue
		}

		if engineVersion != "" && cev.EngineVersion != engineVersion {
			continue
		}

		result = append(result, *cev)
	}

	slices.SortFunc(result, func(a, b CustomDBEngineVersion) int {
		ka := a.Engine + "/" + a.EngineVersion
		kb := b.Engine + "/" + b.EngineVersion
		if ka < kb {
			return -1
		}
		if ka > kb {
			return 1
		}

		return 0
	})

	return result
}

// ValidateEngineLifecycleSupport returns an error if the value is not a recognized
// EngineLifecycleSupport option.
func ValidateEngineLifecycleSupport(val string) error {
	switch val {
	case "", engineLifecycleSupportOpenSource, engineLifecycleSupportOpenSourceDisabled:
		return nil
	default:
		return fmt.Errorf(
			"%w: EngineLifecycleSupport must be %q or %q, got %q",
			ErrInvalidParameter,
			engineLifecycleSupportOpenSource,
			engineLifecycleSupportOpenSourceDisabled,
			val,
		)
	}
}
