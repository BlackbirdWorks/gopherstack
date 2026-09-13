package rds

import (
	"fmt"
	"net/url"
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
			ErrCustomDBEngineVersionAlreadyExists,
			engine,
			engineVersion,
		)
	}

	cev := &CustomDBEngineVersion{
		Engine:             engine,
		EngineVersion:      engineVersion,
		DBEngineVersionArn: b.rdsARN("cev", engine+"/"+engineVersion),
		Status:             instanceStatusAvailable,
		Description:        description,
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
		return nil, fmt.Errorf(
			"%w: custom engine version %s/%s not found",
			ErrCustomDBEngineVersionNotFound,
			engine,
			engineVersion,
		)
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
		return nil, fmt.Errorf(
			"%w: custom engine version %s/%s not found",
			ErrCustomDBEngineVersionNotFound,
			engine,
			engineVersion,
		)
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

// DescribeDBEngineVersions returns available engine versions, filtered by engine and/or
// version. This includes custom engine versions created via CreateCustomDBEngineVersion:
// real AWS has no separate "describe custom engine versions" operation on this client --
// custom engine versions (custom-oracle-ee, custom-oracle-se2, etc.) are returned by this
// same operation like any other engine/version pair, distinguished only by their Engine
// value.
func (b *InMemoryBackend) DescribeDBEngineVersions(engine, engineVersion string) []DBEngineVersion {
	builtin := []DBEngineVersion{
		{Engine: enginePostgres, EngineVersion: "14.10", DBEngineDescription: "PostgreSQL 14.10"},
		{Engine: enginePostgres, EngineVersion: "15.5", DBEngineDescription: "PostgreSQL 15.5"},
		{Engine: engineMySQL, EngineVersion: "8.0.35", DBEngineDescription: "MySQL 8.0.35"},
		{Engine: engineMariaDB, EngineVersion: "10.6.14", DBEngineDescription: "MariaDB 10.6.14"},
		{Engine: engineAuroraMySQL, EngineVersion: "3.04.0", DBEngineDescription: "Aurora MySQL 3.04.0"},
		{Engine: engineAuroraPostgresql, EngineVersion: "14.9", DBEngineDescription: "Aurora PostgreSQL 14.9"},
		{Engine: engineAuroraPostgresql, EngineVersion: "15.4", DBEngineDescription: "Aurora PostgreSQL 15.4"},
	}

	b.mu.RLock("DescribeDBEngineVersions")
	custom := b.customEngineVersions.All()
	all := make([]DBEngineVersion, 0, len(builtin)+len(custom))
	all = append(all, builtin...)

	for _, cev := range custom {
		all = append(all, DBEngineVersion{
			Engine:              cev.Engine,
			EngineVersion:       cev.EngineVersion,
			DBEngineDescription: cev.Description,
		})
	}
	b.mu.RUnlock()

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

// isKnownDBEngineVersionFilterName reports whether name is a
// Filters.Filter.N.Name value AWS recognizes for DescribeDBEngineVersions
// (rds@v1.124.1 api_op_DescribeDBEngineVersions.go:94-129).
// "db-parameter-group-family", "engine-mode", and "status" are accepted (to
// avoid rejecting an otherwise-valid client request) but DBEngineVersion
// carries none of those attributes, so they are not implemented as match
// predicates, matching the existing DescribeDBInstances "domain" precedent
// (db_instances.go).
func isKnownDBEngineVersionFilterName(name string) bool {
	switch name {
	case filterNameDBParameterGroupFamily, filterNameEngine, filterNameEngineMode,
		filterNameEngineVersion, filterNameStatus:
		return true
	default:
		return false
	}
}

// applyDBEngineVersionFilters narrows versions per the AWS
// DescribeDBEngineVersions Filters contract: each filter ANDs together
// (and with the top-level Engine/EngineVersion params, applied earlier by
// the caller), and a filter's Values list is OR-matched against the
// corresponding version field. An unrecognized filter name returns
// InvalidParameterValue, matching real AWS.
func applyDBEngineVersionFilters(vals url.Values, versions []DBEngineVersion) ([]DBEngineVersion, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return versions, nil
	}

	for name := range filters {
		if !isKnownDBEngineVersionFilterName(name) {
			return nil, fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	filtered := make([]DBEngineVersion, 0, len(versions))
	for _, v := range versions {
		if matchesAllDBEngineVersionFilters(v, filters) {
			filtered = append(filtered, v)
		}
	}

	return filtered, nil
}

func matchesAllDBEngineVersionFilters(v DBEngineVersion, filters map[string][]string) bool {
	for name, values := range filters {
		switch name {
		case filterNameEngine:
			if !slices.Contains(values, v.Engine) {
				return false
			}
		case filterNameEngineVersion:
			if !slices.Contains(values, v.EngineVersion) {
				return false
			}
		case filterNameDBParameterGroupFamily, filterNameEngineMode, filterNameStatus:
			// Not modeled; accept unconditionally.
		}
	}

	return true
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

// validDBInstanceEngines is the set of Engine values aws-sdk-go-v2's
// CreateDBInstanceInput documents as valid for CreateDBInstance, verified
// against aws-sdk-go-v2/service/rds@v1.116.2's api_op_CreateDBInstance.go
// "Valid Values" doc comment -- the ground truth for what a real RDS server
// accepts, since the Engine field itself is a plain *string on the wire (no
// SDK-side enum type exists to lean on).
//
//nolint:gochecknoglobals // static lookup table, same pattern as errCodeLookup elsewhere
var validDBInstanceEngines = map[string]bool{
	engineAuroraMySQL:       true,
	engineAuroraPostgresql:  true,
	"custom-oracle-ee":      true,
	"custom-oracle-ee-cdb":  true,
	"custom-oracle-se2":     true,
	"custom-oracle-se2-cdb": true,
	"custom-sqlserver-ee":   true,
	"custom-sqlserver-se":   true,
	"custom-sqlserver-web":  true,
	"custom-sqlserver-dev":  true,
	"db2-ae":                true,
	"db2-se":                true,
	engineMariaDB:           true,
	engineMySQL:             true,
	"oracle-ee":             true,
	"oracle-ee-cdb":         true,
	"oracle-se2":            true,
	"oracle-se2-cdb":        true,
	enginePostgres:          true,
	"sqlserver-dev-ee":      true,
	"sqlserver-ee":          true,
	"sqlserver-se":          true,
	"sqlserver-ex":          true,
	"sqlserver-web":         true,
}

// validDBClusterEngines is the set of Engine values aws-sdk-go-v2's
// CreateDBClusterInput documents as valid for CreateDBCluster (a narrower
// list than CreateDBInstance's -- clusters are only Aurora/Multi-AZ DB
// clusters/Neptune, never the single-instance-only engines like Oracle or
// SQL Server), verified against
// aws-sdk-go-v2/service/rds@v1.116.2's api_op_CreateDBCluster.go "Valid
// Values" doc comment.
//
//nolint:gochecknoglobals // static lookup table, same pattern as errCodeLookup elsewhere
var validDBClusterEngines = map[string]bool{
	engineAuroraMySQL:      true,
	engineAuroraPostgresql: true,
	engineMySQL:            true,
	enginePostgres:         true,
	"neptune":              true,
}

// validateDBInstanceEngine returns InvalidParameterValue (matching real
// AWS) if engine is non-empty and not one of validDBInstanceEngines. An
// empty engine is not rejected here: CreateDBInstance defaults it (see
// normalizeDBInstanceDefaults) before this validation is meaningfully
// exercised against a caller-supplied value.
func validateDBInstanceEngine(engine string) error {
	if engine == "" || validDBInstanceEngines[engine] {
		return nil
	}

	return fmt.Errorf("%w: invalid engine %q for CreateDBInstance", ErrInvalidParameter, engine)
}

// validateDBClusterEngine returns InvalidParameterValue (matching real AWS)
// if engine is non-empty and not one of validDBClusterEngines. An empty
// engine is not rejected here: CreateDBCluster defaults it to
// aurora-postgresql before this validation is meaningfully exercised
// against a caller-supplied value.
func validateDBClusterEngine(engine string) error {
	if engine == "" || validDBClusterEngines[engine] {
		return nil
	}

	return fmt.Errorf("%w: invalid engine %q for CreateDBCluster", ErrInvalidParameter, engine)
}
