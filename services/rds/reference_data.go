package rds

import "fmt"

// serverlessV2StatusDisabled is the Status value ServerlessV2PlatformVersionInfo uses
// for a platform version that is no longer in use (excluded unless IncludeAll is set).
// Verified against api_op_DescribeServerlessV2PlatformVersions.go's Status doc comment
// ("disabled - The platform version is not in use"; the other valid value, "enabled",
// has no corresponding constant here since nothing in this file currently needs to
// compare against it).
const serverlessV2StatusDisabled = "disabled"

// DescribeAccountAttributes returns RDS account-level quota attributes.
func (b *InMemoryBackend) DescribeAccountAttributes() []AccountAttribute {
	b.mu.RLock("DescribeAccountAttributes")
	defer b.mu.RUnlock()

	return []AccountAttribute{
		{AttributeName: "AllocatedStorage", Used: 0, Max: quotaMaxAllocatedStorage},
		{AttributeName: "DBInstances", Used: b.instances.Len(), Max: quotaMaxDBInstances},
		{AttributeName: "DBClusters", Used: b.clusters.Len(), Max: quotaMaxDBClusters},
		{AttributeName: "DBParameterGroups", Used: b.parameterGroups.Len(), Max: quotaMaxDBParameterGroups},
		{AttributeName: "DBSubnetGroups", Used: b.subnetGroups.Len(), Max: quotaMaxDBSubnetGroups},
		{AttributeName: "DBSnapshots", Used: b.snapshots.Len(), Max: quotaMaxDBSnapshots},
		{AttributeName: "OptionGroups", Used: b.optionGroups.Len(), Max: quotaMaxOptionGroups},
		{AttributeName: "ReservedDBInstances", Used: b.reservedInstances.Len(), Max: quotaMaxReservedInstances},
	}
}

// DescribeCertificates returns RDS CA certificates, optionally filtered by ID. The
// certificate currently set as the account default (via ModifyCertificates) is
// reported with CustomerOverride=true.
func (b *InMemoryBackend) DescribeCertificates(certID string) ([]Certificate, error) {
	b.mu.RLock("DescribeCertificates")
	defaultID := b.defaultCACertificateID
	b.mu.RUnlock()

	certs := staticCertificates()
	for i := range certs {
		if certs[i].CertificateIdentifier == defaultID {
			certs[i].CustomerOverride = true
		}
	}
	if certID == "" {
		return certs, nil
	}
	for _, c := range certs {
		if c.CertificateIdentifier == certID {
			return []Certificate{c}, nil
		}
	}

	return nil, fmt.Errorf("%w: certificate %s not found", ErrInvalidParameter, certID)
}

// ModifyCertificates sets (or, when certID is empty, resets) the default CA
// certificate identifier for the account and returns the resulting default.
func (b *InMemoryBackend) ModifyCertificates(certID string) (*Certificate, error) {
	certs := staticCertificates()

	// An empty identifier resets to the system default.
	if certID == "" {
		certID = defaultCACertificateID
	}

	for _, c := range certs {
		if c.CertificateIdentifier == certID {
			b.mu.Lock("ModifyCertificates")
			b.defaultCACertificateID = certID
			b.mu.Unlock()
			cp := c
			cp.CustomerOverride = true

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: certificate %s not found", ErrInvalidParameter, certID)
}

// DescribeSourceRegions returns available source regions for cross-region operations.
func (b *InMemoryBackend) DescribeSourceRegions(regionName string) []SourceRegion {
	regions := staticSourceRegions()
	if regionName == "" {
		return regions
	}
	result := make([]SourceRegion, 0, len(regions))
	for _, r := range regions {
		if r.RegionName == regionName {
			result = append(result, r)
		}
	}

	return result
}

// DescribeDBMajorEngineVersions returns available major engine versions.
func (b *InMemoryBackend) DescribeDBMajorEngineVersions(engine string) []DBMajorEngineVersion {
	versions := staticMajorEngineVersions()
	if engine == "" {
		return versions
	}
	result := make([]DBMajorEngineVersion, 0, len(versions))
	for _, v := range versions {
		if v.Engine == engine {
			result = append(result, v)
		}
	}

	return result
}

// validServerlessV2Engines is the set of Engine values
// DescribeServerlessV2PlatformVersionsInput documents as valid, verified against
// aws-sdk-go-v2/service/rds@v1.124.1's api_op_DescribeServerlessV2PlatformVersions.go
// "Valid Values" doc comment on the Engine field (a plain *string on the wire, no
// SDK-side enum type).
//
//nolint:gochecknoglobals // static lookup table, same pattern as validDBInstanceEngines
var validServerlessV2Engines = map[string]bool{
	engineAuroraMySQL:      true,
	engineAuroraPostgresql: true,
}

// DescribeServerlessV2PlatformVersions returns Aurora Serverless v2 platform versions,
// filtered by engine, a specific version, "default only", and "include all" (disabled
// versions).
//
// This backend's catalog is deliberately empty: aws-sdk-go-v2/service/rds/types's
// ServerlessV2PlatformVersionInfo.ServerlessV2PlatformVersion is a plain *string on the
// wire (confirmed via `go doc`), and the only enumerable values documented anywhere in
// the installed SDK module are the two valid Engine values checked above -- there is no
// SDK-side list of real platform version numbers/descriptions to derive from. Inventing
// specific version strings (e.g. "3", "4") would fabricate data indistinguishable from
// genuine AWS output with nothing in this SDK module to field-diff them against, so an
// honestly empty catalog is returned instead (see PARITY.md). The filtering logic below
// is real and ready to activate correctly the moment genuine entries are ever added to
// staticServerlessV2PlatformVersions -- it is not dead code, just currently applied to
// zero rows.
func (b *InMemoryBackend) DescribeServerlessV2PlatformVersions(
	engine, version string, defaultOnly, includeAll bool,
) ([]ServerlessV2PlatformVersionInfo, error) {
	if engine != "" && !validServerlessV2Engines[engine] {
		return nil, fmt.Errorf(
			"%w: Engine must be %q or %q, got %q",
			ErrInvalidParameter, engineAuroraMySQL, engineAuroraPostgresql, engine,
		)
	}

	all := staticServerlessV2PlatformVersions()
	result := make([]ServerlessV2PlatformVersionInfo, 0, len(all))

	for _, v := range all {
		if engine != "" && v.Engine != engine {
			continue
		}

		if version != "" && v.ServerlessV2PlatformVersion != version {
			continue
		}

		if defaultOnly && !v.IsDefault {
			continue
		}

		if !includeAll && v.Status == serverlessV2StatusDisabled {
			continue
		}

		result = append(result, v)
	}

	return result, nil
}

// staticServerlessV2PlatformVersions returns this mock's Aurora Serverless v2 platform
// version catalog -- currently empty, see DescribeServerlessV2PlatformVersions's doc
// comment for why.
func staticServerlessV2PlatformVersions() []ServerlessV2PlatformVersionInfo {
	return []ServerlessV2PlatformVersionInfo{}
}

func staticCertificates() []Certificate {
	return []Certificate{
		{
			CertificateIdentifier: "rds-ca-2019",
			CertificateType:       "CA",
			ValidFrom:             "2019-09-19T17:10:00Z",
			ValidTill:             "2024-08-22T17:08:50Z",
			CustomerOverride:      false,
			Thumbprint:            "",
		},
		{
			CertificateIdentifier: "rds-ca-rsa2048-g1",
			CertificateType:       "CA",
			ValidFrom:             reservedValidFrom,
			ValidTill:             "2061-05-25T00:00:00Z",
			CustomerOverride:      false,
			Thumbprint:            "",
		},
		{
			CertificateIdentifier: "rds-ca-rsa4096-g1",
			CertificateType:       "CA",
			ValidFrom:             reservedValidFrom,
			ValidTill:             "2121-05-25T00:00:00Z",
			CustomerOverride:      false,
			Thumbprint:            "",
		},
		{
			CertificateIdentifier: "rds-ca-ecc384-g1",
			CertificateType:       "CA",
			ValidFrom:             reservedValidFrom,
			ValidTill:             "2121-05-25T00:00:00Z",
			CustomerOverride:      false,
			Thumbprint:            "",
		},
	}
}

func staticSourceRegions() []SourceRegion {
	return []SourceRegion{
		{RegionName: "us-east-1", Endpoint: "rds.us-east-1.amazonaws.com", Status: instanceStatusAvailable},
		{RegionName: "us-east-2", Endpoint: "rds.us-east-2.amazonaws.com", Status: instanceStatusAvailable},
		{RegionName: "us-west-1", Endpoint: "rds.us-west-1.amazonaws.com", Status: instanceStatusAvailable},
		{RegionName: "us-west-2", Endpoint: "rds.us-west-2.amazonaws.com", Status: instanceStatusAvailable},
		{RegionName: "eu-west-1", Endpoint: "rds.eu-west-1.amazonaws.com", Status: instanceStatusAvailable},
		{RegionName: "eu-west-2", Endpoint: "rds.eu-west-2.amazonaws.com", Status: instanceStatusAvailable},
		{RegionName: "eu-central-1", Endpoint: "rds.eu-central-1.amazonaws.com", Status: instanceStatusAvailable},
		{RegionName: "ap-southeast-1", Endpoint: "rds.ap-southeast-1.amazonaws.com", Status: instanceStatusAvailable},
		{RegionName: "ap-southeast-2", Endpoint: "rds.ap-southeast-2.amazonaws.com", Status: instanceStatusAvailable},
		{RegionName: "ap-northeast-1", Endpoint: "rds.ap-northeast-1.amazonaws.com", Status: instanceStatusAvailable},
		{RegionName: "sa-east-1", Endpoint: "rds.sa-east-1.amazonaws.com", Status: instanceStatusAvailable},
		{RegionName: "ca-central-1", Endpoint: "rds.ca-central-1.amazonaws.com", Status: instanceStatusAvailable},
	}
}

func staticMajorEngineVersions() []DBMajorEngineVersion {
	return []DBMajorEngineVersion{
		{Engine: engineMySQL, MajorEngineVersion: "8.0", Status: instanceStatusAvailable},
		{Engine: engineMySQL, MajorEngineVersion: "5.7", Status: instanceStatusAvailable},
		{Engine: enginePostgres, MajorEngineVersion: "15", Status: instanceStatusAvailable},
		{Engine: enginePostgres, MajorEngineVersion: "14", Status: instanceStatusAvailable},
		{Engine: enginePostgres, MajorEngineVersion: "13", Status: instanceStatusAvailable},
		{Engine: engineMariaDB, MajorEngineVersion: "10.6", Status: instanceStatusAvailable},
		{Engine: "oracle-ee", MajorEngineVersion: "19", Status: instanceStatusAvailable},
		{Engine: "sqlserver-ee", MajorEngineVersion: "15.00", Status: instanceStatusAvailable},
		{Engine: engineAuroraMySQL, MajorEngineVersion: "8.0", Status: instanceStatusAvailable},
		{Engine: engineAuroraPostgresql, MajorEngineVersion: "15", Status: instanceStatusAvailable},
		{Engine: engineAuroraPostgresql, MajorEngineVersion: "14", Status: instanceStatusAvailable},
	}
}

const (
	quotaMaxAllocatedStorage  = 100000
	quotaMaxDBInstances       = 40
	quotaMaxDBClusters        = 40
	quotaMaxDBParameterGroups = 50
	quotaMaxDBSubnetGroups    = 50
	quotaMaxDBSnapshots       = 100
	quotaMaxOptionGroups      = 20
	quotaMaxReservedInstances = 40
)
