package rds

import "fmt"

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
