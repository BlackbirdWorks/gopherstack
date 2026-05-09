package rds

import (
	"fmt"
	"slices"
	"time"
)

const (
	reservedDurationOneYear       = 31536000
	reservedFixedPriceMicro       = 51.0
	reservedFixedPriceSmall       = 102.0
	reservedFixedPriceMedium      = 540.0
	reservedFixedPriceMediumMulti = 1080.0
	reservedFixedPriceLarge       = 810.0

	quotaMaxAllocatedStorage  = 100000
	quotaMaxDBInstances       = 40
	quotaMaxDBClusters        = 40
	quotaMaxDBParameterGroups = 50
	quotaMaxDBSubnetGroups    = 50
	quotaMaxDBSnapshots       = 100
	quotaMaxOptionGroups      = 20
	quotaMaxReservedInstances = 40

	instanceReadyDelaySeconds = 2
)

// CreateDBSecurityGroup creates a new DB security group.
func (b *InMemoryBackend) CreateDBSecurityGroup(name, description string) (*DBSecurityGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBSecurityGroup")
	defer b.mu.Unlock()
	if _, exists := b.dbSecurityGroups[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDBSecurityGroupAlreadyExists, name)
	}
	sg := &DBSecurityGroup{
		DBSecurityGroupName:        name,
		DBSecurityGroupDescription: description,
	}
	b.dbSecurityGroups[name] = sg
	cp := *sg

	return &cp, nil
}

// RemoveFromGlobalCluster removes a DB cluster from a global cluster.
func (b *InMemoryBackend) RemoveFromGlobalCluster(globalClusterID, dbClusterARN string) (*GlobalCluster, error) {
	b.mu.Lock("RemoveFromGlobalCluster")
	defer b.mu.Unlock()
	gc, ok := b.globalClusters[globalClusterID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrGlobalClusterNotFound, globalClusterID)
	}
	gc.ClusterARNs = slices.DeleteFunc(gc.ClusterARNs, func(arn string) bool {
		return arn == dbClusterARN
	})
	cp := *gc

	return &cp, nil
}

// FailoverGlobalCluster initiates a failover for a global cluster.
func (b *InMemoryBackend) FailoverGlobalCluster(
	globalClusterID, _ string,
) (*GlobalCluster, error) {
	b.mu.Lock("FailoverGlobalCluster")
	defer b.mu.Unlock()
	gc, ok := b.globalClusters[globalClusterID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrGlobalClusterNotFound, globalClusterID)
	}
	gc.Status = instanceStatusAvailable
	cp := *gc

	return &cp, nil
}

// SwitchoverGlobalCluster initiates a switchover for a global cluster.
func (b *InMemoryBackend) SwitchoverGlobalCluster(
	globalClusterID, _ string,
) (*GlobalCluster, error) {
	b.mu.Lock("SwitchoverGlobalCluster")
	defer b.mu.Unlock()
	gc, ok := b.globalClusters[globalClusterID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrGlobalClusterNotFound, globalClusterID)
	}
	gc.Status = instanceStatusAvailable
	cp := *gc

	return &cp, nil
}

// SwitchoverReadReplica converts a read replica to a standalone instance.
func (b *InMemoryBackend) SwitchoverReadReplica(instanceID string) (*DBInstance, error) {
	b.mu.Lock("SwitchoverReadReplica")
	defer b.mu.Unlock()
	inst, ok := b.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}
	inst.ReplicaSourceDBInstanceIdentifier = ""
	cp := *inst

	return &cp, nil
}

// PromoteReadReplicaDBCluster promotes a read replica DB cluster.
func (b *InMemoryBackend) PromoteReadReplicaDBCluster(clusterID string) (*DBCluster, error) {
	b.mu.Lock("PromoteReadReplicaDBCluster")
	defer b.mu.Unlock()
	cluster, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterID)
	}
	cluster.Status = instanceStatusAvailable
	cp := *cluster

	return &cp, nil
}

// DescribeAccountAttributes returns RDS account-level quota attributes.
func (b *InMemoryBackend) DescribeAccountAttributes() []AccountAttribute {
	b.mu.RLock("DescribeAccountAttributes")
	defer b.mu.RUnlock()

	return []AccountAttribute{
		{AttributeName: "AllocatedStorage", Used: 0, Max: quotaMaxAllocatedStorage},
		{AttributeName: "DBInstances", Used: len(b.instances), Max: quotaMaxDBInstances},
		{AttributeName: "DBClusters", Used: len(b.clusters), Max: quotaMaxDBClusters},
		{AttributeName: "DBParameterGroups", Used: len(b.parameterGroups), Max: quotaMaxDBParameterGroups},
		{AttributeName: "DBSubnetGroups", Used: len(b.subnetGroups), Max: quotaMaxDBSubnetGroups},
		{AttributeName: "DBSnapshots", Used: len(b.snapshots), Max: quotaMaxDBSnapshots},
		{AttributeName: "OptionGroups", Used: len(b.optionGroups), Max: quotaMaxOptionGroups},
		{AttributeName: "ReservedDBInstances", Used: len(b.reservedInstances), Max: quotaMaxReservedInstances},
	}
}

// DescribeCertificates returns RDS CA certificates, optionally filtered by ID.
func (b *InMemoryBackend) DescribeCertificates(certID string) ([]Certificate, error) {
	certs := staticCertificates()
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

// ModifyCertificates modifies the default certificate for the account.
func (b *InMemoryBackend) ModifyCertificates(certID string) (*Certificate, error) {
	certs := staticCertificates()
	for _, c := range certs {
		if c.CertificateIdentifier == certID {
			cp := c

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: certificate %s not found", ErrInvalidParameter, certID)
}

// DescribePendingMaintenanceActions returns pending maintenance actions.
func (b *InMemoryBackend) DescribePendingMaintenanceActions(_ string) []PendingMaintenanceAction {
	return []PendingMaintenanceAction{}
}

// DescribeSourceRegions returns available source regions for cross-region operations.
func (b *InMemoryBackend) DescribeSourceRegions(regionName string) []SourceRegion {
	regions := staticSourceRegions()
	if regionName == "" {
		return regions
	}
	result := make([]SourceRegion, 0)
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
	result := make([]DBMajorEngineVersion, 0)
	for _, v := range versions {
		if v.Engine == engine {
			result = append(result, v)
		}
	}

	return result
}

// DescribeEngineDefaultParameters returns default parameters for an engine family.
func (b *InMemoryBackend) DescribeEngineDefaultParameters(_ string) []DBParameter {
	return []DBParameter{}
}

// DescribeEngineDefaultClusterParameters returns default cluster parameters for an engine family.
func (b *InMemoryBackend) DescribeEngineDefaultClusterParameters(_ string) []DBParameter {
	return []DBParameter{}
}

// DescribeDBSnapshotAttributes returns attributes for a DB snapshot.
func (b *InMemoryBackend) DescribeDBSnapshotAttributes(snapshotID string) (*DBSnapshotAttributesResult, error) {
	b.mu.RLock("DescribeDBSnapshotAttributes")
	defer b.mu.RUnlock()
	if _, ok := b.snapshots[snapshotID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if attrs, ok := b.snapshotAttributes[snapshotID]; ok {
		cp := *attrs

		return &cp, nil
	}
	result := &DBSnapshotAttributesResult{
		DBSnapshotIdentifier: snapshotID,
		DBSnapshotAttributes: []DBSnapshotAttribute{},
	}
	cp := *result

	return &cp, nil
}

// ModifyDBSnapshot modifies settings of a DB snapshot.
func (b *InMemoryBackend) ModifyDBSnapshot(snapshotID, optionGroupName, engineVersion string) (*DBSnapshot, error) {
	b.mu.Lock("ModifyDBSnapshot")
	defer b.mu.Unlock()
	snap, ok := b.snapshots[snapshotID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if optionGroupName != "" {
		snap.OptionGroupName = optionGroupName
	}
	if engineVersion != "" {
		snap.EngineVersion = engineVersion
	}
	cp := *snap

	return &cp, nil
}

// ModifyDBSnapshotAttribute adds or removes attribute values for a DB snapshot.
func (b *InMemoryBackend) ModifyDBSnapshotAttribute(
	snapshotID, attributeName string,
	valuesToAdd, valuesToRemove []string,
) (*DBSnapshotAttributesResult, error) {
	b.mu.Lock("ModifyDBSnapshotAttribute")
	defer b.mu.Unlock()
	if _, ok := b.snapshots[snapshotID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if b.snapshotAttributes[snapshotID] == nil {
		b.snapshotAttributes[snapshotID] = &DBSnapshotAttributesResult{
			DBSnapshotIdentifier: snapshotID,
			DBSnapshotAttributes: []DBSnapshotAttribute{},
		}
	}
	result := b.snapshotAttributes[snapshotID]
	applySnapshotAttributeChange(&result.DBSnapshotAttributes, attributeName, valuesToAdd, valuesToRemove)
	cp := *result

	return &cp, nil
}

// DescribeDBClusterSnapshotAttributes returns attributes for a DB cluster snapshot.
func (b *InMemoryBackend) DescribeDBClusterSnapshotAttributes(
	snapshotID string,
) (*DBClusterSnapshotAttributesResult, error) {
	b.mu.RLock("DescribeDBClusterSnapshotAttributes")
	defer b.mu.RUnlock()
	if _, ok := b.clusterSnapshots[snapshotID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if attrs, ok := b.clusterSnapshotAttributes[snapshotID]; ok {
		cp := *attrs

		return &cp, nil
	}
	result := &DBClusterSnapshotAttributesResult{
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterSnapshotAttributes: []DBSnapshotAttribute{},
	}
	cp := *result

	return &cp, nil
}

// ModifyDBClusterSnapshotAttribute adds or removes attribute values for a cluster snapshot.
func (b *InMemoryBackend) ModifyDBClusterSnapshotAttribute(
	snapshotID, attributeName string,
	valuesToAdd, valuesToRemove []string,
) (*DBClusterSnapshotAttributesResult, error) {
	b.mu.Lock("ModifyDBClusterSnapshotAttribute")
	defer b.mu.Unlock()
	if _, ok := b.clusterSnapshots[snapshotID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if b.clusterSnapshotAttributes[snapshotID] == nil {
		b.clusterSnapshotAttributes[snapshotID] = &DBClusterSnapshotAttributesResult{
			DBClusterSnapshotIdentifier: snapshotID,
			DBClusterSnapshotAttributes: []DBSnapshotAttribute{},
		}
	}
	result := b.clusterSnapshotAttributes[snapshotID]
	applySnapshotAttributeChange(&result.DBClusterSnapshotAttributes, attributeName, valuesToAdd, valuesToRemove)
	cp := *result

	return &cp, nil
}

// applySnapshotAttributeChange modifies a list of snapshot attributes by adding and removing values.
func applySnapshotAttributeChange(
	attrs *[]DBSnapshotAttribute,
	attributeName string,
	valuesToAdd, valuesToRemove []string,
) {
	var attr *DBSnapshotAttribute
	for i := range *attrs {
		if (*attrs)[i].AttributeName == attributeName {
			attr = &(*attrs)[i]

			break
		}
	}
	if attr == nil {
		*attrs = append(*attrs, DBSnapshotAttribute{AttributeName: attributeName})
		attr = &(*attrs)[len(*attrs)-1]
	}
	for _, v := range valuesToAdd {
		if !slices.Contains(attr.AttributeValues, v) {
			attr.AttributeValues = append(attr.AttributeValues, v)
		}
	}
	attr.AttributeValues = slices.DeleteFunc(attr.AttributeValues, func(v string) bool {
		return slices.Contains(valuesToRemove, v)
	})
}

// DescribeDBClusterBacktracks returns backtracks for a DB cluster.
func (b *InMemoryBackend) DescribeDBClusterBacktracks(clusterID string) ([]DBClusterBacktrack, error) {
	b.mu.RLock("DescribeDBClusterBacktracks")
	defer b.mu.RUnlock()
	if _, ok := b.clusters[clusterID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterID)
	}

	return []DBClusterBacktrack{}, nil
}

// EnableHTTPEndpoint enables the HTTP endpoint for an Aurora Serverless cluster.
func (b *InMemoryBackend) EnableHTTPEndpoint(resourceARN string) error {
	b.mu.Lock("EnableHTTPEndpoint")
	defer b.mu.Unlock()
	for _, cluster := range b.clusters {
		if cluster.DBClusterIdentifier == resourceARN ||
			b.rdsARN("cluster", cluster.DBClusterIdentifier) == resourceARN {
			cluster.HTTPEndpointEnabled = true

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrClusterNotFound, resourceARN)
}

// DisableHTTPEndpoint disables the HTTP endpoint for an Aurora Serverless cluster.
func (b *InMemoryBackend) DisableHTTPEndpoint(resourceARN string) error {
	b.mu.Lock("DisableHTTPEndpoint")
	defer b.mu.Unlock()
	for _, cluster := range b.clusters {
		if cluster.DBClusterIdentifier == resourceARN ||
			b.rdsARN("cluster", cluster.DBClusterIdentifier) == resourceARN {
			cluster.HTTPEndpointEnabled = false

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrClusterNotFound, resourceARN)
}

// ModifyCurrentDBClusterCapacity modifies the serverless capacity of a DB cluster.
func (b *InMemoryBackend) ModifyCurrentDBClusterCapacity(clusterID string, capacity int) (*DBCluster, error) {
	b.mu.Lock("ModifyCurrentDBClusterCapacity")
	defer b.mu.Unlock()
	cluster, ok := b.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterID)
	}
	cluster.ServerlessCapacity = capacity
	cp := *cluster

	return &cp, nil
}

// RestoreDBInstanceFromS3 restores a DB instance from an S3 backup.
func (b *InMemoryBackend) RestoreDBInstanceFromS3(id, engine, dbInstanceClass, s3Bucket string) (*DBInstance, error) {
	if s3Bucket == "" {
		return nil, fmt.Errorf("%w: s3BucketName is required", ErrInvalidParameter)
	}
	if id == "" {
		return nil, fmt.Errorf("%w: dbInstanceIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("RestoreDBInstanceFromS3")
	defer b.mu.Unlock()
	if _, exists := b.instances[id]; exists {
		return nil, fmt.Errorf("%w: %s", ErrInstanceAlreadyExists, id)
	}
	inst := &DBInstance{
		DBInstanceIdentifier: id,
		DBInstanceClass:      dbInstanceClass,
		Engine:               engine,
		DBInstanceStatus:     "creating",
		AllocatedStorage:     defaultAllocatedStorage,
		StorageType:          "gp2",
	}
	b.instances[id] = inst
	b.instanceReadyAt[id] = time.Now().Add(instanceReadyDelaySeconds * time.Second)
	cp := *inst

	return &cp, nil
}

// RestoreDBClusterFromS3 restores a DB cluster from an S3 backup.
func (b *InMemoryBackend) RestoreDBClusterFromS3(id, engine, masterUsername, s3Bucket string) (*DBCluster, error) {
	if s3Bucket == "" {
		return nil, fmt.Errorf("%w: s3BucketName is required", ErrInvalidParameter)
	}
	if id == "" {
		return nil, fmt.Errorf("%w: dbClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("RestoreDBClusterFromS3")
	defer b.mu.Unlock()
	if _, exists := b.clusters[id]; exists {
		return nil, fmt.Errorf("%w: %s", ErrClusterAlreadyExists, id)
	}
	cluster := &DBCluster{
		DBClusterIdentifier: id,
		Engine:              engine,
		MasterUsername:      masterUsername,
		Status:              "creating",
	}
	b.clusters[id] = cluster
	cp := *cluster

	return &cp, nil
}

// ModifyDBRecommendation modifies the status of a DB recommendation.
func (b *InMemoryBackend) ModifyDBRecommendation(recID, status string) (*DBRecommendation, error) {
	b.mu.Lock("ModifyDBRecommendation")
	defer b.mu.Unlock()
	rec, ok := b.recommendations[recID]
	if !ok {
		return nil, fmt.Errorf("%w: recommendation %s not found", ErrInvalidParameter, recID)
	}
	rec.Status = status
	cp := *rec

	return &cp, nil
}

// DescribeDBRecommendations returns DB recommendations filtered by optional parameters.
func (b *InMemoryBackend) DescribeDBRecommendations(recID, status string) []DBRecommendation {
	b.mu.RLock("DescribeDBRecommendations")
	defer b.mu.RUnlock()
	result := make([]DBRecommendation, 0, len(b.recommendations))
	for _, rec := range b.recommendations {
		if recID != "" && rec.RecommendationID != recID {
			continue
		}
		if status != "" && rec.Status != status {
			continue
		}
		result = append(result, *rec)
	}

	return result
}

// PurchaseReservedDBInstancesOffering purchases a reserved DB instance offering.
func (b *InMemoryBackend) PurchaseReservedDBInstancesOffering(
	offeringID, reservedDBInstanceID string,
	dbInstanceCount int,
) (*ReservedDBInstance, error) {
	if dbInstanceCount < 1 {
		return nil, fmt.Errorf("%w: dbInstanceCount must be at least 1", ErrInvalidParameter)
	}
	var offering *ReservedDBInstancesOffering
	for _, o := range staticReservedOfferings() {
		if o.ReservedDBInstancesOfferingID == offeringID {
			cp := o
			offering = &cp

			break
		}
	}
	if offering == nil {
		offering = &ReservedDBInstancesOffering{
			ReservedDBInstancesOfferingID: offeringID,
			DBInstanceClass:               defaultInstanceClass,
			Duration:                      reservedDurationOneYear,
			FixedPrice:                    0,
			UsagePrice:                    0,
			ProductDescription:            engineMySQL,
			OfferingType:                  "No Upfront",
			MultiAZ:                       false,
			CurrencyCode:                  currencyUSD,
		}
	}
	if reservedDBInstanceID == "" {
		reservedDBInstanceID = fmt.Sprintf("ri-%s-%d", offeringID, time.Now().UnixNano())
	}
	b.mu.Lock("PurchaseReservedDBInstancesOffering")
	defer b.mu.Unlock()
	if _, exists := b.reservedInstances[reservedDBInstanceID]; exists {
		return nil, fmt.Errorf("%w: reserved instance %s already exists", ErrInvalidParameter, reservedDBInstanceID)
	}
	ri := &ReservedDBInstance{
		ReservedDBInstanceID:          reservedDBInstanceID,
		ReservedDBInstancesOfferingID: offeringID,
		DBInstanceClass:               offering.DBInstanceClass,
		StartTime:                     time.Now().UTC().Format(time.RFC3339),
		Duration:                      offering.Duration,
		FixedPrice:                    offering.FixedPrice,
		UsagePrice:                    offering.UsagePrice,
		DBInstanceCount:               dbInstanceCount,
		ProductDescription:            offering.ProductDescription,
		OfferingType:                  offering.OfferingType,
		MultiAZ:                       offering.MultiAZ,
		State:                         subscriptionStatusActive,
		CurrencyCode:                  offering.CurrencyCode,
	}
	b.reservedInstances[reservedDBInstanceID] = ri
	cp := *ri

	return &cp, nil
}

// DescribeReservedDBInstances returns reserved DB instances.
func (b *InMemoryBackend) DescribeReservedDBInstances(
	reservedDBInstanceID, dbInstanceClass string,
) []ReservedDBInstance {
	b.mu.RLock("DescribeReservedDBInstances")
	defer b.mu.RUnlock()
	result := make([]ReservedDBInstance, 0, len(b.reservedInstances))
	for _, ri := range b.reservedInstances {
		if reservedDBInstanceID != "" && ri.ReservedDBInstanceID != reservedDBInstanceID {
			continue
		}
		if dbInstanceClass != "" && ri.DBInstanceClass != dbInstanceClass {
			continue
		}
		result = append(result, *ri)
	}

	return result
}

// DescribeReservedDBInstancesOfferings returns available reserved DB instance offerings.
func (b *InMemoryBackend) DescribeReservedDBInstancesOfferings(
	offeringID, dbInstanceClass string,
) []ReservedDBInstancesOffering {
	offerings := staticReservedOfferings()
	if offeringID == "" && dbInstanceClass == "" {
		return offerings
	}
	result := make([]ReservedDBInstancesOffering, 0)
	for _, o := range offerings {
		if offeringID != "" && o.ReservedDBInstancesOfferingID != offeringID {
			continue
		}
		if dbInstanceClass != "" && o.DBInstanceClass != dbInstanceClass {
			continue
		}
		result = append(result, o)
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

func staticReservedOfferings() []ReservedDBInstancesOffering {
	return []ReservedDBInstancesOffering{
		{
			ReservedDBInstancesOfferingID: "01f5e8a3-2f47-4f47-8a7f-1234567890ab",
			DBInstanceClass:               defaultInstanceClass,
			Duration:                      reservedDurationOneYear,
			FixedPrice:                    reservedFixedPriceMicro,
			UsagePrice:                    0.0,
			ProductDescription:            engineMySQL,
			OfferingType:                  reservedAllUpfront,
			MultiAZ:                       false,
			CurrencyCode:                  currencyUSD,
		},
		{
			ReservedDBInstancesOfferingID: "12a1e534-e8a3-4f47-8a7f-2345678901bc",
			DBInstanceClass:               "db.t3.small",
			Duration:                      reservedDurationOneYear,
			FixedPrice:                    reservedFixedPriceSmall,
			UsagePrice:                    0.0,
			ProductDescription:            engineMySQL,
			OfferingType:                  reservedAllUpfront,
			MultiAZ:                       false,
			CurrencyCode:                  currencyUSD,
		},
		{
			ReservedDBInstancesOfferingID: "23b2f645-f9b4-4f47-8a7f-3456789012cd",
			DBInstanceClass:               "db.m5.large",
			Duration:                      reservedDurationOneYear,
			FixedPrice:                    reservedFixedPriceMedium,
			UsagePrice:                    0.0,
			ProductDescription:            enginePostgres,
			OfferingType:                  reservedAllUpfront,
			MultiAZ:                       false,
			CurrencyCode:                  currencyUSD,
		},
		{
			ReservedDBInstancesOfferingID: "34c3g756-g0c5-4f47-8a7f-4567890123de",
			DBInstanceClass:               "db.m5.large",
			Duration:                      reservedDurationOneYear,
			FixedPrice:                    reservedFixedPriceMediumMulti,
			UsagePrice:                    0.0,
			ProductDescription:            enginePostgres,
			OfferingType:                  reservedAllUpfront,
			MultiAZ:                       true,
			CurrencyCode:                  currencyUSD,
		},
		{
			ReservedDBInstancesOfferingID: "45d4h867-h1d6-4f47-8a7f-5678901234ef",
			DBInstanceClass:               "db.r5.large",
			Duration:                      reservedDurationOneYear,
			FixedPrice:                    reservedFixedPriceLarge,
			UsagePrice:                    0.0,
			ProductDescription:            engineAuroraMySQL,
			OfferingType:                  reservedAllUpfront,
			MultiAZ:                       false,
			CurrencyCode:                  currencyUSD,
		},
	}
}
