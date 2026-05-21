package neptune

import (
	"errors"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	pgFamilyDefaultNeptune13 = "default.neptune1.3"
	snapshotSourceManual     = "manual"
	engineModeProvisioned    = "provisioned"
	engineModeServerless     = "serverless"
)

var (
	ErrClusterNotFound                    = errors.New("DBClusterNotFound")
	ErrClusterAlreadyExists               = errors.New("DBClusterAlreadyExists")
	ErrInstanceNotFound                   = errors.New("DBInstanceNotFound")
	ErrInstanceAlreadyExists              = errors.New("DBInstanceAlreadyExists")
	ErrSubnetGroupNotFound                = errors.New("DBSubnetGroupNotFound")
	ErrSubnetGroupAlreadyExists           = errors.New("DBSubnetGroupAlreadyExists")
	ErrClusterParameterGroupNotFound      = errors.New("DBClusterParameterGroupNotFound")
	ErrClusterParameterGroupAlreadyExists = errors.New("DBClusterParameterGroupAlreadyExists")
	ErrClusterSnapshotNotFound            = errors.New("DBClusterSnapshotNotFound")
	ErrClusterSnapshotAlreadyExists       = errors.New("DBClusterSnapshotAlreadyExists")
	ErrParameterGroupNotFound             = errors.New("DBParameterGroupNotFound")
	ErrParameterGroupAlreadyExists        = errors.New("DBParameterGroupAlreadyExists")
	ErrClusterEndpointNotFound            = errors.New("DBClusterEndpointNotFound")
	ErrClusterEndpointAlreadyExists       = errors.New("DBClusterEndpointAlreadyExists")
	ErrSubscriptionNotFound               = errors.New("SubscriptionNotFound")
	ErrSubscriptionAlreadyExists          = errors.New("SubscriptionAlreadyExists")
	ErrGlobalClusterNotFound              = errors.New("GlobalClusterNotFound")
	ErrGlobalClusterAlreadyExists         = errors.New("GlobalClusterAlreadyExists")
	ErrInvalidParameter                   = errors.New("InvalidParameterValue")
	ErrUnknownAction                      = errors.New("InvalidAction")
)

const (
	defaultNeptunePort           = 8182
	defaultInstanceClass         = "db.r5.large"
	neptuneEngine                = "neptune"
	defaultEngineVersion         = "1.3.0.0"
	defaultBackupRetentionPeriod = 1
	clusterStatusAvailable       = "available"
	clusterStatusStopped         = "stopped"
	subscriptionStatusActive     = "active"
	endpointTypeReader           = "READER"
	endpointTypeWriter           = "WRITER"
	endpointTypeCustom           = "CUSTOM"
	endpointTypeAny              = "ANY"
	defaultMaintenanceWindow     = "sun:05:00-sun:06:00"
)

// ServerlessV2ScalingConfiguration holds Neptune Serverless v2 capacity settings.
type ServerlessV2ScalingConfiguration struct {
	MinCapacity float64 `json:"minCapacity"`
	MaxCapacity float64 `json:"maxCapacity"`
}

// MasterUserManagedSecret holds the ARN of the Secrets Manager secret for the master user password.
type MasterUserManagedSecret struct {
	SecretARN string `json:"secretArn"`
	SecretStatus string `json:"secretStatus"`
}

// DBClusterCreateOptions holds optional fields for CreateDBCluster.
type DBClusterCreateOptions struct {
	ServerlessV2ScalingConfig      *ServerlessV2ScalingConfiguration
	EngineVersion                  string
	EngineMode                     string
	KmsKeyID                       string
	PreferredBackupWindow          string
	PreferredMaintenanceWindow     string
	EnableIAMDatabaseAuthentication bool
	ManageMasterUserPassword       bool
	StorageEncrypted               bool
	DeletionProtection             bool
}

// DBClusterModifyOptions holds optional fields for ModifyDBCluster.
type DBClusterModifyOptions struct {
	ServerlessV2ScalingConfig         *ServerlessV2ScalingConfiguration
	EngineVersion                     string
	PreferredBackupWindow             string
	PreferredMaintenanceWindow        string
	EnableIAMDatabaseAuthentication   bool
	IamAuthSet                        bool
	ManageMasterUserPassword          bool
	DeletionProtection                bool
	DeletionProtectionSet             bool
}

// DBClusterMember represents a single DB instance member of a Neptune cluster.
type DBClusterMember struct {
	DBInstanceIdentifier string `json:"DBInstanceIdentifier"`
	IsClusterWriter      bool   `json:"IsClusterWriter"`
}

// DBCluster represents an Amazon Neptune DB cluster.
type DBCluster struct {
	ServerlessV2ScalingConfig       *ServerlessV2ScalingConfiguration `json:"ServerlessV2ScalingConfiguration,omitempty"`
	MasterUserManagedSecret         *MasterUserManagedSecret          `json:"MasterUserManagedSecret,omitempty"`
	DBClusterIdentifier             string                            `json:"DBClusterIdentifier"`
	DBClusterArn                    string                            `json:"DBClusterArn"`
	Engine                          string                            `json:"Engine"`
	EngineVersion                   string                            `json:"EngineVersion"`
	EngineMode                      string                            `json:"EngineMode"`
	Status                          string                            `json:"Status"`
	DBClusterParameterGroupName     string                            `json:"DBClusterParameterGroupName"`
	DBSubnetGroupName               string                            `json:"DBSubnetGroupName"`
	Endpoint                        string                            `json:"Endpoint"`
	ReaderEndpoint                  string                            `json:"ReaderEndpoint"`
	PreferredBackupWindow           string                            `json:"PreferredBackupWindow"`
	PreferredMaintenanceWindow      string                            `json:"PreferredMaintenanceWindow"`
	KmsKeyID                        string                            `json:"KmsKeyID"`
	DBClusterMembers                []DBClusterMember                 `json:"DBClusterMembers"`
	Port                            int                               `json:"Port"`
	BackupRetentionPeriod           int                               `json:"BackupRetentionPeriod"`
	EnableIAMDatabaseAuthentication bool                              `json:"EnableIAMDatabaseAuthentication"`
	StorageEncrypted                bool                              `json:"StorageEncrypted"`
	MultiAZ                         bool                              `json:"MultiAZ"`
	DeletionProtection              bool                              `json:"DeletionProtection"`
}

// DBInstance represents an Amazon Neptune DB instance.
type DBInstance struct {
	DBInstanceIdentifier            string `json:"DBInstanceIdentifier"`
	DBInstanceArn                   string `json:"DBInstanceArn"`
	DBClusterIdentifier             string `json:"DBClusterIdentifier"`
	DBInstanceClass                 string `json:"DBInstanceClass"`
	Engine                          string `json:"Engine"`
	EngineVersion                   string `json:"EngineVersion"`
	DBInstanceStatus                string `json:"DBInstanceStatus"`
	Endpoint                        string `json:"Endpoint"`
	DBParameterGroupName            string `json:"DBParameterGroupName"`
	PreferredMaintenanceWindow      string `json:"PreferredMaintenanceWindow"`
	PreferredBackupWindow           string `json:"PreferredBackupWindow"`
	AvailabilityZone                string `json:"AvailabilityZone"`
	Port                            int    `json:"Port"`
	PromotionTier                   int    `json:"PromotionTier"`
	StorageEncrypted                bool   `json:"StorageEncrypted"`
	AutoMinorVersionUpgrade         bool   `json:"AutoMinorVersionUpgrade"`
	CopyTagsToSnapshot              bool   `json:"CopyTagsToSnapshot"`
	EnableIAMDatabaseAuthentication bool   `json:"EnableIAMDatabaseAuthentication"`
}

// DBInstanceCreateOptions holds optional fields for CreateDBInstance.
type DBInstanceCreateOptions struct {
	DBParameterGroupName            string
	PreferredMaintenanceWindow      string
	PreferredBackupWindow           string
	AvailabilityZone                string
	AutoMinorVersionUpgrade         bool
	CopyTagsToSnapshot              bool
	EnableIAMDatabaseAuthentication bool
	PromotionTier                   int
	StorageEncrypted                bool
}

// DBInstanceModifyOptions holds optional fields for ModifyDBInstance.
type DBInstanceModifyOptions struct {
	DBParameterGroupName            string
	PreferredMaintenanceWindow      string
	PreferredBackupWindow           string
	AvailabilityZone                string
	AutoMinorVersionUpgrade         bool
	AutoMinorVersionUpgradeSet      bool
	CopyTagsToSnapshot              bool
	CopyTagsToSnapshotSet           bool
	EnableIAMDatabaseAuthentication bool
	IamAuthSet                      bool
	PromotionTier                   int
	PromotionTierSet                bool
}

// DBSubnetGroup represents a Neptune DB subnet group.
type DBSubnetGroup struct {
	DBSubnetGroupName        string   `json:"DBSubnetGroupName"`
	DBSubnetGroupDescription string   `json:"DBSubnetGroupDescription"`
	VpcID                    string   `json:"VpcID"`
	Status                   string   `json:"Status"`
	SubnetIDs                []string `json:"SubnetIDs"`
}

// Tag is a key-value pair tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// DBClusterParameterGroup represents a Neptune DB cluster parameter group.
type DBClusterParameterGroup struct {
	DBClusterParameterGroupName string `json:"DBClusterParameterGroupName"`
	DBParameterGroupFamily      string `json:"DBParameterGroupFamily"`
	Description                 string `json:"Description"`
}

// DBClusterSnapshot represents a Neptune DB cluster snapshot.
type DBClusterSnapshot struct {
	DBClusterSnapshotIdentifier string `json:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotArn        string `json:"DBClusterSnapshotArn"`
	DBClusterIdentifier         string `json:"DBClusterIdentifier"`
	Engine                      string `json:"Engine"`
	EngineVersion               string `json:"EngineVersion"`
	Status                      string `json:"Status"`
	SnapshotType                string `json:"SnapshotType"`
	StorageEncrypted            bool   `json:"StorageEncrypted"`
}

// DBParameterGroup represents a Neptune DB parameter group.
type DBParameterGroup struct {
	DBParameterGroupName   string `json:"DBParameterGroupName"`
	DBParameterGroupFamily string `json:"DBParameterGroupFamily"`
	Description            string `json:"Description"`
}

// DBClusterEndpoint represents a Neptune DB cluster custom endpoint.
type DBClusterEndpoint struct {
	DBClusterEndpointIdentifier string `json:"DBClusterEndpointIdentifier"`
	DBClusterIdentifier         string `json:"DBClusterIdentifier"`
	EndpointType                string `json:"EndpointType"`
	Status                      string `json:"Status"`
	Endpoint                    string `json:"Endpoint"`
}

// EventSubscription represents a Neptune event subscription.
type EventSubscription struct {
	CustSubscriptionID string   `json:"CustSubscriptionID"`
	SnsTopicARN        string   `json:"SnsTopicARN"`
	Status             string   `json:"Status"`
	SourceIDs          []string `json:"SourceIDs"`
}

// GlobalCluster represents a Neptune global cluster.
type GlobalCluster struct {
	GlobalClusterIdentifier string                `json:"GlobalClusterIdentifier"`
	Status                  string                `json:"Status"`
	GlobalClusterMembers    []GlobalClusterMember `json:"GlobalClusterMembers"`
}

// GlobalClusterMember represents a member cluster in a global cluster.
type GlobalClusterMember struct {
	DBClusterARN string `json:"DBClusterARN"`
	IsWriter     bool   `json:"IsWriter"`
}

// InMemoryBackend is a thread-safe in-memory backend for Neptune.
type InMemoryBackend struct {
	clusters               map[string]*DBCluster
	instances              map[string]*DBInstance
	subnetGroups           map[string]*DBSubnetGroup
	clusterParameterGroups map[string]*DBClusterParameterGroup
	clusterSnapshots       map[string]*DBClusterSnapshot
	parameterGroups        map[string]*DBParameterGroup
	clusterEndpoints       map[string]*DBClusterEndpoint
	eventSubscriptions     map[string]*EventSubscription
	globalClusters         map[string]*GlobalCluster
	clusterRoles           map[string][]string
	tags                   map[string][]Tag
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

// NewInMemoryBackend creates a new in-memory Neptune backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:               make(map[string]*DBCluster),
		instances:              make(map[string]*DBInstance),
		subnetGroups:           make(map[string]*DBSubnetGroup),
		clusterParameterGroups: make(map[string]*DBClusterParameterGroup),
		clusterSnapshots:       make(map[string]*DBClusterSnapshot),
		parameterGroups:        make(map[string]*DBParameterGroup),
		clusterEndpoints:       make(map[string]*DBClusterEndpoint),
		eventSubscriptions:     make(map[string]*EventSubscription),
		globalClusters:         make(map[string]*GlobalCluster),
		clusterRoles:           make(map[string][]string),
		tags:                   make(map[string][]Tag),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("neptune"),
	}
}

// Region returns the backend's AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// cloneCluster deep-copies a DBCluster to avoid shared slice/pointer mutation.
func cloneCluster(c *DBCluster) DBCluster {
	cp := *c
	cp.DBClusterMembers = make([]DBClusterMember, len(c.DBClusterMembers))
	copy(cp.DBClusterMembers, c.DBClusterMembers)
	if c.ServerlessV2ScalingConfig != nil {
		sv2 := *c.ServerlessV2ScalingConfig
		cp.ServerlessV2ScalingConfig = &sv2
	}
	if c.MasterUserManagedSecret != nil {
		ms := *c.MasterUserManagedSecret
		cp.MasterUserManagedSecret = &ms
	}

	return cp
}

// clusterARN returns the ARN for a Neptune DB cluster.
func (b *InMemoryBackend) clusterARN(id string) string {
	return arn.Build("neptune", b.region, b.accountID, "cluster:"+id)
}

// instanceARN returns the ARN for a Neptune DB instance.
func (b *InMemoryBackend) instanceARN(id string) string {
	return arn.Build("neptune", b.region, b.accountID, "db:"+id)
}

// subnetGroupARN returns the ARN for a Neptune DB subnet group.
func (b *InMemoryBackend) subnetGroupARN(name string) string {
	return arn.Build("rds", b.region, b.accountID, "subgrp:"+name)
}

// clusterParameterGroupARN returns the ARN for a Neptune DB cluster parameter group.
func (b *InMemoryBackend) clusterParameterGroupARN(name string) string {
	return arn.Build("rds", b.region, b.accountID, "cluster-pg:"+name)
}

// clusterSnapshotARN returns the ARN for a Neptune DB cluster snapshot.
func (b *InMemoryBackend) clusterSnapshotARN(id string) string {
	return arn.Build("rds", b.region, b.accountID, "cluster-snapshot:"+id)
}

// CreateDBCluster creates a new Neptune DB cluster.
func (b *InMemoryBackend) CreateDBCluster(id, paramGroupName string, port int, opts DBClusterCreateOptions) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBCluster")
	defer b.mu.Unlock()
	if _, exists := b.clusters[id]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}
	if paramGroupName == "" {
		paramGroupName = pgFamilyDefaultNeptune13
	}
	if port <= 0 {
		port = defaultNeptunePort
	}
	engineVersion := defaultEngineVersion
	if opts.EngineVersion != "" {
		engineVersion = opts.EngineVersion
	}
	engineMode := engineModeProvisioned
	if opts.EngineMode != "" {
		engineMode = opts.EngineMode
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.neptune.amazonaws.com", id, b.region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.%s.neptune.amazonaws.com", id, b.region)
	cluster := &DBCluster{
		DBClusterIdentifier:             id,
		DBClusterArn:                    b.clusterARN(id),
		Engine:                          neptuneEngine,
		EngineVersion:                   engineVersion,
		EngineMode:                      engineMode,
		Status:                          clusterStatusAvailable,
		DBClusterParameterGroupName:     paramGroupName,
		Endpoint:                        endpoint,
		ReaderEndpoint:                  readerEndpoint,
		Port:                            port,
		DBClusterMembers:                []DBClusterMember{},
		BackupRetentionPeriod:           defaultBackupRetentionPeriod,
		StorageEncrypted:                opts.StorageEncrypted,
		EnableIAMDatabaseAuthentication: opts.EnableIAMDatabaseAuthentication,
		DeletionProtection:              opts.DeletionProtection,
		PreferredBackupWindow:           opts.PreferredBackupWindow,
		PreferredMaintenanceWindow:      opts.PreferredMaintenanceWindow,
		KmsKeyID:                        opts.KmsKeyID,
		ServerlessV2ScalingConfig:       opts.ServerlessV2ScalingConfig,
	}
	if opts.ManageMasterUserPassword {
		cluster.MasterUserManagedSecret = &MasterUserManagedSecret{
			SecretARN:    fmt.Sprintf("arn:aws:secretsmanager:%s:%s:secret:rds!cluster-%s", b.region, b.accountID, id),
			SecretStatus: "active",
		}
	}
	b.clusters[id] = cluster
	cp := cloneCluster(cluster)

	return &cp, nil
}

// DescribeDBClusters returns all Neptune DB clusters or a specific one.
func (b *InMemoryBackend) DescribeDBClusters(id string) ([]DBCluster, error) {
	b.mu.RLock("DescribeDBClusters")
	defer b.mu.RUnlock()
	if id != "" {
		c, exists := b.clusters[id]
		if !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}

		return []DBCluster{cloneCluster(c)}, nil
	}
	result := make([]DBCluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		result = append(result, cloneCluster(c))
	}

	return result, nil
}

// DeleteDBCluster deletes a Neptune DB cluster and all associated DB instances.
func (b *InMemoryBackend) DeleteDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("DeleteDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cp := cloneCluster(c)
	delete(b.clusters, id)
	delete(b.tags, b.clusterARN(id))
	delete(b.clusterRoles, id)

	// Clean up all instances associated with this cluster.
	for instID, inst := range b.instances {
		if inst.DBClusterIdentifier == id {
			delete(b.instances, instID)
			delete(b.tags, b.instanceARN(instID))
		}
	}

	// Clean up all custom endpoints associated with this cluster.
	for epID, ep := range b.clusterEndpoints {
		if ep.DBClusterIdentifier == id {
			delete(b.clusterEndpoints, epID)
		}
	}

	return &cp, nil
}

// ModifyDBCluster modifies a Neptune DB cluster.
func (b *InMemoryBackend) ModifyDBCluster(id, paramGroupName string, opts DBClusterModifyOptions) (*DBCluster, error) {
	b.mu.Lock("ModifyDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if paramGroupName != "" {
		c.DBClusterParameterGroupName = paramGroupName
	}
	if opts.EngineVersion != "" {
		c.EngineVersion = opts.EngineVersion
	}
	if opts.PreferredBackupWindow != "" {
		c.PreferredBackupWindow = opts.PreferredBackupWindow
	}
	if opts.PreferredMaintenanceWindow != "" {
		c.PreferredMaintenanceWindow = opts.PreferredMaintenanceWindow
	}
	if opts.IamAuthSet {
		c.EnableIAMDatabaseAuthentication = opts.EnableIAMDatabaseAuthentication
	}
	if opts.DeletionProtectionSet {
		c.DeletionProtection = opts.DeletionProtection
	}
	if opts.ServerlessV2ScalingConfig != nil {
		sv2 := *opts.ServerlessV2ScalingConfig
		c.ServerlessV2ScalingConfig = &sv2
	}
	if opts.ManageMasterUserPassword {
		if c.MasterUserManagedSecret == nil {
			c.MasterUserManagedSecret = &MasterUserManagedSecret{
				SecretARN:    fmt.Sprintf("arn:aws:secretsmanager:%s:%s:secret:rds!cluster-%s", b.region, b.accountID, id),
				SecretStatus: "active",
			}
		}
	}
	cp := cloneCluster(c)

	return &cp, nil
}

// StopDBCluster stops a Neptune DB cluster.
func (b *InMemoryBackend) StopDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("StopDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	c.Status = clusterStatusStopped
	cp := cloneCluster(c)

	return &cp, nil
}

// StartDBCluster starts a stopped Neptune DB cluster.
func (b *InMemoryBackend) StartDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("StartDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	c.Status = clusterStatusAvailable
	cp := cloneCluster(c)

	return &cp, nil
}

// FailoverDBCluster triggers a failover for a Neptune DB cluster.
func (b *InMemoryBackend) FailoverDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("FailoverDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cp := cloneCluster(c)

	return &cp, nil
}

// CreateDBInstance creates a new Neptune DB instance.
func (b *InMemoryBackend) CreateDBInstance(id, clusterID, instanceClass string, opts DBInstanceCreateOptions) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBInstance")
	defer b.mu.Unlock()
	if _, exists := b.instances[id]; exists {
		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}
	if clusterID != "" {
		if _, exists := b.clusters[clusterID]; !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
		}
	}
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	maintenanceWindow := defaultMaintenanceWindow
	if opts.PreferredMaintenanceWindow != "" {
		maintenanceWindow = opts.PreferredMaintenanceWindow
	}
	endpoint := fmt.Sprintf("%s.neptune.%s.amazonaws.com", id, b.region)
	engineVersion := defaultEngineVersion
	if clusterID != "" {
		if cl, ok := b.clusters[clusterID]; ok {
			engineVersion = cl.EngineVersion
		}
	}
	inst := &DBInstance{
		DBInstanceIdentifier:            id,
		DBInstanceArn:                   b.instanceARN(id),
		DBClusterIdentifier:             clusterID,
		DBInstanceClass:                 instanceClass,
		Engine:                          neptuneEngine,
		EngineVersion:                   engineVersion,
		DBInstanceStatus:                clusterStatusAvailable,
		Endpoint:                        endpoint,
		Port:                            defaultNeptunePort,
		AutoMinorVersionUpgrade:         true,
		PreferredMaintenanceWindow:      maintenanceWindow,
		DBParameterGroupName:            opts.DBParameterGroupName,
		PreferredBackupWindow:           opts.PreferredBackupWindow,
		AvailabilityZone:                opts.AvailabilityZone,
		CopyTagsToSnapshot:              opts.CopyTagsToSnapshot,
		EnableIAMDatabaseAuthentication: opts.EnableIAMDatabaseAuthentication,
		PromotionTier:                   opts.PromotionTier,
		StorageEncrypted:                opts.StorageEncrypted,
	}
	if opts.AutoMinorVersionUpgrade {
		inst.AutoMinorVersionUpgrade = opts.AutoMinorVersionUpgrade
	}
	b.instances[id] = inst
	if clusterID != "" {
		if cl, ok := b.clusters[clusterID]; ok {
			isWriter := len(cl.DBClusterMembers) == 0
			cl.DBClusterMembers = append(cl.DBClusterMembers, DBClusterMember{
				DBInstanceIdentifier: id,
				IsClusterWriter:      isWriter,
			})
		}
	}
	cp := *inst

	return &cp, nil
}

// DescribeDBInstances returns all Neptune DB instances or a specific one by ID.
func (b *InMemoryBackend) DescribeDBInstances(id string) ([]DBInstance, error) {
	b.mu.RLock("DescribeDBInstances")
	defer b.mu.RUnlock()
	if id != "" {
		inst, exists := b.instances[id]
		if !exists {
			return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
		}
		cp := *inst

		return []DBInstance{cp}, nil
	}
	result := make([]DBInstance, 0, len(b.instances))
	for _, inst := range b.instances {
		result = append(result, *inst)
	}

	return result, nil
}

// DeleteDBInstance deletes a Neptune DB instance.
func (b *InMemoryBackend) DeleteDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("DeleteDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := *inst
	delete(b.instances, id)
	delete(b.tags, b.instanceARN(id))
	if cp.DBClusterIdentifier != "" {
		if cl, ok := b.clusters[cp.DBClusterIdentifier]; ok {
			members := make([]DBClusterMember, 0, len(cl.DBClusterMembers))
			for _, m := range cl.DBClusterMembers {
				if m.DBInstanceIdentifier != id {
					members = append(members, m)
				}
			}
			cl.DBClusterMembers = members
		}
	}

	return &cp, nil
}

// ModifyDBInstance modifies a Neptune DB instance.
func (b *InMemoryBackend) ModifyDBInstance(id, instanceClass string, opts DBInstanceModifyOptions) (*DBInstance, error) {
	b.mu.Lock("ModifyDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	if instanceClass != "" {
		inst.DBInstanceClass = instanceClass
	}
	if opts.DBParameterGroupName != "" {
		inst.DBParameterGroupName = opts.DBParameterGroupName
	}
	if opts.PreferredMaintenanceWindow != "" {
		inst.PreferredMaintenanceWindow = opts.PreferredMaintenanceWindow
	}
	if opts.PreferredBackupWindow != "" {
		inst.PreferredBackupWindow = opts.PreferredBackupWindow
	}
	if opts.AutoMinorVersionUpgradeSet {
		inst.AutoMinorVersionUpgrade = opts.AutoMinorVersionUpgrade
	}
	if opts.CopyTagsToSnapshotSet {
		inst.CopyTagsToSnapshot = opts.CopyTagsToSnapshot
	}
	if opts.IamAuthSet {
		inst.EnableIAMDatabaseAuthentication = opts.EnableIAMDatabaseAuthentication
	}
	if opts.PromotionTierSet {
		inst.PromotionTier = opts.PromotionTier
	}
	cp := *inst

	return &cp, nil
}

// RebootDBInstance reboots a Neptune DB instance.
func (b *InMemoryBackend) RebootDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("RebootDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := *inst

	return &cp, nil
}

// CreateDBSubnetGroup creates a new Neptune DB subnet group.
func (b *InMemoryBackend) CreateDBSubnetGroup(
	name, description, vpcID string,
	subnetIDs []string,
) (*DBSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBSubnetGroupName is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBSubnetGroup")
	defer b.mu.Unlock()
	if _, exists := b.subnetGroups[name]; exists {
		return nil, fmt.Errorf("%w: subnet group %s already exists", ErrSubnetGroupAlreadyExists, name)
	}
	ids := make([]string, len(subnetIDs))
	copy(ids, subnetIDs)
	sg := &DBSubnetGroup{
		DBSubnetGroupName:        name,
		DBSubnetGroupDescription: description,
		VpcID:                    vpcID,
		Status:                   "Complete",
		SubnetIDs:                ids,
	}
	b.subnetGroups[name] = sg
	cp := *sg
	cp.SubnetIDs = make([]string, len(ids))
	copy(cp.SubnetIDs, ids)

	return &cp, nil
}

// DescribeDBSubnetGroups returns all Neptune DB subnet groups or a specific one.
func (b *InMemoryBackend) DescribeDBSubnetGroups(name string) ([]DBSubnetGroup, error) {
	b.mu.RLock("DescribeDBSubnetGroups")
	defer b.mu.RUnlock()
	if name != "" {
		sg, exists := b.subnetGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
		}
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)

		return []DBSubnetGroup{cp}, nil
	}
	result := make([]DBSubnetGroup, 0, len(b.subnetGroups))
	for _, sg := range b.subnetGroups {
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)
		result = append(result, cp)
	}

	return result, nil
}

// DeleteDBSubnetGroup deletes a Neptune DB subnet group.
func (b *InMemoryBackend) DeleteDBSubnetGroup(name string) error {
	b.mu.Lock("DeleteDBSubnetGroup")
	defer b.mu.Unlock()
	if _, exists := b.subnetGroups[name]; !exists {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	delete(b.subnetGroups, name)
	delete(b.tags, b.subnetGroupARN(name))

	return nil
}

// CreateDBClusterParameterGroup creates a Neptune DB cluster parameter group.
func (b *InMemoryBackend) CreateDBClusterParameterGroup(
	name, family, description string,
) (*DBClusterParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.clusterParameterGroups[name]; exists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrClusterParameterGroupAlreadyExists,
			name,
		)
	}
	pg := &DBClusterParameterGroup{
		DBClusterParameterGroupName: name,
		DBParameterGroupFamily:      family,
		Description:                 description,
	}
	b.clusterParameterGroups[name] = pg
	cp := *pg

	return &cp, nil
}

// DescribeDBClusterParameterGroups returns all Neptune cluster parameter groups or a specific one.
func (b *InMemoryBackend) DescribeDBClusterParameterGroups(name string) ([]DBClusterParameterGroup, error) {
	b.mu.RLock("DescribeDBClusterParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.clusterParameterGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
		}
		cp := *pg

		return []DBClusterParameterGroup{cp}, nil
	}
	result := make([]DBClusterParameterGroup, 0, len(b.clusterParameterGroups))
	for _, pg := range b.clusterParameterGroups {
		result = append(result, *pg)
	}

	return result, nil
}

// DeleteDBClusterParameterGroup deletes a Neptune DB cluster parameter group.
func (b *InMemoryBackend) DeleteDBClusterParameterGroup(name string) error {
	b.mu.Lock("DeleteDBClusterParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.clusterParameterGroups[name]; !exists {
		return fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	delete(b.clusterParameterGroups, name)
	delete(b.tags, b.clusterParameterGroupARN(name))

	return nil
}

// ModifyDBClusterParameterGroup modifies a Neptune DB cluster parameter group.
func (b *InMemoryBackend) ModifyDBClusterParameterGroup(name string) (*DBClusterParameterGroup, error) {
	b.mu.Lock("ModifyDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	cp := *pg

	return &cp, nil
}

// CreateDBClusterSnapshot creates a Neptune DB cluster snapshot.
func (b *InMemoryBackend) CreateDBClusterSnapshot(snapshotID, clusterID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterSnapshot")
	defer b.mu.Unlock()
	if _, exists := b.clusterSnapshots[snapshotID]; exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s already exists", ErrClusterSnapshotAlreadyExists, snapshotID)
	}
	cl, exists := b.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterSnapshotArn:        b.clusterSnapshotARN(snapshotID),
		DBClusterIdentifier:         clusterID,
		Engine:                      neptuneEngine,
		EngineVersion:               cl.EngineVersion,
		Status:                      clusterStatusAvailable,
		StorageEncrypted:            cl.StorageEncrypted,
		SnapshotType:                snapshotSourceManual,
	}
	b.clusterSnapshots[snapshotID] = snap
	cp := *snap

	return &cp, nil
}

// DescribeDBClusterSnapshots returns all Neptune cluster snapshots or a specific one.
// If clusterID is set, results are filtered to that cluster.
func (b *InMemoryBackend) DescribeDBClusterSnapshots(snapshotID, clusterID string) ([]DBClusterSnapshot, error) {
	b.mu.RLock("DescribeDBClusterSnapshots")
	defer b.mu.RUnlock()
	if snapshotID != "" {
		snap, exists := b.clusterSnapshots[snapshotID]
		if !exists {
			return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
		}
		cp := *snap

		return []DBClusterSnapshot{cp}, nil
	}
	result := make([]DBClusterSnapshot, 0, len(b.clusterSnapshots))
	for _, snap := range b.clusterSnapshots {
		if clusterID != "" && snap.DBClusterIdentifier != clusterID {
			continue
		}
		result = append(result, *snap)
	}

	return result, nil
}

// DeleteDBClusterSnapshot deletes a Neptune DB cluster snapshot.
func (b *InMemoryBackend) DeleteDBClusterSnapshot(snapshotID string) (*DBClusterSnapshot, error) {
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	cp := *snap
	delete(b.clusterSnapshots, snapshotID)
	delete(b.tags, b.clusterSnapshotARN(snapshotID))

	return &cp, nil
}

// AddTagsToResource adds or updates tags on a Neptune resource.
func (b *InMemoryBackend) AddTagsToResource(arn string, tags []Tag) {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()
	current := b.tags[arn]
	idx := make(map[string]int, len(current))
	for i, t := range current {
		idx[t.Key] = i
	}
	for _, t := range tags {
		if i, ok := idx[t.Key]; ok {
			current[i].Value = t.Value
		} else {
			idx[t.Key] = len(current)
			current = append(current, t)
		}
	}
	b.tags[arn] = current
}

// RemoveTagsFromResource removes tags from a Neptune resource.
func (b *InMemoryBackend) RemoveTagsFromResource(arn string, keys []string) {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()
	remove := make(map[string]bool, len(keys))
	for _, k := range keys {
		remove[k] = true
	}
	current := b.tags[arn]
	kept := make([]Tag, 0, len(current))
	for _, t := range current {
		if !remove[t.Key] {
			kept = append(kept, t)
		}
	}
	b.tags[arn] = kept
}

// ListTagsForResource returns the tags for a Neptune resource.
func (b *InMemoryBackend) ListTagsForResource(arn string) []Tag {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	src := b.tags[arn]
	cp := make([]Tag, len(src))
	copy(cp, src)

	return cp
}

// AddRoleToDBCluster associates an IAM role with a Neptune DB cluster.
func (b *InMemoryBackend) AddRoleToDBCluster(clusterID, roleARN string) error {
	if clusterID == "" {
		return fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn is required", ErrInvalidParameter)
	}
	b.mu.Lock("AddRoleToDBCluster")
	defer b.mu.Unlock()
	if _, exists := b.clusters[clusterID]; !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	if slices.Contains(b.clusterRoles[clusterID], roleARN) {
		return nil
	}
	b.clusterRoles[clusterID] = append(b.clusterRoles[clusterID], roleARN)

	return nil
}

// AddSourceIdentifierToSubscription adds a source identifier to an event subscription.
func (b *InMemoryBackend) AddSourceIdentifierToSubscription(name, sourceID string) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("AddSourceIdentifierToSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions[name]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
	}
	if !slices.Contains(sub.SourceIDs, sourceID) {
		sub.SourceIDs = append(sub.SourceIDs, sourceID)
	}
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

	return &cp, nil
}

// ApplyPendingMaintenanceAction applies a pending maintenance action to a resource.
func (b *InMemoryBackend) ApplyPendingMaintenanceAction(resourceID, applyAction, optInType string) error {
	if resourceID == "" {
		return fmt.Errorf("%w: ResourceIdentifier is required", ErrInvalidParameter)
	}
	if applyAction == "" {
		return fmt.Errorf("%w: ApplyAction is required", ErrInvalidParameter)
	}
	if optInType == "" {
		return fmt.Errorf("%w: OptInType is required", ErrInvalidParameter)
	}

	return nil
}

// CopyDBClusterParameterGroup copies a Neptune DB cluster parameter group.
func (b *InMemoryBackend) CopyDBClusterParameterGroup(
	sourceName, targetName, targetDescription string,
) (*DBClusterParameterGroup, error) {
	if sourceName == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	if targetName == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CopyDBClusterParameterGroup")
	defer b.mu.Unlock()
	src, exists := b.clusterParameterGroups[sourceName]
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, sourceName)
	}
	_, targetExists := b.clusterParameterGroups[targetName]
	if targetExists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrClusterParameterGroupAlreadyExists,
			targetName,
		)
	}
	description := targetDescription
	if description == "" {
		description = src.Description
	}
	pg := &DBClusterParameterGroup{
		DBClusterParameterGroupName: targetName,
		DBParameterGroupFamily:      src.DBParameterGroupFamily,
		Description:                 description,
	}
	b.clusterParameterGroups[targetName] = pg
	cp := *pg

	return &cp, nil
}

// CopyDBClusterSnapshot copies a Neptune DB cluster snapshot.
func (b *InMemoryBackend) CopyDBClusterSnapshot(sourceSnapshotID, targetSnapshotID string) (*DBClusterSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CopyDBClusterSnapshot")
	defer b.mu.Unlock()
	src, exists := b.clusterSnapshots[sourceSnapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, sourceSnapshotID)
	}
	_, targetExists := b.clusterSnapshots[targetSnapshotID]
	if targetExists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			targetSnapshotID,
		)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: targetSnapshotID,
		DBClusterSnapshotArn:        b.clusterSnapshotARN(targetSnapshotID),
		DBClusterIdentifier:         src.DBClusterIdentifier,
		Engine:                      src.Engine,
		EngineVersion:               src.EngineVersion,
		Status:                      clusterStatusAvailable,
		StorageEncrypted:            src.StorageEncrypted,
		SnapshotType:                snapshotSourceManual,
	}
	b.clusterSnapshots[targetSnapshotID] = snap
	cp := *snap

	return &cp, nil
}

// CopyDBParameterGroup copies a Neptune DB parameter group.
func (b *InMemoryBackend) CopyDBParameterGroup(
	sourceName, targetName, targetDescription string,
) (*DBParameterGroup, error) {
	if sourceName == "" {
		return nil, fmt.Errorf("%w: SourceDBParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	if targetName == "" {
		return nil, fmt.Errorf("%w: TargetDBParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CopyDBParameterGroup")
	defer b.mu.Unlock()
	src, exists := b.parameterGroups[sourceName]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, sourceName)
	}
	_, targetExists := b.parameterGroups[targetName]
	if targetExists {
		return nil, fmt.Errorf("%w: parameter group %s already exists", ErrParameterGroupAlreadyExists, targetName)
	}
	description := targetDescription
	if description == "" {
		description = src.Description
	}
	pg := &DBParameterGroup{
		DBParameterGroupName:   targetName,
		DBParameterGroupFamily: src.DBParameterGroupFamily,
		Description:            description,
	}
	b.parameterGroups[targetName] = pg
	cp := *pg

	return &cp, nil
}

// CreateDBClusterEndpoint creates a Neptune DB cluster custom endpoint.
func (b *InMemoryBackend) CreateDBClusterEndpoint(
	endpointID, clusterID, endpointType string,
) (*DBClusterEndpoint, error) {
	if endpointID == "" {
		return nil, fmt.Errorf("%w: DBClusterEndpointIdentifier is required", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterEndpoint")
	defer b.mu.Unlock()
	if _, exists := b.clusterEndpoints[endpointID]; exists {
		return nil, fmt.Errorf("%w: cluster endpoint %s already exists", ErrClusterEndpointAlreadyExists, endpointID)
	}
	if _, exists := b.clusters[clusterID]; !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	if endpointType == "" {
		endpointType = endpointTypeReader
	}
	switch endpointType {
	case endpointTypeReader, endpointTypeWriter, endpointTypeCustom, endpointTypeAny:
	default:
		return nil, fmt.Errorf("%w: EndpointType must be one of READER, WRITER, CUSTOM, ANY", ErrInvalidParameter)
	}
	ep := &DBClusterEndpoint{
		DBClusterEndpointIdentifier: endpointID,
		DBClusterIdentifier:         clusterID,
		EndpointType:                endpointType,
		Status:                      clusterStatusAvailable,
		Endpoint:                    fmt.Sprintf("%s.cluster-custom.neptune.%s.amazonaws.com", endpointID, b.region),
	}
	b.clusterEndpoints[endpointID] = ep
	cp := *ep

	return &cp, nil
}

// CreateDBParameterGroup creates a Neptune DB parameter group.
func (b *InMemoryBackend) CreateDBParameterGroup(name, family, description string) (*DBParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBParameterGroupName is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.parameterGroups[name]; exists {
		return nil, fmt.Errorf("%w: parameter group %s already exists", ErrParameterGroupAlreadyExists, name)
	}
	pg := &DBParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupFamily: family,
		Description:            description,
	}
	b.parameterGroups[name] = pg
	cp := *pg

	return &cp, nil
}

// CreateEventSubscription creates a Neptune event notification subscription.
func (b *InMemoryBackend) CreateEventSubscription(
	name, snsTopicARN string,
	sourceIDs []string,
) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	if snsTopicARN == "" {
		return nil, fmt.Errorf("%w: SnsTopicArn is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateEventSubscription")
	defer b.mu.Unlock()
	if _, exists := b.eventSubscriptions[name]; exists {
		return nil, fmt.Errorf("%w: subscription %s already exists", ErrSubscriptionAlreadyExists, name)
	}
	ids := make([]string, len(sourceIDs))
	copy(ids, sourceIDs)
	sub := &EventSubscription{
		CustSubscriptionID: name,
		SnsTopicARN:        snsTopicARN,
		Status:             subscriptionStatusActive,
		SourceIDs:          ids,
	}
	b.eventSubscriptions[name] = sub
	cp := *sub
	cp.SourceIDs = make([]string, len(ids))
	copy(cp.SourceIDs, ids)

	return &cp, nil
}

// CreateGlobalCluster creates a Neptune global cluster.
func (b *InMemoryBackend) CreateGlobalCluster(globalClusterID, sourceDBClusterID string) (*GlobalCluster, error) {
	if globalClusterID == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateGlobalCluster")
	defer b.mu.Unlock()
	if _, exists := b.globalClusters[globalClusterID]; exists {
		return nil, fmt.Errorf("%w: global cluster %s already exists", ErrGlobalClusterAlreadyExists, globalClusterID)
	}
	gc := &GlobalCluster{
		GlobalClusterIdentifier: globalClusterID,
		Status:                  clusterStatusAvailable,
	}
	if sourceDBClusterID != "" {
		if cl, exists := b.clusters[sourceDBClusterID]; exists {
			gc.GlobalClusterMembers = []GlobalClusterMember{
				{
					DBClusterARN: b.clusterARN(cl.DBClusterIdentifier),
					IsWriter:     true,
				},
			}
		}
	}
	b.globalClusters[globalClusterID] = gc
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// DescribeGlobalClusters returns all Neptune global clusters.
func (b *InMemoryBackend) DescribeGlobalClusters() []GlobalCluster {
	b.mu.RLock("DescribeGlobalClusters")
	defer b.mu.RUnlock()
	result := make([]GlobalCluster, 0, len(b.globalClusters))
	for _, gc := range b.globalClusters {
		cp := *gc
		cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
		copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)
		result = append(result, cp)
	}

	return result
}

// DeleteDBClusterEndpoint deletes a Neptune DB cluster custom endpoint.
func (b *InMemoryBackend) DeleteDBClusterEndpoint(endpointID string) error {
	b.mu.Lock("DeleteDBClusterEndpoint")
	defer b.mu.Unlock()
	if _, exists := b.clusterEndpoints[endpointID]; !exists {
		return fmt.Errorf("%w: cluster endpoint %s not found", ErrClusterEndpointNotFound, endpointID)
	}
	delete(b.clusterEndpoints, endpointID)

	return nil
}

// DescribeDBClusterEndpoints returns all Neptune DB cluster endpoints or a specific one.
func (b *InMemoryBackend) DescribeDBClusterEndpoints(endpointID, clusterID string) ([]DBClusterEndpoint, error) {
	b.mu.RLock("DescribeDBClusterEndpoints")
	defer b.mu.RUnlock()
	if endpointID != "" {
		ep, exists := b.clusterEndpoints[endpointID]
		if !exists {
			return nil, fmt.Errorf("%w: cluster endpoint %s not found", ErrClusterEndpointNotFound, endpointID)
		}
		cp := *ep

		return []DBClusterEndpoint{cp}, nil
	}
	result := make([]DBClusterEndpoint, 0, len(b.clusterEndpoints))
	for _, ep := range b.clusterEndpoints {
		if clusterID != "" && ep.DBClusterIdentifier != clusterID {
			continue
		}
		result = append(result, *ep)
	}

	return result, nil
}

// ModifyDBClusterEndpoint modifies a Neptune DB cluster custom endpoint.
func (b *InMemoryBackend) ModifyDBClusterEndpoint(endpointID, endpointType string) (*DBClusterEndpoint, error) {
	b.mu.Lock("ModifyDBClusterEndpoint")
	defer b.mu.Unlock()
	ep, exists := b.clusterEndpoints[endpointID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster endpoint %s not found", ErrClusterEndpointNotFound, endpointID)
	}
	if endpointType != "" {
		ep.EndpointType = endpointType
	}
	cp := *ep

	return &cp, nil
}

// DeleteDBParameterGroup deletes a Neptune DB parameter group.
func (b *InMemoryBackend) DeleteDBParameterGroup(name string) error {
	b.mu.Lock("DeleteDBParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.parameterGroups[name]; !exists {
		return fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	delete(b.parameterGroups, name)

	return nil
}

// DescribeDBParameterGroups returns all Neptune DB parameter groups or a specific one.
func (b *InMemoryBackend) DescribeDBParameterGroups(name string) ([]DBParameterGroup, error) {
	b.mu.RLock("DescribeDBParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.parameterGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
		}
		cp := *pg

		return []DBParameterGroup{cp}, nil
	}
	result := make([]DBParameterGroup, 0, len(b.parameterGroups))
	for _, pg := range b.parameterGroups {
		result = append(result, *pg)
	}

	return result, nil
}

// ModifyDBParameterGroup modifies a Neptune DB parameter group.
func (b *InMemoryBackend) ModifyDBParameterGroup(name string) (*DBParameterGroup, error) {
	b.mu.Lock("ModifyDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	cp := *pg

	return &cp, nil
}

// ResetDBParameterGroup resets a Neptune DB parameter group to its default values.
func (b *InMemoryBackend) ResetDBParameterGroup(name string) (*DBParameterGroup, error) {
	b.mu.Lock("ResetDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	cp := *pg

	return &cp, nil
}

// ResetDBClusterParameterGroup resets a Neptune DB cluster parameter group to its default values.
func (b *InMemoryBackend) ResetDBClusterParameterGroup(name string) (*DBClusterParameterGroup, error) {
	b.mu.Lock("ResetDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	cp := *pg

	return &cp, nil
}

// DeleteEventSubscription deletes a Neptune event subscription.
func (b *InMemoryBackend) DeleteEventSubscription(name string) (*EventSubscription, error) {
	b.mu.Lock("DeleteEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions[name]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
	}
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)
	delete(b.eventSubscriptions, name)

	return &cp, nil
}

// DescribeEventSubscriptions returns all event subscriptions or a specific one.
func (b *InMemoryBackend) DescribeEventSubscriptions(name string) ([]EventSubscription, error) {
	b.mu.RLock("DescribeEventSubscriptions")
	defer b.mu.RUnlock()
	if name != "" {
		sub, exists := b.eventSubscriptions[name]
		if !exists {
			return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
		}
		cp := *sub
		cp.SourceIDs = make([]string, len(sub.SourceIDs))
		copy(cp.SourceIDs, sub.SourceIDs)

		return []EventSubscription{cp}, nil
	}
	result := make([]EventSubscription, 0, len(b.eventSubscriptions))
	for _, sub := range b.eventSubscriptions {
		cp := *sub
		cp.SourceIDs = make([]string, len(sub.SourceIDs))
		copy(cp.SourceIDs, sub.SourceIDs)
		result = append(result, cp)
	}

	return result, nil
}

// ModifyEventSubscription modifies a Neptune event subscription.
func (b *InMemoryBackend) ModifyEventSubscription(name, snsTopicARN string) (*EventSubscription, error) {
	b.mu.Lock("ModifyEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions[name]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
	}
	if snsTopicARN != "" {
		sub.SnsTopicARN = snsTopicARN
	}
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

	return &cp, nil
}

// RemoveSourceIdentifierFromSubscription removes a source identifier from a Neptune event subscription.
func (b *InMemoryBackend) RemoveSourceIdentifierFromSubscription(name, sourceID string) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("RemoveSourceIdentifierFromSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions[name]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
	}
	kept := make([]string, 0, len(sub.SourceIDs))
	for _, id := range sub.SourceIDs {
		if id != sourceID {
			kept = append(kept, id)
		}
	}
	sub.SourceIDs = kept
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

	return &cp, nil
}

// DeleteGlobalCluster deletes a Neptune global cluster.
func (b *InMemoryBackend) DeleteGlobalCluster(globalClusterID string) (*GlobalCluster, error) {
	b.mu.Lock("DeleteGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[globalClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, globalClusterID)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)
	delete(b.globalClusters, globalClusterID)

	return &cp, nil
}

// FailoverGlobalCluster performs a failover for a Neptune global cluster.
// targetDBClusterID is accepted for API compatibility but not used in the in-memory backend.
func (b *InMemoryBackend) FailoverGlobalCluster(globalClusterID, _ string) (*GlobalCluster, error) {
	b.mu.Lock("FailoverGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[globalClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, globalClusterID)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// ModifyGlobalCluster modifies a Neptune global cluster.
func (b *InMemoryBackend) ModifyGlobalCluster(globalClusterID string) (*GlobalCluster, error) {
	b.mu.Lock("ModifyGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[globalClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, globalClusterID)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// RemoveFromGlobalCluster removes a DB cluster from a Neptune global cluster.
func (b *InMemoryBackend) RemoveFromGlobalCluster(globalClusterID, dbClusterARN string) (*GlobalCluster, error) {
	b.mu.Lock("RemoveFromGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[globalClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, globalClusterID)
	}
	kept := make([]GlobalClusterMember, 0, len(gc.GlobalClusterMembers))
	for _, m := range gc.GlobalClusterMembers {
		if m.DBClusterARN != dbClusterARN {
			kept = append(kept, m)
		}
	}
	gc.GlobalClusterMembers = kept
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// SwitchoverGlobalCluster switches over a Neptune global cluster to a new primary.
// targetDBClusterID is accepted for API compatibility but not used in the in-memory backend.
func (b *InMemoryBackend) SwitchoverGlobalCluster(globalClusterID, _ string) (*GlobalCluster, error) {
	b.mu.Lock("SwitchoverGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[globalClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, globalClusterID)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// RemoveRoleFromDBCluster removes an IAM role association from a Neptune DB cluster.
func (b *InMemoryBackend) RemoveRoleFromDBCluster(clusterID, roleARN string) error {
	if clusterID == "" {
		return fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn is required", ErrInvalidParameter)
	}
	b.mu.Lock("RemoveRoleFromDBCluster")
	defer b.mu.Unlock()
	if _, exists := b.clusters[clusterID]; !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	roles := b.clusterRoles[clusterID]
	kept := make([]string, 0, len(roles))
	for _, r := range roles {
		if r != roleARN {
			kept = append(kept, r)
		}
	}
	b.clusterRoles[clusterID] = kept

	return nil
}

// RestoreDBClusterFromSnapshot restores a Neptune DB cluster from a snapshot.
func (b *InMemoryBackend) RestoreDBClusterFromSnapshot(snapshotID, clusterID string) (*DBCluster, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("RestoreDBClusterFromSnapshot")
	defer b.mu.Unlock()
	snap, snapExists := b.clusterSnapshots[snapshotID]
	if !snapExists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	if _, clExists := b.clusters[clusterID]; clExists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, clusterID)
	}
	// Derive parameter group from the source cluster if available.
	paramGroupName := pgFamilyDefaultNeptune13
	if srcCluster, ok := b.clusters[snap.DBClusterIdentifier]; ok {
		paramGroupName = srcCluster.DBClusterParameterGroupName
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.neptune.amazonaws.com", clusterID, b.region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.%s.neptune.amazonaws.com", clusterID, b.region)
	cluster := &DBCluster{
		DBClusterIdentifier:         clusterID,
		DBClusterArn:                b.clusterARN(clusterID),
		Engine:                      snap.Engine,
		EngineVersion:               snap.EngineVersion,
		EngineMode:                  engineModeProvisioned,
		Status:                      clusterStatusAvailable,
		DBClusterParameterGroupName: paramGroupName,
		Endpoint:                    endpoint,
		ReaderEndpoint:              readerEndpoint,
		Port:                        defaultNeptunePort,
		StorageEncrypted:            snap.StorageEncrypted,
		DBClusterMembers:            []DBClusterMember{},
		BackupRetentionPeriod:       defaultBackupRetentionPeriod,
	}
	b.clusters[clusterID] = cluster
	cp := cloneCluster(cluster)

	return &cp, nil
}

// RestoreDBClusterToPointInTime restores a Neptune DB cluster to a point in time.
func (b *InMemoryBackend) RestoreDBClusterToPointInTime(srcClusterID, targetClusterID string) (*DBCluster, error) {
	if srcClusterID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterIdentifier is required", ErrInvalidParameter)
	}
	if targetClusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("RestoreDBClusterToPointInTime")
	defer b.mu.Unlock()
	src, srcExists := b.clusters[srcClusterID]
	if !srcExists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, srcClusterID)
	}
	if _, tgtExists := b.clusters[targetClusterID]; tgtExists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, targetClusterID)
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.neptune.amazonaws.com", targetClusterID, b.region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.%s.neptune.amazonaws.com", targetClusterID, b.region)
	cluster := &DBCluster{
		DBClusterIdentifier:             targetClusterID,
		DBClusterArn:                    b.clusterARN(targetClusterID),
		Engine:                          src.Engine,
		EngineVersion:                   src.EngineVersion,
		EngineMode:                      src.EngineMode,
		Status:                          clusterStatusAvailable,
		DBClusterParameterGroupName:     src.DBClusterParameterGroupName,
		Endpoint:                        endpoint,
		ReaderEndpoint:                  readerEndpoint,
		Port:                            src.Port,
		StorageEncrypted:                src.StorageEncrypted,
		EnableIAMDatabaseAuthentication: src.EnableIAMDatabaseAuthentication,
		DeletionProtection:              src.DeletionProtection,
		DBClusterMembers:                []DBClusterMember{},
		BackupRetentionPeriod:           src.BackupRetentionPeriod,
	}
	b.clusters[targetClusterID] = cluster
	cp := cloneCluster(cluster)

	return &cp, nil
}

// ModifyDBSubnetGroup modifies a Neptune DB subnet group.
func (b *InMemoryBackend) ModifyDBSubnetGroup(name, description string) (*DBSubnetGroup, error) {
	b.mu.Lock("ModifyDBSubnetGroup")
	defer b.mu.Unlock()
	sg, exists := b.subnetGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	if description != "" {
		sg.DBSubnetGroupDescription = description
	}
	cp := *sg
	cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
	copy(cp.SubnetIDs, sg.SubnetIDs)

	return &cp, nil
}

// AccountID returns the backend's AWS account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all backend state, returning it to a clean empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.clusters = make(map[string]*DBCluster)
	b.instances = make(map[string]*DBInstance)
	b.subnetGroups = make(map[string]*DBSubnetGroup)
	b.clusterParameterGroups = make(map[string]*DBClusterParameterGroup)
	b.clusterSnapshots = make(map[string]*DBClusterSnapshot)
	b.parameterGroups = make(map[string]*DBParameterGroup)
	b.clusterEndpoints = make(map[string]*DBClusterEndpoint)
	b.eventSubscriptions = make(map[string]*EventSubscription)
	b.globalClusters = make(map[string]*GlobalCluster)
	b.clusterRoles = make(map[string][]string)
	b.tags = make(map[string][]Tag)
}

// AddClusterInternal creates a cluster directly, bypassing normal validation. Used for seeding tests.
func (b *InMemoryBackend) AddClusterInternal(id string) *DBCluster {
	b.mu.Lock("AddClusterInternal")
	defer b.mu.Unlock()
	endpoint := fmt.Sprintf("%s.cluster.%s.neptune.amazonaws.com", id, b.region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.%s.neptune.amazonaws.com", id, b.region)
	c := &DBCluster{
		DBClusterIdentifier:         id,
		DBClusterArn:                b.clusterARN(id),
		Engine:                      neptuneEngine,
		EngineVersion:               defaultEngineVersion,
		EngineMode:                  engineModeProvisioned,
		Status:                      clusterStatusAvailable,
		DBClusterParameterGroupName: pgFamilyDefaultNeptune13,
		Endpoint:                    endpoint,
		ReaderEndpoint:              readerEndpoint,
		Port:                        defaultNeptunePort,
		BackupRetentionPeriod:       defaultBackupRetentionPeriod,
	}
	b.clusters[id] = c
	cp := cloneCluster(c)

	return &cp
}

// AddSnapshotInternal creates a snapshot directly, bypassing normal validation. Used for seeding tests.
func (b *InMemoryBackend) AddSnapshotInternal(snapshotID, clusterID string) *DBClusterSnapshot {
	b.mu.Lock("AddSnapshotInternal")
	defer b.mu.Unlock()
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterSnapshotArn:        b.clusterSnapshotARN(snapshotID),
		DBClusterIdentifier:         clusterID,
		Engine:                      neptuneEngine,
		EngineVersion:               defaultEngineVersion,
		Status:                      clusterStatusAvailable,
		SnapshotType:                snapshotSourceManual,
	}
	b.clusterSnapshots[snapshotID] = snap
	cp := *snap

	return &cp
}

// AddClusterParameterGroupInternal creates a cluster parameter group directly. Used for seeding tests.
func (b *InMemoryBackend) AddClusterParameterGroupInternal(name, family string) *DBClusterParameterGroup {
	b.mu.Lock("AddClusterParameterGroupInternal")
	defer b.mu.Unlock()
	pg := &DBClusterParameterGroup{
		DBClusterParameterGroupName: name,
		DBParameterGroupFamily:      family,
		Description:                 "seeded for tests",
	}
	b.clusterParameterGroups[name] = pg
	cp := *pg

	return &cp
}

// AddParameterGroupInternal creates a DB parameter group directly. Used for seeding tests.
func (b *InMemoryBackend) AddParameterGroupInternal(name, family string) *DBParameterGroup {
	b.mu.Lock("AddParameterGroupInternal")
	defer b.mu.Unlock()
	pg := &DBParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupFamily: family,
		Description:            "seeded for tests",
	}
	b.parameterGroups[name] = pg
	cp := *pg

	return &cp
}

// AddEventSubscriptionInternal creates an event subscription directly. Used for seeding tests.
func (b *InMemoryBackend) AddEventSubscriptionInternal(name, snsTopicARN string) *EventSubscription {
	b.mu.Lock("AddEventSubscriptionInternal")
	defer b.mu.Unlock()
	sub := &EventSubscription{
		CustSubscriptionID: name,
		SnsTopicARN:        snsTopicARN,
		Status:             subscriptionStatusActive,
	}
	b.eventSubscriptions[name] = sub
	cp := *sub

	return &cp
}
