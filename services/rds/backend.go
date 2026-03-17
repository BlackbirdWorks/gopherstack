package rds

import (
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrInstanceNotFound is returned when an RDS instance does not exist.
	ErrInstanceNotFound = errors.New("DBInstanceNotFound")
	// ErrInstanceAlreadyExists is returned when an RDS instance already exists.
	ErrInstanceAlreadyExists = errors.New("DBInstanceAlreadyExists")
	// ErrSnapshotNotFound is returned when a snapshot does not exist.
	ErrSnapshotNotFound = errors.New("DBSnapshotNotFound")
	// ErrSnapshotAlreadyExists is returned when a snapshot already exists.
	ErrSnapshotAlreadyExists = errors.New("DBSnapshotAlreadyExists")
	// ErrSubnetGroupNotFound is returned when a subnet group does not exist.
	ErrSubnetGroupNotFound = errors.New("DBSubnetGroupNotFound")
	// ErrSubnetGroupAlreadyExists is returned when a subnet group already exists.
	ErrSubnetGroupAlreadyExists = errors.New("DBSubnetGroupAlreadyExists")
	// ErrInvalidParameter is returned for invalid input.
	ErrInvalidParameter = errors.New("InvalidParameterValue")
	// ErrUnknownAction is returned for unrecognized RDS actions.
	ErrUnknownAction = errors.New("InvalidAction")
	// ErrInvalidDBInstanceState is returned when an instance operation is invalid given its current state.
	ErrInvalidDBInstanceState = errors.New("InvalidDBInstanceState")

	ErrParameterGroupNotFound       = errors.New("DBParameterGroupNotFound")
	ErrParameterGroupAlreadyExists  = errors.New("DBParameterGroupAlreadyExists")
	ErrOptionGroupNotFound          = errors.New("OptionGroupNotFound")
	ErrOptionGroupAlreadyExists     = errors.New("OptionGroupAlreadyExists")
	ErrClusterNotFound              = errors.New("DBClusterNotFound")
	ErrClusterAlreadyExists         = errors.New("DBClusterAlreadyExists")
	ErrClusterSnapshotNotFound      = errors.New("DBClusterSnapshotNotFound")
	ErrClusterSnapshotAlreadyExists = errors.New("DBClusterSnapshotAlreadyExists")
	ErrClusterEndpointNotFound      = errors.New("DBClusterEndpointNotFound")
	ErrClusterEndpointAlreadyExists = errors.New("DBClusterEndpointAlreadyExists")
	ErrExportTaskNotFound           = errors.New("ExportTaskNotFound")
	ErrExportTaskAlreadyExists      = errors.New("ExportTaskAlreadyExists")
	ErrGlobalClusterNotFound        = errors.New("GlobalClusterNotFound")
	ErrGlobalClusterAlreadyExists   = errors.New("GlobalClusterAlreadyExists")
)

const (
	defaultPort             = 5432
	mysqlPort               = 3306
	defaultInstanceClass    = "db.t3.micro"
	defaultAllocatedStorage = 20

	instanceStatusAvailable = "available"
	instanceStatusStopped   = "stopped"
)

// DBInstance represents an RDS database instance.
type DBInstance struct {
	DBInstanceIdentifier              string `json:"dbInstanceIdentifier"`
	DbiResourceID                     string `json:"dbiResourceID"`
	DBInstanceClass                   string `json:"dbInstanceClass"`
	Engine                            string `json:"engine"`
	EngineVersion                     string `json:"engineVersion"`
	DBInstanceStatus                  string `json:"dbInstanceStatus"`
	MasterUsername                    string `json:"masterUsername"`
	DBName                            string `json:"dbName"`
	Endpoint                          string `json:"endpoint"`
	VpcID                             string `json:"vpcID"`
	DBSubnetGroupName                 string `json:"dbSubnetGroupName"`
	DBParameterGroupName              string `json:"dbParameterGroupName"`
	ReplicaSourceDBInstanceIdentifier string `json:"replicaSourceDBInstanceIdentifier"`
	AvailabilityZone                  string `json:"availabilityZone"`
	StorageType                       string `json:"storageType"`
	Port                              int    `json:"port"`
	AllocatedStorage                  int    `json:"allocatedStorage"`
	BackupRetentionPeriod             int    `json:"backupRetentionPeriod"`
	MultiAZ                           bool   `json:"multiAZ"`
	StorageEncrypted                  bool   `json:"storageEncrypted"`
	IAMDatabaseAuthenticationEnabled  bool   `json:"iamDatabaseAuthenticationEnabled"`
	DeletionProtection                bool   `json:"deletionProtection"`
}

// DBSnapshot represents an RDS database snapshot.
type DBSnapshot struct {
	DBSnapshotIdentifier string `json:"dbSnapshotIdentifier"`
	DBInstanceIdentifier string `json:"dbInstanceIdentifier"`
	Engine               string `json:"engine"`
	EngineVersion        string `json:"engineVersion"`
	Status               string `json:"status"`
	StorageType          string `json:"storageType"`
	AllocatedStorage     int    `json:"allocatedStorage"`
	Port                 int    `json:"port"`
	StorageEncrypted     bool   `json:"storageEncrypted"`
}

// DBSubnetGroup represents an RDS DB subnet group.
type DBSubnetGroup struct {
	DBSubnetGroupName        string   `json:"dbSubnetGroupName"`
	DBSubnetGroupDescription string   `json:"dbSubnetGroupDescription"`
	VpcID                    string   `json:"vpcID"`
	Status                   string   `json:"status"`
	SubnetIDs                []string `json:"subnetIDs"`
}

// Tag is a key/value tag attached to an RDS resource.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// DBParameter represents a single RDS parameter.
type DBParameter struct {
	ParameterName  string `json:"parameterName"`
	ParameterValue string `json:"parameterValue"`
	Description    string `json:"description"`
	ApplyType      string `json:"applyType"`
	DataType       string `json:"dataType"`
	IsModifiable   bool   `json:"isModifiable"`
}

// DBParameterGroup represents an RDS DB parameter group.
type DBParameterGroup struct {
	Parameters             map[string]DBParameter `json:"parameters"`
	DBParameterGroupName   string                 `json:"dbParameterGroupName"`
	DBParameterGroupFamily string                 `json:"dbParameterGroupFamily"`
	Description            string                 `json:"description"`
}

// OptionGroupOption represents an option within an option group.
type OptionGroupOption struct {
	OptionName    string `json:"optionName"`
	OptionVersion string `json:"optionVersion"`
}

// OptionGroup represents an RDS option group.
type OptionGroup struct {
	OptionGroupName        string              `json:"optionGroupName"`
	OptionGroupDescription string              `json:"optionGroupDescription"`
	EngineName             string              `json:"engineName"`
	MajorEngineVersion     string              `json:"majorEngineVersion"`
	Options                []OptionGroupOption `json:"options"`
}

// DBCluster represents an Aurora-style RDS cluster.
type DBCluster struct {
	DBClusterIdentifier         string `json:"dbClusterIdentifier"`
	Engine                      string `json:"engine"`
	Status                      string `json:"status"`
	MasterUsername              string `json:"masterUsername"`
	DatabaseName                string `json:"databaseName"`
	DBClusterParameterGroupName string `json:"dbClusterParameterGroupName"`
	Endpoint                    string `json:"endpoint"`
	Port                        int    `json:"port"`
}

// DBClusterSnapshot represents an RDS cluster snapshot.
type DBClusterSnapshot struct {
	DBClusterSnapshotIdentifier string `json:"dbClusterSnapshotIdentifier"`
	DBClusterIdentifier         string `json:"dbClusterIdentifier"`
	Engine                      string `json:"engine"`
	Status                      string `json:"status"`
}

// DBClusterEndpoint represents a custom endpoint for an RDS cluster.
type DBClusterEndpoint struct {
	DBClusterEndpointIdentifier string `json:"dbClusterEndpointIdentifier"`
	DBClusterIdentifier         string `json:"dbClusterIdentifier"`
	EndpointType                string `json:"endpointType"`
	Status                      string `json:"status"`
	Endpoint                    string `json:"endpoint"`
}

// ExportTask represents an RDS export task.
type ExportTask struct {
	ExportTaskIdentifier string `json:"exportTaskIdentifier"`
	SourceArn            string `json:"sourceArn"`
	Status               string `json:"status"`
	S3Bucket             string `json:"s3Bucket"`
}

// GlobalCluster represents an RDS global cluster.
type GlobalCluster struct {
	GlobalClusterIdentifier string `json:"globalClusterIdentifier"`
	Engine                  string `json:"engine"`
	EngineVersion           string `json:"engineVersion"`
	Status                  string `json:"status"`
	StorageEncrypted        bool   `json:"storageEncrypted"`
	DeletionProtection      bool   `json:"deletionProtection"`
}

// DBEngineVersion represents an available RDS engine version.
type DBEngineVersion struct {
	Engine              string `json:"engine"`
	EngineVersion       string `json:"engineVersion"`
	DBEngineDescription string `json:"dbEngineDescription"`
}

// OrderableDBInstanceOption represents an orderable DB instance option.
type OrderableDBInstanceOption struct {
	Engine          string `json:"engine"`
	EngineVersion   string `json:"engineVersion"`
	DBInstanceClass string `json:"dbInstanceClass"`
	MultiAZCapable  bool   `json:"multiAZCapable"`
}

// DBLogFile represents a log file for a DB instance.
type DBLogFile struct {
	LogFileName string `json:"logFileName"`
	Size        int64  `json:"size"`
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// DBInstanceOptions holds optional fields for CreateDBInstance and ModifyDBInstance.
type DBInstanceOptions struct {
	EngineVersion                    string
	StorageType                      string
	AvailabilityZone                 string
	BackupRetentionPeriod            int
	MultiAZ                          bool
	StorageEncrypted                 bool
	IAMDatabaseAuthenticationEnabled bool
	DeletionProtection               bool
}

// InMemoryBackend is the in-memory store for RDS resources.
type InMemoryBackend struct {
	dnsRegistrar           DNSRegistrar
	instances              map[string]*DBInstance
	snapshots              map[string]*DBSnapshot
	subnetGroups           map[string]*DBSubnetGroup
	tags                   map[string][]Tag
	parameterGroups        map[string]*DBParameterGroup
	clusterParameterGroups map[string]*DBParameterGroup
	optionGroups           map[string]*OptionGroup
	clusters               map[string]*DBCluster
	clusterSnapshots       map[string]*DBClusterSnapshot
	clusterEndpoints       map[string]*DBClusterEndpoint
	exportTasks            map[string]*ExportTask
	globalClusters         map[string]*GlobalCluster
	fisFailoverFaults      map[string]time.Time // keyed by cluster identifier; value is expiry (zero = permanent)
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		instances:              make(map[string]*DBInstance),
		snapshots:              make(map[string]*DBSnapshot),
		subnetGroups:           make(map[string]*DBSubnetGroup),
		tags:                   make(map[string][]Tag),
		parameterGroups:        make(map[string]*DBParameterGroup),
		clusterParameterGroups: make(map[string]*DBParameterGroup),
		optionGroups:           make(map[string]*OptionGroup),
		clusters:               make(map[string]*DBCluster),
		clusterSnapshots:       make(map[string]*DBClusterSnapshot),
		clusterEndpoints:       make(map[string]*DBClusterEndpoint),
		exportTasks:            make(map[string]*ExportTask),
		globalClusters:         make(map[string]*GlobalCluster),
		fisFailoverFaults:      make(map[string]time.Time),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("rds"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// SetDNSRegistrar wires a DNS server so RDS instance hostnames are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	b.dnsRegistrar = dns
	b.mu.Unlock()
}

// enginePort returns the default port for the given database engine.
func enginePort(engine string) int {
	switch engine {
	case "mysql", "mariadb", "aurora-mysql":
		return mysqlPort
	default:
		return defaultPort
	}
}

// CreateDBInstance creates a new RDS DB instance.
func (b *InMemoryBackend) CreateDBInstance(
	id, engine, instanceClass, dbName, masterUser, paramGroupName string,
	allocatedStorage int,
	opts DBInstanceOptions,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDBInstance")

	if _, exists := b.instances[id]; exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}

	if engine == "" {
		engine = "postgres"
	}
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	if allocatedStorage <= 0 {
		allocatedStorage = defaultAllocatedStorage
	}
	if masterUser == "" {
		masterUser = "admin"
	}
	if opts.StorageType == "" {
		opts.StorageType = "gp2"
	}
	if opts.AvailabilityZone == "" {
		opts.AvailabilityZone = b.region + "a"
	}

	port := enginePort(engine)
	endpoint := fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)

	inst := &DBInstance{
		DBInstanceIdentifier:             id,
		DbiResourceID:                    id,
		DBInstanceClass:                  instanceClass,
		Engine:                           engine,
		EngineVersion:                    opts.EngineVersion,
		DBInstanceStatus:                 instanceStatusAvailable,
		MasterUsername:                   masterUser,
		DBName:                           dbName,
		Endpoint:                         endpoint,
		Port:                             port,
		AllocatedStorage:                 allocatedStorage,
		DBParameterGroupName:             paramGroupName,
		MultiAZ:                          opts.MultiAZ,
		StorageType:                      opts.StorageType,
		StorageEncrypted:                 opts.StorageEncrypted,
		AvailabilityZone:                 opts.AvailabilityZone,
		BackupRetentionPeriod:            opts.BackupRetentionPeriod,
		IAMDatabaseAuthenticationEnabled: opts.IAMDatabaseAuthenticationEnabled,
		DeletionProtection:               opts.DeletionProtection,
	}
	b.instances[id] = inst
	cp := *inst

	b.mu.Unlock()

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	return &cp, nil
}

// rdsARN constructs the ARN for an RDS resource.
// The format is: arn:aws:rds:{region}:{accountID}:{resourceType}:{id}.
func (b *InMemoryBackend) rdsARN(resourceType, id string) string {
	return fmt.Sprintf("arn:aws:rds:%s:%s:%s:%s", b.region, b.accountID, resourceType, id)
}

// DeleteDBInstance removes the DB instance with the given identifier.
func (b *InMemoryBackend) DeleteDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("DeleteDBInstance")

	inst, exists := b.instances[id]
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	cp := *inst
	delete(b.instances, id)
	delete(b.tags, b.rdsARN("db", id))

	b.mu.Unlock()

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(cp.Endpoint)
	}

	return &cp, nil
}

// DescribeDBInstances returns instances. If id is non-empty, returns only that instance.
func (b *InMemoryBackend) DescribeDBInstances(id string) ([]DBInstance, error) {
	b.mu.RLock("DescribeDBInstances")
	defer b.mu.RUnlock()

	if id != "" {
		inst, exists := b.instances[id]
		if !exists {
			return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
		}

		return []DBInstance{*inst}, nil
	}

	instances := make([]DBInstance, 0, len(b.instances))
	for _, inst := range b.instances {
		instances = append(instances, *inst)
	}

	return instances, nil
}

// ModifyDBInstance modifies properties of an existing DB instance.
func (b *InMemoryBackend) ModifyDBInstance(
	id, instanceClass string,
	allocatedStorage int,
	opts DBInstanceOptions,
) (*DBInstance, error) {
	b.mu.Lock("ModifyDBInstance")
	defer b.mu.Unlock()

	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	if instanceClass != "" {
		inst.DBInstanceClass = instanceClass
	}
	if allocatedStorage > 0 {
		inst.AllocatedStorage = allocatedStorage
	}
	if opts.StorageType != "" {
		inst.StorageType = opts.StorageType
	}
	if opts.BackupRetentionPeriod >= 0 {
		inst.BackupRetentionPeriod = opts.BackupRetentionPeriod
	}
	if opts.MultiAZ {
		inst.MultiAZ = opts.MultiAZ
	}
	if opts.IAMDatabaseAuthenticationEnabled {
		inst.IAMDatabaseAuthenticationEnabled = opts.IAMDatabaseAuthenticationEnabled
	}
	if opts.DeletionProtection {
		inst.DeletionProtection = opts.DeletionProtection
	}

	cp := *inst

	return &cp, nil
}

// CreateDBSnapshot creates a snapshot of the given DB instance.
func (b *InMemoryBackend) CreateDBSnapshot(snapshotID, instanceID string) (*DBSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBSnapshotIdentifier is required", ErrInvalidParameter)
	}

	if instanceID == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDBSnapshot")
	defer b.mu.Unlock()

	if _, exists := b.snapshots[snapshotID]; exists {
		return nil, fmt.Errorf("%w: snapshot %s already exists", ErrSnapshotAlreadyExists, snapshotID)
	}

	inst, exists := b.instances[instanceID]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	snap := &DBSnapshot{
		DBSnapshotIdentifier: snapshotID,
		DBInstanceIdentifier: instanceID,
		Engine:               inst.Engine,
		EngineVersion:        inst.EngineVersion,
		Status:               "available",
		AllocatedStorage:     inst.AllocatedStorage,
		Port:                 inst.Port,
		StorageType:          inst.StorageType,
		StorageEncrypted:     inst.StorageEncrypted,
	}
	b.snapshots[snapshotID] = snap

	cp := *snap

	return &cp, nil
}

// DescribeDBSnapshots returns snapshots. If snapshotID is non-empty, returns only that snapshot.
func (b *InMemoryBackend) DescribeDBSnapshots(snapshotID string) ([]DBSnapshot, error) {
	b.mu.RLock("DescribeDBSnapshots")
	defer b.mu.RUnlock()

	if snapshotID != "" {
		snap, exists := b.snapshots[snapshotID]
		if !exists {
			return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
		}

		return []DBSnapshot{*snap}, nil
	}

	snaps := make([]DBSnapshot, 0, len(b.snapshots))
	for _, snap := range b.snapshots {
		snaps = append(snaps, *snap)
	}

	return snaps, nil
}

// DeleteDBSnapshot removes the given snapshot.
func (b *InMemoryBackend) DeleteDBSnapshot(snapshotID string) (*DBSnapshot, error) {
	b.mu.Lock("DeleteDBSnapshot")
	defer b.mu.Unlock()

	snap, exists := b.snapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	cp := *snap
	delete(b.snapshots, snapshotID)
	delete(b.tags, b.rdsARN("snapshot", snapshotID))

	return &cp, nil
}

// CopyDBSnapshot creates a copy of the given snapshot with a new identifier.
func (b *InMemoryBackend) CopyDBSnapshot(sourceSnapshotID, targetSnapshotID string) (*DBSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceDBSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: TargetDBSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("CopyDBSnapshot")
	defer b.mu.Unlock()

	src, exists := b.snapshots[sourceSnapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, sourceSnapshotID)
	}
	if _, alreadyExists := b.snapshots[targetSnapshotID]; alreadyExists {
		return nil, fmt.Errorf("%w: snapshot %s already exists", ErrSnapshotAlreadyExists, targetSnapshotID)
	}

	snap := &DBSnapshot{
		DBSnapshotIdentifier: targetSnapshotID,
		DBInstanceIdentifier: src.DBInstanceIdentifier,
		Engine:               src.Engine,
		EngineVersion:        src.EngineVersion,
		Status:               "available",
		AllocatedStorage:     src.AllocatedStorage,
		Port:                 src.Port,
		StorageType:          src.StorageType,
		StorageEncrypted:     src.StorageEncrypted,
	}
	b.snapshots[targetSnapshotID] = snap
	cp := *snap

	return &cp, nil
}

// RestoreDBInstanceFromDBSnapshot creates a new DB instance from the given snapshot.
func (b *InMemoryBackend) RestoreDBInstanceFromDBSnapshot(
	id, snapshotID string,
	opts DBInstanceOptions,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBSnapshotIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreDBInstanceFromDBSnapshot")

	if _, exists := b.instances[id]; exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}

	snap, exists := b.snapshots[snapshotID]
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	if opts.StorageType == "" {
		opts.StorageType = snap.StorageType
	}
	if opts.AvailabilityZone == "" {
		opts.AvailabilityZone = b.region + "a"
	}

	endpoint := fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
	port := snap.Port
	if port == 0 {
		port = enginePort(snap.Engine)
	}

	inst := &DBInstance{
		DBInstanceIdentifier: id,
		DbiResourceID:        id,
		Engine:               snap.Engine,
		EngineVersion:        snap.EngineVersion,
		DBInstanceStatus:     instanceStatusAvailable,
		Endpoint:             endpoint,
		Port:                 port,
		AllocatedStorage:     snap.AllocatedStorage,
		StorageType:          opts.StorageType,
		StorageEncrypted:     snap.StorageEncrypted,
		AvailabilityZone:     opts.AvailabilityZone,
		MultiAZ:              opts.MultiAZ,
		DeletionProtection:   opts.DeletionProtection,
	}
	b.instances[id] = inst
	cp := *inst

	b.mu.Unlock()

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	return &cp, nil
}

// RestoreDBInstanceToPointInTime creates a new DB instance as a point-in-time restore of the source.
func (b *InMemoryBackend) RestoreDBInstanceToPointInTime(
	id, sourceID string,
	opts DBInstanceOptions,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: TargetDBInstanceIdentifier is required", ErrInvalidParameter)
	}
	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceDBInstanceIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreDBInstanceToPointInTime")

	if _, exists := b.instances[id]; exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}

	source, exists := b.instances[sourceID]
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: source instance %s not found", ErrInstanceNotFound, sourceID)
	}

	if opts.StorageType == "" {
		opts.StorageType = source.StorageType
	}
	if opts.AvailabilityZone == "" {
		opts.AvailabilityZone = source.AvailabilityZone
	}

	endpoint := fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
	inst := &DBInstance{
		DBInstanceIdentifier: id,
		DbiResourceID:        id,
		DBInstanceClass:      source.DBInstanceClass,
		Engine:               source.Engine,
		EngineVersion:        source.EngineVersion,
		DBInstanceStatus:     instanceStatusAvailable,
		MasterUsername:       source.MasterUsername,
		DBName:               source.DBName,
		Endpoint:             endpoint,
		Port:                 source.Port,
		AllocatedStorage:     source.AllocatedStorage,
		DBParameterGroupName: source.DBParameterGroupName,
		StorageType:          opts.StorageType,
		StorageEncrypted:     source.StorageEncrypted,
		AvailabilityZone:     opts.AvailabilityZone,
		MultiAZ:              opts.MultiAZ,
		DeletionProtection:   opts.DeletionProtection,
	}
	b.instances[id] = inst
	cp := *inst

	b.mu.Unlock()

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	return &cp, nil
}

// StartDBInstance starts a stopped DB instance.
func (b *InMemoryBackend) StartDBInstance(id string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("StartDBInstance")
	defer b.mu.Unlock()

	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	if inst.DBInstanceStatus != instanceStatusStopped {
		return nil, fmt.Errorf("%w: instance %s is not in stopped state", ErrInvalidDBInstanceState, id)
	}

	inst.DBInstanceStatus = instanceStatusAvailable
	cp := *inst

	return &cp, nil
}

// StopDBInstance stops a running DB instance.
func (b *InMemoryBackend) StopDBInstance(id string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("StopDBInstance")
	defer b.mu.Unlock()

	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	if inst.DBInstanceStatus != instanceStatusAvailable {
		return nil, fmt.Errorf("%w: instance %s is not in available state", ErrInvalidDBInstanceState, id)
	}

	inst.DBInstanceStatus = instanceStatusStopped
	cp := *inst

	return &cp, nil
}

// CreateDBSubnetGroup creates a DB subnet group.
func (b *InMemoryBackend) CreateDBSubnetGroup(
	name, description, vpcID string,
	subnetIDs []string,
) (*DBSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBSubnetGroupName must not be empty", ErrInvalidParameter)
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
		SubnetIDs:                ids,
		Status:                   "Complete",
	}
	b.subnetGroups[name] = sg

	cp := *sg
	cp.SubnetIDs = make([]string, len(ids))
	copy(cp.SubnetIDs, ids)

	return &cp, nil
}

// DescribeDBSubnetGroups returns subnet groups. If name is non-empty, returns only that group.
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

	sgs := make([]DBSubnetGroup, 0, len(b.subnetGroups))

	for _, sg := range b.subnetGroups {
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)
		sgs = append(sgs, cp)
	}

	return sgs, nil
}

// DeleteDBSubnetGroup removes the given subnet group.
func (b *InMemoryBackend) DeleteDBSubnetGroup(name string) error {
	b.mu.Lock("DeleteDBSubnetGroup")
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[name]; !exists {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}

	delete(b.subnetGroups, name)
	delete(b.tags, b.rdsARN("subgrp", name))

	return nil
}

// AddTagsToResource adds or overwrites tags on the resource identified by arn.
func (b *InMemoryBackend) AddTagsToResource(arn string, tags []Tag) {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	current := b.tags[arn]
	// Build an index for O(1) key lookup.
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

// RemoveTagsFromResource removes the named tags from the resource identified by arn.
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

// ListTagsForResource returns the tags for the resource identified by arn.
func (b *InMemoryBackend) ListTagsForResource(arn string) []Tag {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	src := b.tags[arn]
	cp := make([]Tag, len(src))
	copy(cp, src)

	return cp
}

// CreateDBParameterGroup creates a new DB parameter group.
func (b *InMemoryBackend) CreateDBParameterGroup(name, family, description string) (*DBParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBParameterGroupName must not be empty", ErrInvalidParameter)
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
		Parameters:             make(map[string]DBParameter),
	}
	b.parameterGroups[name] = pg
	cp := *pg
	cp.Parameters = make(map[string]DBParameter)

	return &cp, nil
}

// copyDBParameterGroup returns a deep copy of the given parameter group.
func copyDBParameterGroup(pg *DBParameterGroup) DBParameterGroup {
	cp := *pg
	cp.Parameters = make(map[string]DBParameter, len(pg.Parameters))
	maps.Copy(cp.Parameters, pg.Parameters)

	return cp
}

// DescribeDBParameterGroups returns parameter groups. If name is non-empty, returns only that group.
func (b *InMemoryBackend) DescribeDBParameterGroups(name string) ([]DBParameterGroup, error) {
	b.mu.RLock("DescribeDBParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.parameterGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
		}

		return []DBParameterGroup{copyDBParameterGroup(pg)}, nil
	}
	result := make([]DBParameterGroup, 0, len(b.parameterGroups))
	for _, pg := range b.parameterGroups {
		result = append(result, copyDBParameterGroup(pg))
	}

	return result, nil
}

// DeleteDBParameterGroup removes the given parameter group.
func (b *InMemoryBackend) DeleteDBParameterGroup(name string) error {
	b.mu.Lock("DeleteDBParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.parameterGroups[name]; !exists {
		return fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	delete(b.parameterGroups, name)
	delete(b.tags, b.rdsARN("pg", name))

	return nil
}

// ModifyDBParameterGroup modifies parameters in a parameter group.
func (b *InMemoryBackend) ModifyDBParameterGroup(name string, params []DBParameter) (*DBParameterGroup, error) {
	b.mu.Lock("ModifyDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	for _, p := range params {
		pg.Parameters[p.ParameterName] = p
	}
	cp := copyDBParameterGroup(pg)

	return &cp, nil
}

// DescribeDBParameters returns parameters for a parameter group.
func (b *InMemoryBackend) DescribeDBParameters(groupName string) ([]DBParameter, error) {
	b.mu.RLock("DescribeDBParameters")
	defer b.mu.RUnlock()
	pg, exists := b.parameterGroups[groupName]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, groupName)
	}
	result := make([]DBParameter, 0, len(pg.Parameters))
	for _, p := range pg.Parameters {
		result = append(result, p)
	}

	return result, nil
}

// ResetDBParameterGroup resets parameters in a parameter group.
func (b *InMemoryBackend) ResetDBParameterGroup(
	name string,
	resetAll bool,
	params []string,
) (*DBParameterGroup, error) {
	b.mu.Lock("ResetDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	if resetAll {
		for k, p := range pg.Parameters {
			p.ParameterValue = ""
			pg.Parameters[k] = p
		}
	} else {
		for _, pName := range params {
			if p, ok := pg.Parameters[pName]; ok {
				p.ParameterValue = ""
				pg.Parameters[pName] = p
			}
		}
	}
	cp := copyDBParameterGroup(pg)

	return &cp, nil
}

// CreateOptionGroup creates a new option group.
func (b *InMemoryBackend) CreateOptionGroup(name, engine, majorVersion, description string) (*OptionGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: OptionGroupName must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateOptionGroup")
	defer b.mu.Unlock()
	if _, exists := b.optionGroups[name]; exists {
		return nil, fmt.Errorf("%w: option group %s already exists", ErrOptionGroupAlreadyExists, name)
	}
	og := &OptionGroup{
		OptionGroupName:        name,
		OptionGroupDescription: description,
		EngineName:             engine,
		MajorEngineVersion:     majorVersion,
		Options:                []OptionGroupOption{},
	}
	b.optionGroups[name] = og
	cp := *og
	cp.Options = make([]OptionGroupOption, len(og.Options))
	copy(cp.Options, og.Options)

	return &cp, nil
}

// DescribeOptionGroups returns option groups. If name is non-empty, returns only that group.
func (b *InMemoryBackend) DescribeOptionGroups(name string) ([]OptionGroup, error) {
	b.mu.RLock("DescribeOptionGroups")
	defer b.mu.RUnlock()
	if name != "" {
		og, exists := b.optionGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, name)
		}
		cp := *og
		cp.Options = make([]OptionGroupOption, len(og.Options))
		copy(cp.Options, og.Options)

		return []OptionGroup{cp}, nil
	}
	result := make([]OptionGroup, 0, len(b.optionGroups))
	for _, og := range b.optionGroups {
		cp := *og
		cp.Options = make([]OptionGroupOption, len(og.Options))
		copy(cp.Options, og.Options)
		result = append(result, cp)
	}

	return result, nil
}

// DeleteOptionGroup removes the given option group.
func (b *InMemoryBackend) DeleteOptionGroup(name string) error {
	b.mu.Lock("DeleteOptionGroup")
	defer b.mu.Unlock()
	if _, exists := b.optionGroups[name]; !exists {
		return fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, name)
	}
	delete(b.optionGroups, name)
	delete(b.tags, b.rdsARN("og", name))

	return nil
}

// ModifyOptionGroup modifies an option group by adding/removing options.
func (b *InMemoryBackend) ModifyOptionGroup(
	name string,
	optionsToAdd []OptionGroupOption,
	optionsToRemove []string,
) (*OptionGroup, error) {
	b.mu.Lock("ModifyOptionGroup")
	defer b.mu.Unlock()
	og, exists := b.optionGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, name)
	}
	removeSet := make(map[string]bool, len(optionsToRemove))
	for _, o := range optionsToRemove {
		removeSet[o] = true
	}
	kept := make([]OptionGroupOption, 0, len(og.Options))
	for _, o := range og.Options {
		if !removeSet[o.OptionName] {
			kept = append(kept, o)
		}
	}
	kept = append(kept, optionsToAdd...)
	og.Options = kept
	cp := *og
	cp.Options = make([]OptionGroupOption, len(og.Options))
	copy(cp.Options, og.Options)

	return &cp, nil
}

// CreateDBCluster creates a new DB cluster.
func (b *InMemoryBackend) CreateDBCluster(
	id, engine, masterUser, dbName, paramGroupName string,
	port int,
) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBCluster")
	defer b.mu.Unlock()
	if _, exists := b.clusters[id]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}
	if engine == "" {
		engine = "aurora-postgresql"
	}
	if paramGroupName == "" {
		paramGroupName = "default." + engine
	}
	if port <= 0 {
		port = enginePort(engine)
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
	cluster := &DBCluster{
		DBClusterIdentifier:         id,
		Engine:                      engine,
		Status:                      "available",
		MasterUsername:              masterUser,
		DatabaseName:                dbName,
		DBClusterParameterGroupName: paramGroupName,
		Endpoint:                    endpoint,
		Port:                        port,
	}
	b.clusters[id] = cluster
	cp := *cluster

	return &cp, nil
}

// DescribeDBClusters returns clusters. If id is non-empty, returns only that cluster.
func (b *InMemoryBackend) DescribeDBClusters(id string) ([]DBCluster, error) {
	b.mu.RLock("DescribeDBClusters")
	defer b.mu.RUnlock()
	if id != "" {
		cluster, exists := b.clusters[id]
		if !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}
		cp := *cluster

		return []DBCluster{cp}, nil
	}
	result := make([]DBCluster, 0, len(b.clusters))
	for _, cluster := range b.clusters {
		result = append(result, *cluster)
	}

	return result, nil
}

// DeleteDBCluster removes the given cluster.
func (b *InMemoryBackend) DeleteDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("DeleteDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cp := *cluster
	delete(b.clusters, id)
	delete(b.tags, b.rdsARN("cluster", id))
	delete(b.fisFailoverFaults, id)

	return &cp, nil
}

// ModifyDBCluster modifies a DB cluster.
func (b *InMemoryBackend) ModifyDBCluster(id, paramGroupName string) (*DBCluster, error) {
	b.mu.Lock("ModifyDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if paramGroupName != "" {
		cluster.DBClusterParameterGroupName = paramGroupName
	}
	cp := *cluster

	return &cp, nil
}

// CreateDBClusterParameterGroup creates a new cluster parameter group.
func (b *InMemoryBackend) CreateDBClusterParameterGroup(name, family, description string) (*DBParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.clusterParameterGroups[name]; exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s already exists", ErrParameterGroupAlreadyExists, name)
	}
	pg := &DBParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupFamily: family,
		Description:            description,
		Parameters:             make(map[string]DBParameter),
	}
	b.clusterParameterGroups[name] = pg
	cp := *pg
	cp.Parameters = make(map[string]DBParameter)

	return &cp, nil
}

// DescribeDBClusterParameterGroups returns cluster parameter groups.
func (b *InMemoryBackend) DescribeDBClusterParameterGroups(name string) ([]DBParameterGroup, error) {
	b.mu.RLock("DescribeDBClusterParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.clusterParameterGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrParameterGroupNotFound, name)
		}

		return []DBParameterGroup{copyDBParameterGroup(pg)}, nil
	}
	result := make([]DBParameterGroup, 0, len(b.clusterParameterGroups))
	for _, pg := range b.clusterParameterGroups {
		result = append(result, copyDBParameterGroup(pg))
	}

	return result, nil
}

// CreateDBClusterSnapshot creates a snapshot of the given cluster.
func (b *InMemoryBackend) CreateDBClusterSnapshot(snapshotID, clusterID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterSnapshot")
	defer b.mu.Unlock()
	if _, exists := b.clusterSnapshots[snapshotID]; exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s already exists", ErrClusterSnapshotAlreadyExists, snapshotID)
	}
	cluster, exists := b.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterIdentifier:         clusterID,
		Engine:                      cluster.Engine,
		Status:                      "available",
	}
	b.clusterSnapshots[snapshotID] = snap
	cp := *snap

	return &cp, nil
}

// DescribeDBClusterSnapshots returns cluster snapshots.
func (b *InMemoryBackend) DescribeDBClusterSnapshots(snapshotID string) ([]DBClusterSnapshot, error) {
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
		result = append(result, *snap)
	}

	return result, nil
}

// CreateDBInstanceReadReplica creates a read replica of the given source instance.
func (b *InMemoryBackend) CreateDBInstanceReadReplica(id, sourceID string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBInstanceReadReplica")
	defer b.mu.Unlock()
	if _, exists := b.instances[id]; exists {
		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}
	source, exists := b.instances[sourceID]
	if !exists {
		return nil, fmt.Errorf("%w: source instance %s not found", ErrInstanceNotFound, sourceID)
	}
	port := source.Port
	endpoint := fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
	replica := &DBInstance{
		DBInstanceIdentifier:              id,
		DbiResourceID:                     id,
		DBInstanceClass:                   source.DBInstanceClass,
		Engine:                            source.Engine,
		DBInstanceStatus:                  instanceStatusAvailable,
		MasterUsername:                    source.MasterUsername,
		Endpoint:                          endpoint,
		Port:                              port,
		AllocatedStorage:                  source.AllocatedStorage,
		ReplicaSourceDBInstanceIdentifier: sourceID,
	}
	b.instances[id] = replica
	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}
	cp := *replica

	return &cp, nil
}

// PromoteReadReplica promotes a read replica to a standalone instance.
func (b *InMemoryBackend) PromoteReadReplica(id string) (*DBInstance, error) {
	b.mu.Lock("PromoteReadReplica")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	inst.ReplicaSourceDBInstanceIdentifier = ""
	cp := *inst

	return &cp, nil
}

// RebootDBInstance reboots the given instance.
func (b *InMemoryBackend) RebootDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("RebootDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	inst.DBInstanceStatus = instanceStatusAvailable
	cp := *inst

	return &cp, nil
}

// DescribeDBEngineVersions returns available engine versions, filtered by engine and/or version.
func (b *InMemoryBackend) DescribeDBEngineVersions(engine, engineVersion string) []DBEngineVersion {
	all := []DBEngineVersion{
		{Engine: "postgres", EngineVersion: "14.10", DBEngineDescription: "PostgreSQL 14.10"},
		{Engine: "postgres", EngineVersion: "15.5", DBEngineDescription: "PostgreSQL 15.5"},
		{Engine: "mysql", EngineVersion: "8.0.35", DBEngineDescription: "MySQL 8.0.35"},
		{Engine: "mariadb", EngineVersion: "10.6.14", DBEngineDescription: "MariaDB 10.6.14"},
		{Engine: "aurora-mysql", EngineVersion: "3.04.0", DBEngineDescription: "Aurora MySQL 3.04.0"},
		{Engine: "aurora-postgresql", EngineVersion: "14.9", DBEngineDescription: "Aurora PostgreSQL 14.9"},
		{Engine: "aurora-postgresql", EngineVersion: "15.4", DBEngineDescription: "Aurora PostgreSQL 15.4"},
	}
	if engine == "" && engineVersion == "" {
		return all
	}
	result := make([]DBEngineVersion, 0)
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
	classes := []string{"db.t3.micro", "db.t3.small", "db.t3.medium", "db.r5.large", "db.r5.xlarge"}
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

// DescribeDBLogFiles returns the log files for the given instance.
func (b *InMemoryBackend) DescribeDBLogFiles(instanceID string) ([]DBLogFile, error) {
	b.mu.RLock("DescribeDBLogFiles")
	defer b.mu.RUnlock()
	if _, exists := b.instances[instanceID]; !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	return []DBLogFile{}, nil
}

// DownloadDBLogFilePortion returns log file content for the given instance.
func (b *InMemoryBackend) DownloadDBLogFilePortion(instanceID, _ string) (string, error) {
	b.mu.RLock("DownloadDBLogFilePortion")
	defer b.mu.RUnlock()
	if _, exists := b.instances[instanceID]; !exists {
		return "", fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	return "", nil
}

// StartDBCluster starts a stopped DB cluster.
func (b *InMemoryBackend) StartDBCluster(id string) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("StartDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cluster.Status = "available"
	cp := *cluster

	return &cp, nil
}

// StopDBCluster stops a running DB cluster.
func (b *InMemoryBackend) StopDBCluster(id string) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("StopDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cluster.Status = "stopped"
	cp := *cluster

	return &cp, nil
}

// DeleteDBClusterSnapshot removes the given cluster snapshot.
func (b *InMemoryBackend) DeleteDBClusterSnapshot(snapshotID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	cp := *snap
	delete(b.clusterSnapshots, snapshotID)
	delete(b.tags, b.rdsARN("cluster-snapshot", snapshotID))

	return &cp, nil
}

// RestoreDBClusterFromSnapshot creates a new DB cluster from the given snapshot.
func (b *InMemoryBackend) RestoreDBClusterFromSnapshot(clusterID, snapshotID, engine string) (*DBCluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: SnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("RestoreDBClusterFromSnapshot")
	defer b.mu.Unlock()
	if _, exists := b.clusters[clusterID]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, clusterID)
	}
	snap, exists := b.clusterSnapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	if engine == "" {
		engine = snap.Engine
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.%s.rds.amazonaws.com", clusterID, b.accountID, b.region)
	cluster := &DBCluster{
		DBClusterIdentifier:         clusterID,
		Engine:                      engine,
		Status:                      "available",
		DBClusterParameterGroupName: "default." + engine,
		Endpoint:                    endpoint,
		Port:                        enginePort(engine),
	}
	b.clusters[clusterID] = cluster
	cp := *cluster

	return &cp, nil
}

// RestoreDBClusterToPointInTime creates a new DB cluster as a point-in-time restore of the source cluster.
func (b *InMemoryBackend) RestoreDBClusterToPointInTime(clusterID, sourceClusterID string) (*DBCluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	if sourceClusterID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("RestoreDBClusterToPointInTime")
	defer b.mu.Unlock()
	if _, exists := b.clusters[clusterID]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, clusterID)
	}
	source, exists := b.clusters[sourceClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: source cluster %s not found", ErrClusterNotFound, sourceClusterID)
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.%s.rds.amazonaws.com", clusterID, b.accountID, b.region)
	cluster := &DBCluster{
		DBClusterIdentifier:         clusterID,
		Engine:                      source.Engine,
		Status:                      "available",
		MasterUsername:              source.MasterUsername,
		DatabaseName:                source.DatabaseName,
		DBClusterParameterGroupName: source.DBClusterParameterGroupName,
		Endpoint:                    endpoint,
		Port:                        source.Port,
	}
	b.clusters[clusterID] = cluster
	cp := *cluster

	return &cp, nil
}

// CopyDBClusterSnapshot creates a copy of the given cluster snapshot.
func (b *InMemoryBackend) CopyDBClusterSnapshot(sourceSnapshotID, targetSnapshotID string) (*DBClusterSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CopyDBClusterSnapshot")
	defer b.mu.Unlock()
	source, srcExists := b.clusterSnapshots[sourceSnapshotID]
	if !srcExists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, sourceSnapshotID)
	}
	if _, dstExists := b.clusterSnapshots[targetSnapshotID]; dstExists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			targetSnapshotID,
		)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: targetSnapshotID,
		DBClusterIdentifier:         source.DBClusterIdentifier,
		Engine:                      source.Engine,
		Status:                      "available",
	}
	b.clusterSnapshots[targetSnapshotID] = snap
	cp := *snap

	return &cp, nil
}

// CreateDBClusterEndpoint creates a custom endpoint for the given cluster.
func (b *InMemoryBackend) CreateDBClusterEndpoint(
	endpointID, clusterID, endpointType string,
) (*DBClusterEndpoint, error) {
	if endpointID == "" {
		return nil, fmt.Errorf("%w: DBClusterEndpointIdentifier must not be empty", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier must not be empty", ErrInvalidParameter)
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
		endpointType = "ANY"
	}
	ep := &DBClusterEndpoint{
		DBClusterEndpointIdentifier: endpointID,
		DBClusterIdentifier:         clusterID,
		EndpointType:                endpointType,
		Status:                      "available",
		Endpoint: fmt.Sprintf(
			"%s.cluster-custom.%s.%s.rds.amazonaws.com",
			endpointID,
			b.accountID,
			b.region,
		),
	}
	b.clusterEndpoints[endpointID] = ep
	cp := *ep

	return &cp, nil
}

// DescribeDBClusterEndpoints returns cluster endpoints, filtered by cluster or endpoint ID.
func (b *InMemoryBackend) DescribeDBClusterEndpoints(clusterID, endpointID string) ([]DBClusterEndpoint, error) {
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
	result := make([]DBClusterEndpoint, 0)
	for _, ep := range b.clusterEndpoints {
		if clusterID != "" && ep.DBClusterIdentifier != clusterID {
			continue
		}
		result = append(result, *ep)
	}

	return result, nil
}

// DeleteDBClusterEndpoint removes the given custom cluster endpoint.
func (b *InMemoryBackend) DeleteDBClusterEndpoint(endpointID string) (*DBClusterEndpoint, error) {
	b.mu.Lock("DeleteDBClusterEndpoint")
	defer b.mu.Unlock()
	ep, exists := b.clusterEndpoints[endpointID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster endpoint %s not found", ErrClusterEndpointNotFound, endpointID)
	}
	cp := *ep
	delete(b.clusterEndpoints, endpointID)
	delete(b.tags, b.rdsARN("cluster-endpoint", endpointID))

	return &cp, nil
}

// DescribeValidDBInstanceModifications returns the valid modifications for the given instance.
// This is a stub that returns a minimal set of valid instance classes.
func (b *InMemoryBackend) DescribeValidDBInstanceModifications(id string) (*DBInstance, error) {
	b.mu.RLock("DescribeValidDBInstanceModifications")
	defer b.mu.RUnlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := *inst

	return &cp, nil
}

// StartExportTask creates a new export task for the given source ARN.
func (b *InMemoryBackend) StartExportTask(taskID, sourceARN, s3Bucket string) (*ExportTask, error) {
	if taskID == "" {
		return nil, fmt.Errorf("%w: ExportTaskIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("StartExportTask")
	defer b.mu.Unlock()
	if _, exists := b.exportTasks[taskID]; exists {
		return nil, fmt.Errorf("%w: export task %s already exists", ErrExportTaskAlreadyExists, taskID)
	}
	task := &ExportTask{
		ExportTaskIdentifier: taskID,
		SourceArn:            sourceARN,
		Status:               "complete",
		S3Bucket:             s3Bucket,
	}
	b.exportTasks[taskID] = task
	cp := *task

	return &cp, nil
}

// DescribeExportTasks returns export tasks, optionally filtered by task ID.
func (b *InMemoryBackend) DescribeExportTasks(taskID string) ([]ExportTask, error) {
	b.mu.RLock("DescribeExportTasks")
	defer b.mu.RUnlock()
	if taskID != "" {
		task, exists := b.exportTasks[taskID]
		if !exists {
			return nil, fmt.Errorf("%w: export task %s not found", ErrExportTaskNotFound, taskID)
		}
		cp := *task

		return []ExportTask{cp}, nil
	}
	result := make([]ExportTask, 0, len(b.exportTasks))
	for _, task := range b.exportTasks {
		result = append(result, *task)
	}

	return result, nil
}

// CancelExportTask cancels and removes the export task with the given identifier.
func (b *InMemoryBackend) CancelExportTask(taskID string) (*ExportTask, error) {
	if taskID == "" {
		return nil, fmt.Errorf("%w: ExportTaskIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CancelExportTask")
	defer b.mu.Unlock()
	task, exists := b.exportTasks[taskID]
	if !exists {
		return nil, fmt.Errorf("%w: export task %s not found", ErrExportTaskNotFound, taskID)
	}
	task.Status = "canceled"
	cp := *task
	delete(b.exportTasks, taskID)

	return &cp, nil
}

// CreateGlobalCluster creates a new global cluster.
func (b *InMemoryBackend) CreateGlobalCluster(
	id, engine, engineVersion string,
	storageEncrypted, deletionProtection bool,
) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("CreateGlobalCluster")
	defer b.mu.Unlock()

	if _, exists := b.globalClusters[id]; exists {
		return nil, fmt.Errorf("%w: global cluster %s already exists", ErrGlobalClusterAlreadyExists, id)
	}

	if engine == "" {
		engine = "aurora-postgresql"
	}

	gc := &GlobalCluster{
		GlobalClusterIdentifier: id,
		Engine:                  engine,
		EngineVersion:           engineVersion,
		Status:                  "available",
		StorageEncrypted:        storageEncrypted,
		DeletionProtection:      deletionProtection,
	}
	b.globalClusters[id] = gc
	cp := *gc

	return &cp, nil
}

// DescribeGlobalClusters returns global clusters, optionally filtered by identifier.
func (b *InMemoryBackend) DescribeGlobalClusters(id string) ([]GlobalCluster, error) {
	b.mu.RLock("DescribeGlobalClusters")
	defer b.mu.RUnlock()

	if id != "" {
		gc, exists := b.globalClusters[id]
		if !exists {
			return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
		}
		cp := *gc

		return []GlobalCluster{cp}, nil
	}

	result := make([]GlobalCluster, 0, len(b.globalClusters))
	for _, gc := range b.globalClusters {
		result = append(result, *gc)
	}

	return result, nil
}

// DeleteGlobalCluster removes the given global cluster.
func (b *InMemoryBackend) DeleteGlobalCluster(id string) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteGlobalCluster")
	defer b.mu.Unlock()

	gc, exists := b.globalClusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}

	cp := *gc
	delete(b.globalClusters, id)

	return &cp, nil
}

// ModifyGlobalCluster modifies properties of a global cluster.
func (b *InMemoryBackend) ModifyGlobalCluster(
	id, newGlobalClusterID, engineVersion string,
	deletionProtection *bool,
) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyGlobalCluster")
	defer b.mu.Unlock()

	gc, exists := b.globalClusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}

	if newGlobalClusterID != "" && newGlobalClusterID != id {
		if _, alreadyExists := b.globalClusters[newGlobalClusterID]; alreadyExists {
			return nil, fmt.Errorf(
				"%w: global cluster %s already exists",
				ErrGlobalClusterAlreadyExists,
				newGlobalClusterID,
			)
		}
		delete(b.globalClusters, id)
		gc.GlobalClusterIdentifier = newGlobalClusterID
		b.globalClusters[newGlobalClusterID] = gc
	}
	if engineVersion != "" {
		gc.EngineVersion = engineVersion
	}
	if deletionProtection != nil {
		gc.DeletionProtection = *deletionProtection
	}

	cp := *gc

	return &cp, nil
}
