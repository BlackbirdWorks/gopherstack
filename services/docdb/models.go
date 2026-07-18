package docdb

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	defaultDocDBPort           = 27017
	defaultInstanceClass       = "db.t3.medium"
	defaultEngineVersion       = "4.0.0"
	docDBEngineVersion36       = "3.6.0"
	docDBEngineVersion5        = "5.0.0"
	docDBEngine                = "docdb"
	snapshotPercentageComplete = 100

	paramEnabled   = "enabled"
	paramTypeStr   = "string"
	eventCatBackup = "backup"
	eventCatCreate = "creation"
	eventCatDelete = "deletion"
	eventCatNotify = "notification"

	defaultBackupWindow      = "00:00-01:00" //nolint:gosec // not a credential
	defaultMaintenanceWindow = "mon:05:00-mon:05:30"

	statusAvailable = "available"
	statusStopped   = "stopped"
	statusDeleting  = "deleting"

	// OptInType constants for ApplyPendingMaintenanceAction.
	optInTypeImmediate       = "immediate"
	optInTypeNextMaintenance = "next-maintenance"
	optInTypeUndoOptIn       = "undo-opt-in"

	maxTagCount    = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256

	maxPromotionTier         = 15
	maxBackupRetentionPeriod = 35

	docDBEngineDescription = "Amazon DocumentDB"
)

var validDocDBVersions = map[string]bool{ //nolint:gochecknoglobals // compile-time constant set
	docDBEngineVersion36: true,
	defaultEngineVersion: true,
	docDBEngineVersion5:  true,
}

// defaultParamGroupName returns the default parameter group name for a given engine version.
func defaultParamGroupName(engineVersion string) string {
	switch engineVersion {
	case docDBEngineVersion36:
		return "default.docdb3.6"
	case docDBEngineVersion5:
		return "default.docdb5.0"
	default:
		return "default.docdb4.0"
	}
}

// validateEngineVersion returns an error if engineVersion is non-empty and not a valid DocDB version.
func validateEngineVersion(engineVersion string) error {
	if engineVersion == "" {
		return nil
	}
	if !validDocDBVersions[engineVersion] {
		return fmt.Errorf(
			"%w: EngineVersion %q is not valid for engine %q; valid values: 3.6.0, 4.0.0, 5.0.0",
			ErrInvalidParameter, engineVersion, docDBEngine,
		)
	}

	return nil
}

// validateMasterUserPassword validates per AWS rules: 8-100 chars, no '/', '"', or '@'.
func validateMasterUserPassword(pw string) error {
	if len(pw) < 8 || len(pw) > 100 {
		return fmt.Errorf("%w: MasterUserPassword must be between 8 and 100 characters", ErrInvalidParameter)
	}
	if strings.ContainsAny(pw, "/\"@") {
		return fmt.Errorf("%w: MasterUserPassword must not contain '/', '\"', or '@'", ErrInvalidParameter)
	}

	return nil
}

// validateTags enforces AWS tag limits: key 1-128 chars, value 0-256 chars, max 50 tags.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: cannot specify more than %d tags per resource", ErrInvalidParameter, maxTagCount)
	}
	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be between 1 and %d characters", ErrInvalidParameter, maxTagKeyLen)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be at most %d characters", ErrInvalidParameter, maxTagValueLen)
		}
	}

	return nil
}

// validateTagList enforces AWS tag limits on a slice of Tag.
func validateTagList(tags []Tag) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: cannot specify more than %d tags per resource", ErrInvalidParameter, maxTagCount)
	}
	for _, t := range tags {
		if len(t.Key) == 0 || len(t.Key) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be between 1 and %d characters", ErrInvalidParameter, maxTagKeyLen)
		}
		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be at most %d characters", ErrInvalidParameter, maxTagValueLen)
		}
	}

	return nil
}

type DBCluster struct {
	// region is the AWS region this cluster belongs to. It is the outer half
	// of the composite key ("region|DBClusterIdentifier") used by the
	// backend's flat store.Table[DBCluster] (see store_setup.go), which
	// replaces the old map[string]map[string]*DBCluster nesting (outer key =
	// region). Unexported so it never appears in DocDB wire responses (those
	// are built by copyCluster/hand-assembled describe results, never by
	// marshaling DBCluster directly), but persistence.go must carry it
	// through a DTO explicitly since json.Marshal never sees unexported fields.
	region                           string
	Tags                             map[string]string `json:"tags"`
	DBClusterArn                     string            `json:"dbClusterArn"`
	EngineVersion                    string            `json:"engineVersion"`
	Engine                           string            `json:"engine"`
	PreferredMaintenanceWindow       string            `json:"preferredMaintenanceWindow"`
	MasterUsername                   string            `json:"masterUsername"`
	DatabaseName                     string            `json:"databaseName"`
	DBClusterParameterGroupName      string            `json:"dbClusterParameterGroupName"`
	Endpoint                         string            `json:"endpoint"`
	DBClusterIdentifier              string            `json:"dbClusterIdentifier"`
	ReaderEndpoint                   string            `json:"readerEndpoint"`
	Status                           string            `json:"status"`
	DBSubnetGroupName                string            `json:"dbSubnetGroupName"`
	PreferredBackupWindow            string            `json:"preferredBackupWindow"`
	ClusterCreateTime                string            `json:"clusterCreateTime"`
	HostedZoneID                     string            `json:"hostedZoneId"`
	KmsKeyID                         string            `json:"kmsKeyId"`
	ReplicationSourceIdentifier      string            `json:"replicationSourceIdentifier"`
	AvailabilityZones                []string          `json:"availabilityZones"`
	VpcSecurityGroupIDs              []string          `json:"vpcSecurityGroupIds"`
	EnabledCloudwatchLogsExports     []string          `json:"enabledCloudwatchLogsExports"`
	ReadReplicaIdentifiers           []string          `json:"readReplicaIdentifiers"`
	Port                             int               `json:"port"`
	BackupRetentionPeriod            int               `json:"backupRetentionPeriod"`
	StorageEncrypted                 bool              `json:"storageEncrypted"`
	MultiAZ                          bool              `json:"multiAZ"`
	DeletionProtection               bool              `json:"deletionProtection"`
	IAMDatabaseAuthenticationEnabled bool              `json:"iamDatabaseAuthenticationEnabled"`
}

type DBInstance struct {
	// region is the AWS region this instance belongs to; see DBCluster.region
	// for the composite-key rationale (store_setup.go/persistence.go).
	region                       string
	Tags                         map[string]string `json:"tags"`
	DBInstanceIdentifier         string            `json:"dbInstanceIdentifier"`
	DBClusterIdentifier          string            `json:"dbClusterIdentifier"`
	DBInstanceClass              string            `json:"dbInstanceClass"`
	Engine                       string            `json:"engine"`
	DBInstanceStatus             string            `json:"dbInstanceStatus"`
	Endpoint                     string            `json:"endpoint"`
	DBInstanceArn                string            `json:"dbInstanceArn"`
	EngineVersion                string            `json:"engineVersion"`
	AvailabilityZone             string            `json:"availabilityZone"`
	DBSubnetGroupName            string            `json:"dbSubnetGroupName"`
	PreferredMaintenanceWindow   string            `json:"preferredMaintenanceWindow"`
	CACertificateIdentifier      string            `json:"caCertificateIdentifier"`
	EnabledCloudwatchLogsExports []string          `json:"enabledCloudwatchLogsExports"`
	Port                         int               `json:"port"`
	PromotionTier                int               `json:"promotionTier"`
	StorageEncrypted             bool              `json:"storageEncrypted"`
	AutoMinorVersionUpgrade      bool              `json:"autoMinorVersionUpgrade"`
	PubliclyAccessible           bool              `json:"publiclyAccessible"`
	CopyTagsToSnapshot           bool              `json:"copyTagsToSnapshot"`
}

type DBSubnetGroup struct {
	// region is the AWS region this subnet group belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                   string
	Tags                     map[string]string `json:"tags"`
	DBSubnetGroupName        string            `json:"dbSubnetGroupName"`
	DBSubnetGroupDescription string            `json:"dbSubnetGroupDescription"`
	VpcID                    string            `json:"vpcID"`
	Status                   string            `json:"status"`
	DBSubnetGroupArn         string            `json:"dbSubnetGroupArn"`
	SubnetIDs                []string          `json:"subnetIDs"`
}

type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DBClusterParameterGroup struct {
	// region is the AWS region this cluster parameter group belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                      string
	Tags                        map[string]string `json:"tags"`
	Parameters                  map[string]string `json:"parameters"`
	DBClusterParameterGroupName string            `json:"dbClusterParameterGroupName"`
	DBParameterGroupFamily      string            `json:"dbParameterGroupFamily"`
	Description                 string            `json:"description"`
	DBClusterParameterGroupArn  string            `json:"dbClusterParameterGroupArn"`
}

type DBClusterSnapshot struct {
	// region is the AWS region this cluster snapshot belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                      string
	Tags                        map[string]string `json:"tags"`
	DBClusterSnapshotIdentifier string            `json:"dbClusterSnapshotIdentifier"`
	DBClusterIdentifier         string            `json:"dbClusterIdentifier"`
	DBClusterArn                string            `json:"dbClusterArn"`
	Engine                      string            `json:"engine"`
	Status                      string            `json:"status"`
	EngineVersion               string            `json:"engineVersion"`
	SnapshotType                string            `json:"snapshotType"`
	SnapshotCreateTime          string            `json:"snapshotCreateTime"`
	PercentProgress             int               `json:"percentProgress"`
	StorageEncrypted            bool              `json:"storageEncrypted"`
}

type EventSubscription struct {
	// region is the AWS region this event subscription belongs to; see
	// DBCluster.region for the composite-key rationale.
	region           string
	SubscriptionName string   `json:"subscriptionName"`
	SnsTopicARN      string   `json:"snsTopicARN"`
	Status           string   `json:"status"`
	SourceType       string   `json:"sourceType"`
	SourceIDs        []string `json:"sourceIDs"`
	EventCategories  []string `json:"eventCategories"`
}

type GlobalCluster struct {
	GlobalClusterIdentifier string `json:"globalClusterIdentifier"`
	SourceDBClusterID       string `json:"sourceDBClusterID"`
	Status                  string `json:"status"`
	Engine                  string `json:"engine"`
	EngineVersion           string `json:"engineVersion"`
	GlobalClusterArn        string `json:"globalClusterArn"`
	StorageEncrypted        bool   `json:"storageEncrypted"`
	DeletionProtection      bool   `json:"deletionProtection"`
}

type Certificate struct {
	CertificateIdentifier string
	CertificateType       string
	Thumbprint            string
	ValidFrom             string
	ValidTill             string
}

type DBClusterParameter struct {
	ParameterName  string
	ParameterValue string
	Description    string
	Source         string
	ApplyType      string
	DataType       string
	IsModifiable   bool
}

// DBClusterSnapshotAttribute represents a single attribute of a cluster snapshot.
type DBClusterSnapshotAttribute struct {
	AttributeName   string   `json:"attributeName"`
	AttributeValues []string `json:"attributeValues"`
}

// DBClusterSnapshotAttributesResult holds the attributes for a cluster snapshot.
type DBClusterSnapshotAttributesResult struct {
	// region is the AWS region this snapshot attributes result belongs to; see
	// DBCluster.region for the composite-key rationale. Unlike the other six
	// region-qualified tables, this one has no byRegion index -- see the
	// package doc comment in store_setup.go.
	region                      string
	DBClusterSnapshotIdentifier string                       `json:"dbClusterSnapshotIdentifier"`
	Attributes                  []DBClusterSnapshotAttribute `json:"attributes"`
}

// ResourcePendingMaintenanceActions holds pending maintenance actions for a resource.
type ResourcePendingMaintenanceActions struct {
	ResourceIdentifier string
	Actions            []PendingMaintenanceAction
}

// PendingMaintenanceAction describes a pending maintenance action.
type PendingMaintenanceAction struct {
	Action      string
	OptInStatus string
}

// EventCategoryMap maps a source type to a list of event categories.
type EventCategoryMap struct {
	SourceType      string
	EventCategories []string
}

// InMemoryBackend is the in-memory store for DocDB resources.
//
// Seven resource collections that were previously nested by region (outer key
// = region, e.g. map[string]map[string]*DBCluster) are now each a single flat
// *store.Table keyed by the composite "region|id" string (see regionKey
// below), with a companion *store.Index grouping entries by region for
// per-region scans -- see store_setup.go for the full rationale. Six of the
// seven also have a byRegion index; snapshotAttributes does not (it is only
// ever looked up/written by its exact composite key, never listed by
// region). GlobalClusters are partition-scoped and remain a plain
// (non-composite-key) *store.Table. tags remains a raw nested map: its value
// (a bare []Tag) carries no identity of its own to key a store.Table by (see
// store_setup.go's doc comment for the full rationale).
type InMemoryBackend struct {
	registry                       *store.Registry
	clusters                       *store.Table[DBCluster]
	clustersByRegion               *store.Index[DBCluster]
	instances                      *store.Table[DBInstance]
	instancesByRegion              *store.Index[DBInstance]
	subnetGroups                   *store.Table[DBSubnetGroup]
	subnetGroupsByRegion           *store.Index[DBSubnetGroup]
	clusterParameterGroups         *store.Table[DBClusterParameterGroup]
	clusterParameterGroupsByRegion *store.Index[DBClusterParameterGroup]
	clusterSnapshots               *store.Table[DBClusterSnapshot]
	clusterSnapshotsByRegion       *store.Index[DBClusterSnapshot]
	eventSubscriptions             *store.Table[EventSubscription]
	eventSubscriptionsByRegion     *store.Index[EventSubscription]
	snapshotAttributes             *store.Table[DBClusterSnapshotAttributesResult] // no byRegion index; see doc comment
	globalClusters                 *store.Table[GlobalCluster]                     // global/partition-scoped
	tags                           map[string]map[string][]Tag
	mu                             *lockmetrics.RWMutex
	accountID                      string
	region                         string
}

// CreateDBClusterOptions holds optional parameters for CreateDBCluster.
type CreateDBClusterOptions struct {
	KmsKeyID                         string
	VpcSecurityGroupIDs              []string
	EnabledCloudwatchLogsExports     []string
	IAMDatabaseAuthenticationEnabled bool
}

// DeleteDBClusterOptions holds optional parameters for DeleteDBCluster.
type DeleteDBClusterOptions struct {
	FinalDBClusterSnapshotIdentifier string
	SkipFinalSnapshot                bool
}

// ModifyDBClusterOptions holds optional extra parameters for ModifyDBCluster.
type ModifyDBClusterOptions struct {
	EngineVersion          string
	MasterUserPassword     string
	NewDBClusterIdentifier string
	VpcSecurityGroupIDs    []string
	EnableLogsTypes        []string
	DisableLogsTypes       []string
	Port                   int
}

// CreateDBInstanceOptions holds optional parameters for CreateDBInstance.
type CreateDBInstanceOptions struct {
	CACertificateIdentifier string
	CopyTagsToSnapshot      bool
}

// DBClusterMemberEntry represents an instance that is a member of a DB cluster.
type DBClusterMemberEntry struct {
	DBInstanceIdentifier string
	PromotionTier        int
	IsClusterWriter      bool
}

// ModifyDBInstanceOptions holds optional extra parameters for ModifyDBInstance.
type ModifyDBInstanceOptions struct {
	CopyTagsToSnapshot      *bool
	PromotionTier           *int
	CACertificateIdentifier string
}

// DBEngineVersion represents a supported DocDB engine version.
type DBEngineVersion struct {
	Engine              string
	EngineVersion       string
	DBEngineDescription string
}
