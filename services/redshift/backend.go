package redshift

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// clusterIDRegex matches valid Redshift ClusterIdentifier values:
// begins with a letter, only lowercase letters/digits/hyphens, 1-63 chars.
var clusterIDRegex = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

const (
	errClusterSnapshotNotFound = "ClusterSnapshotNotFound"
)

var (
	ErrClusterNotFound                = errors.New("ClusterNotFound")
	ErrClusterAlreadyExists           = errors.New("ClusterAlreadyExists")
	ErrInvalidParameter               = errors.New("InvalidParameterValue")
	ErrReservedNodeNotFound           = errors.New("ReservedNodeNotFound")
	ErrReservedNodeAlreadyExists      = errors.New("ReservedNodeAlreadyExists")
	ErrReservedNodeOfferingNotFound   = errors.New("ReservedNodeOfferingNotFound")
	ErrPartnerNotFound                = errors.New("PartnerNotFound")
	ErrDataShareNotFound              = errors.New("DataShareNotFound")
	ErrSecurityGroupNotFound          = errors.New("ClusterSecurityGroupNotFound")
	ErrSecurityGroupAlreadyExists     = errors.New("ClusterSecurityGroupAlreadyExists")
	ErrSnapshotNotFound               = errors.New(errClusterSnapshotNotFound)
	ErrSnapshotAlreadyExists          = errors.New("ClusterSnapshotAlreadyExists")
	ErrEndpointAuthNotFound           = errors.New("EndpointAuthorizationNotFound")
	ErrEndpointAuthAlreadyExists      = errors.New("EndpointAuthorizationAlreadyExists")
	ErrResizeNotFound                 = errors.New("ResizeNotFound")
	ErrResizeNotCancellable           = errors.New("InvalidClusterState")
	ErrParameterGroupNotFound         = errors.New("ClusterParameterGroupNotFound")
	ErrParameterGroupAlreadyExists    = errors.New("ClusterParameterGroupAlreadyExists")
	ErrSubnetGroupNotFound            = errors.New("ClusterSubnetGroupNotFound")
	ErrSubnetGroupAlreadyExists       = errors.New("ClusterSubnetGroupAlreadyExists")
	ErrEventSubscriptionNotFound      = errors.New("SubscriptionNotFound")
	ErrEventSubscriptionAlreadyExists = errors.New("SubscriptionAlreadyExist")
	ErrSnapshotCopyGrantNotFound      = errors.New("SnapshotCopyGrantNotFound")
	ErrSnapshotCopyGrantAlreadyExists = errors.New("SnapshotCopyGrantAlreadyExists")
	ErrSnapshotScheduleNotFound       = errors.New("SnapshotScheduleNotFound")
	ErrSnapshotScheduleAlreadyExists  = errors.New("SnapshotScheduleAlreadyExists")
	ErrUsageLimitNotFound             = errors.New("UsageLimitNotFound")
	ErrAuthProfileNotFound            = errors.New("AuthenticationProfileNotFound")
	ErrAuthProfileAlreadyExists       = errors.New("AuthenticationProfileAlreadyExists")
	ErrResourcePolicyNotFound         = errors.New("ResourcePolicyNotFound")
	ErrSnapshotCopyAlreadyEnabled     = errors.New("SnapshotCopyAlreadyEnabled")
	ErrSnapshotCopyNotEnabled         = errors.New("CopyToRegionDisabled")
	ErrHsmClientCertNotFound          = errors.New("HsmClientCertificateNotFound")
	ErrHsmClientCertAlreadyExists     = errors.New("HsmClientCertificateAlreadyExists")
	ErrHsmConfigNotFound              = errors.New("HsmConfigurationNotFound")
	ErrHsmConfigAlreadyExists         = errors.New("HsmConfigurationAlreadyExists")
	ErrScheduledActionNotFound        = errors.New("ScheduledActionNotFound")
	ErrScheduledActionAlreadyExists   = errors.New("ScheduledActionAlreadyExists")
	ErrCustomDomainNotFound           = errors.New("CustomDomainAssociationNotFoundFault")
	ErrCustomDomainAlreadyExists      = errors.New("CustomDomainAssociationAlreadyExistsFault")
	ErrEndpointAccessNotFound         = errors.New("EndpointNotFound")
	ErrEndpointAccessAlreadyExists    = errors.New("EndpointAlreadyExists")
	ErrIntegrationNotFound            = errors.New("IntegrationNotFound")
	ErrIntegrationAlreadyExists       = errors.New("IntegrationAlreadyExists")
	ErrIdcApplicationNotFound         = errors.New("IdcApplicationNotExistsFault")
	ErrIdcApplicationAlreadyExists    = errors.New("IdcApplicationAlreadyExistsFault")
)

// Named status constants for cluster and resource states.
const (
	clusterStatusAvailable       = "available"
	partnerStatusActive          = "Active"
	dataShareStatusAuthorized    = "AUTHORIZED"
	dataShareStatusActive        = "ACTIVE"
	endpointAuthStatusAuthorized = "Authorized"
	ingressStatusAuthorized      = "authorized"
	resizeStatusCancelled        = "CANCELLED"
	clusterTypeMultiNode         = "multi-node"
	clusterTypeSingleNode        = "single-node"
	defaultNodeType              = "dc2.large"
	defaultDBName                = "dev"
	defaultMasterUsername        = "admin"
	defaultPort                  = 5439
)

// ReservedNode represents an in-memory Redshift reserved node.
type ReservedNode struct {
	StartTime              time.Time `json:"startTime"`
	ReservedNodeID         string    `json:"reservedNodeId"`
	ReservedNodeOfferingID string    `json:"reservedNodeOfferingId"`
	NodeType               string    `json:"nodeType"`
	CurrencyCode           string    `json:"currencyCode"`
	State                  string    `json:"state"`
	OfferingType           string    `json:"offeringType"`
	Duration               int       `json:"duration"`
	FixedPrice             float64   `json:"fixedPrice"`
	UsagePrice             float64   `json:"usagePrice"`
	NodeCount              int       `json:"nodeCount"`
}

// Partner represents a partner integration for a Redshift cluster.
type Partner struct {
	AccountID         string `json:"accountId"`
	ClusterIdentifier string `json:"clusterIdentifier"`
	DatabaseName      string `json:"databaseName"`
	PartnerName       string `json:"partnerName"`
	Status            string `json:"status"`
	StatusMessage     string `json:"statusMessage"`
}

// DataShareAssociation represents an association between a data share and a consumer.
type DataShareAssociation struct {
	ConsumerIdentifier string    `json:"consumerIdentifier"`
	ConsumerRegion     string    `json:"consumerRegion"`
	CreatedDate        time.Time `json:"createdDate"`
	StatusChangeDate   time.Time `json:"statusChangeDate"`
	Status             string    `json:"status"`
	Type               string    `json:"type"`
}

// DataShare represents a Redshift data share.
type DataShare struct {
	DataShareArn                     string                 `json:"dataShareArn"`
	ProducerArn                      string                 `json:"producerArn"`
	ManagedBy                        string                 `json:"managedBy"`
	DataShareAssociations            []DataShareAssociation `json:"dataShareAssociations"`
	AllowPubliclyAccessibleConsumers bool                   `json:"allowPubliclyAccessibleConsumers"`
}

// IPRange represents an IP CIDR range within a cluster security group.
type IPRange struct {
	CIDRIP string `json:"cidrip"`
	Status string `json:"status"`
}

// EC2SecurityGroup represents an EC2 security group within a cluster security group.
type EC2SecurityGroup struct {
	EC2SecurityGroupName    string `json:"ec2SecurityGroupName"`
	EC2SecurityGroupOwnerID string `json:"ec2SecurityGroupOwnerId"`
	Status                  string `json:"status"`
}

// ClusterSecurityGroup represents a Redshift cluster security group.
type ClusterSecurityGroup struct {
	ClusterSecurityGroupName string             `json:"clusterSecurityGroupName"`
	Description              string             `json:"description"`
	IPRanges                 []IPRange          `json:"ipRanges"`
	EC2SecurityGroups        []EC2SecurityGroup `json:"ec2SecurityGroups"`
}

// AccountWithRestoreAccess represents an account permitted to restore from a snapshot.
type AccountWithRestoreAccess struct {
	AccountID    string `json:"accountId"`
	AccountAlias string `json:"accountAlias"`
}

// Snapshot represents a Redshift cluster snapshot.
type Snapshot struct {
	SnapshotCreateTime            time.Time                  `json:"snapshotCreateTime"`
	SnapshotIdentifier            string                     `json:"snapshotIdentifier"`
	ClusterIdentifier             string                     `json:"clusterIdentifier"`
	SnapshotType                  string                     `json:"snapshotType"`
	Status                        string                     `json:"status"`
	NodeType                      string                     `json:"nodeType,omitempty"`
	DBName                        string                     `json:"dbName,omitempty"`
	MasterUsername                string                     `json:"masterUsername,omitempty"`
	AccountsWithRestoreAccess     []AccountWithRestoreAccess `json:"accountsWithRestoreAccess"`
	ManualSnapshotRetentionPeriod int                        `json:"manualSnapshotRetentionPeriod"`
	NumberOfNodes                 int                        `json:"numberOfNodes,omitempty"`
}

// EndpointAuthorization represents authorization for a VPC endpoint to a cluster.
type EndpointAuthorization struct {
	AuthorizeTime     time.Time `json:"authorizeTime"`
	Grantor           string    `json:"grantor"`
	Grantee           string    `json:"grantee"`
	ClusterIdentifier string    `json:"clusterIdentifier"`
	ClusterStatus     string    `json:"clusterStatus"`
	Status            string    `json:"status"`
	AllowedVPCs       []string  `json:"allowedVPCs"`
	EndpointCount     int       `json:"endpointCount"`
	AllowedAllVPCs    bool      `json:"allowedAllVPCs"`
}

// ResizeProgress represents in-progress resize information for a cluster.
type ResizeProgress struct {
	TargetNodeType         string   `json:"targetNodeType"`
	TargetClusterType      string   `json:"targetClusterType"`
	Status                 string   `json:"status"`
	Message                string   `json:"message"`
	ResizeType             string   `json:"resizeType"`
	ImportTablesCompleted  []string `json:"importTablesCompleted"`
	ImportTablesInProgress []string `json:"importTablesInProgress"`
	ImportTablesNotStarted []string `json:"importTablesNotStarted"`
	TargetNumberOfNodes    int      `json:"targetNumberOfNodes"`
	AllowCancelResize      bool     `json:"allowCancelResize"`
}

// SnapshotBatchError represents an error when deleting a snapshot in a batch operation.
type SnapshotBatchError struct {
	SnapshotIdentifier        string `json:"snapshotIdentifier"`
	SnapshotClusterIdentifier string `json:"snapshotClusterIdentifier"`
	FailureCode               string `json:"failureCode"`
	FailureReason             string `json:"failureReason"`
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// SnapshotCopyGrant represents a KMS key grant for cross-region snapshot copy.
type SnapshotCopyGrant struct {
	Tags                  map[string]string `json:"tags"`
	SnapshotCopyGrantName string            `json:"snapshotCopyGrantName"`
	KMSKeyID              string            `json:"kmsKeyId"`
}

// SnapshotSchedule represents a snapshot schedule for automated snapshots.
type SnapshotSchedule struct {
	Tags                map[string]string `json:"tags"`
	ScheduleIdentifier  string            `json:"scheduleIdentifier"`
	Description         string            `json:"description"`
	ScheduleDefinitions []string          `json:"scheduleDefinitions"`
}

// UsageLimit represents a usage limit for a Redshift feature.
type UsageLimit struct {
	Tags              map[string]string `json:"tags"`
	UsageLimitID      string            `json:"usageLimitId"`
	ClusterIdentifier string            `json:"clusterIdentifier"`
	FeatureType       string            `json:"featureType"`
	LimitType         string            `json:"limitType"`
	BreachAction      string            `json:"breachAction"`
	Amount            int64             `json:"amount"`
}

// AuthenticationProfile represents an authentication profile for Redshift.
type AuthenticationProfile struct {
	AuthenticationProfileName    string `json:"authenticationProfileName"`
	AuthenticationProfileContent string `json:"authenticationProfileContent"`
}

// ResourcePolicy represents a resource-based policy attached to a Redshift resource.
type ResourcePolicy struct {
	ResourceArn string `json:"resourceArn"`
	Policy      string `json:"policy"`
}

// TableRestoreStatus represents the status of a table-level restore operation.
type TableRestoreStatus struct {
	RequestTime           time.Time `json:"requestTime"`
	TableRestoreRequestID string    `json:"tableRestoreRequestId"`
	ClusterIdentifier     string    `json:"clusterIdentifier"`
	Status                string    `json:"status"`
	Message               string    `json:"message"`
	SourceDatabaseName    string    `json:"sourceDatabaseName"`
	SourceTableName       string    `json:"sourceTableName"`
	TargetDatabaseName    string    `json:"targetDatabaseName"`
	TargetTableName       string    `json:"targetTableName"`
}

// HsmClientCertificate represents a Redshift HSM client certificate.
type HsmClientCertificate struct {
	Tags                           map[string]string `json:"tags"`
	HsmClientCertificateIdentifier string            `json:"hsmClientCertificateIdentifier"`
	HsmClientCertificatePublicKey  string            `json:"hsmClientCertificatePublicKey"`
}

// HsmConfiguration represents a Redshift HSM configuration.
type HsmConfiguration struct {
	Tags                       map[string]string `json:"tags"`
	HsmConfigurationIdentifier string            `json:"hsmConfigurationIdentifier"`
	Description                string            `json:"description"`
	HsmIPAddress               string            `json:"hsmIpAddress"`
	HsmPartitionName           string            `json:"hsmPartitionName"`
}

// ScheduledAction represents a Redshift scheduled action.
type ScheduledAction struct {
	ScheduledActionName        string `json:"scheduledActionName"`
	Schedule                   string `json:"schedule"`
	IamRole                    string `json:"iamRole"`
	ScheduledActionDescription string `json:"scheduledActionDescription"`
	State                      string `json:"state"`
	TargetAction               string `json:"targetAction"`
}

// CustomDomainAssociation represents a custom domain name associated with a Redshift cluster.
type CustomDomainAssociation struct {
	ClusterIdentifier          string `json:"clusterIdentifier"`
	CustomDomainName           string `json:"customDomainName"`
	CustomDomainCertificateArn string `json:"customDomainCertificateArn"`
}

// EndpointAccess represents a Redshift managed VPC endpoint.
type EndpointAccess struct {
	ClusterIdentifier  string `json:"clusterIdentifier"`
	EndpointName       string `json:"endpointName"`
	EndpointStatus     string `json:"endpointStatus"`
	EndpointCreateTime string `json:"endpointCreateTime"`
	VpcID              string `json:"vpcId"`
	Port               int    `json:"port"`
}

// Integration represents a zero-ETL integration from Redshift.
type Integration struct {
	IntegrationArn   string `json:"integrationArn"`
	IntegrationName  string `json:"integrationName"`
	SourceArn        string `json:"sourceArn"`
	TargetArn        string `json:"targetArn"`
	Status           string `json:"status"`
	Description      string `json:"description"`
	AdditionalEncKey string `json:"additionalEncryptionContext,omitempty"`
	KmsKeyID         string `json:"kmsKeyId,omitempty"`
}

// IdcApplication represents a Redshift IDC application.
type IdcApplication struct {
	IdcApplicationArn  string `json:"redshiftIdcApplicationArn"`
	IdcApplicationName string `json:"redshiftIdcApplicationName"`
	IdcInstanceArn     string `json:"idcInstanceArn"`
	IdcDisplayName     string `json:"idcDisplayName"`
	IamRoleArn         string `json:"iamRoleArn"`
}

// SnapshotCopyConfig holds the cross-region snapshot copy configuration for a cluster.
type SnapshotCopyConfig struct {
	DestinationRegion     string `json:"destinationRegion"`
	SnapshotCopyGrantName string `json:"snapshotCopyGrantName"`
	RetentionPeriod       int    `json:"retentionPeriod"`
}

// ClusterPendingModifiedValues holds changes queued for the next maintenance window.
type ClusterPendingModifiedValues struct {
	NodeType      string `json:"nodeType,omitempty"`
	NumberOfNodes int    `json:"numberOfNodes,omitempty"`
	Encrypted     bool   `json:"encrypted,omitempty"`
}

// Cluster represents a Redshift cluster.
type Cluster struct {
	Tags                       *tags.Tags                    `json:"tags,omitempty"`
	PendingModifiedValues      *ClusterPendingModifiedValues `json:"pendingModifiedValues,omitempty"`
	ClusterIdentifier          string                        `json:"clusterIdentifier"`
	NodeType                   string                        `json:"nodeType"`
	ClusterType                string                        `json:"clusterType"`
	Endpoint                   string                        `json:"endpoint"`
	Status                     string                        `json:"status"`
	DBName                     string                        `json:"dbName"`
	MasterUsername             string                        `json:"masterUsername"`
	VpcID                      string                        `json:"vpcId,omitempty"`
	KmsKeyID                   string                        `json:"kmsKeyId,omitempty"`
	PreferredMaintenanceWindow string                        `json:"preferredMaintenanceWindow,omitempty"`
	IamRoles                   []string                      `json:"iamRoles,omitempty"`
	Port                       int                           `json:"port"`
	NumberOfNodes              int                           `json:"numberOfNodes"`
	Encrypted                  bool                          `json:"encrypted"`
	EnhancedVpcRouting         bool                          `json:"enhancedVpcRouting"`
}

// InMemoryBackend is the in-memory store for Redshift clusters.
type InMemoryBackend struct {
	dnsRegistrar        DNSRegistrar
	clusters            map[string]*Cluster
	reservedNodes       map[string]*ReservedNode
	partners            map[string]*Partner
	dataShares          map[string]*DataShare
	securityGroups      map[string]*ClusterSecurityGroup
	snapshots           map[string]*Snapshot
	endpointAuths       map[string]*EndpointAuthorization
	activeResizes       map[string]*ResizeProgress
	parameterGroups     map[string]*ClusterParameterGroup
	subnetGroups        map[string]*ClusterSubnetGroup
	loggingStatuses     map[string]*LoggingStatus
	eventSubscriptions  map[string]*EventSubscription
	events              map[string]*Event
	snapshotCopyGrants  map[string]*SnapshotCopyGrant
	snapshotSchedules   map[string]*SnapshotSchedule
	usageLimits         map[string]*UsageLimit
	authProfiles        map[string]*AuthenticationProfile
	resourcePolicies    map[string]*ResourcePolicy
	tableRestores       map[string]*TableRestoreStatus
	snapshotCopyConfigs map[string]*SnapshotCopyConfig
	hsmClientCerts      map[string]*HsmClientCertificate
	hsmConfigs          map[string]*HsmConfiguration
	scheduledActions    map[string]*ScheduledAction
	customDomains       map[string]*CustomDomainAssociation
	endpointAccesses    map[string]*EndpointAccess
	integrations        map[string]*Integration
	idcApplications     map[string]*IdcApplication
	// Serverless resources
	slNamespaces           map[string]*Namespace
	slWorkgroups           map[string]*Workgroup
	slSnapshots            map[string]*ServerlessSnapshot
	slUsageLimits          map[string]*ServerlessUsageLimit
	slScheduledActions     map[string]*ServerlessScheduledAction
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
	clusterActivationDelay time.Duration
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:            make(map[string]*Cluster),
		reservedNodes:       make(map[string]*ReservedNode),
		partners:            make(map[string]*Partner),
		dataShares:          make(map[string]*DataShare),
		securityGroups:      make(map[string]*ClusterSecurityGroup),
		snapshots:           make(map[string]*Snapshot),
		endpointAuths:       make(map[string]*EndpointAuthorization),
		activeResizes:       make(map[string]*ResizeProgress),
		parameterGroups:     make(map[string]*ClusterParameterGroup),
		subnetGroups:        make(map[string]*ClusterSubnetGroup),
		loggingStatuses:     make(map[string]*LoggingStatus),
		eventSubscriptions:  make(map[string]*EventSubscription),
		events:              make(map[string]*Event),
		snapshotCopyGrants:  make(map[string]*SnapshotCopyGrant),
		snapshotSchedules:   make(map[string]*SnapshotSchedule),
		usageLimits:         make(map[string]*UsageLimit),
		authProfiles:        make(map[string]*AuthenticationProfile),
		resourcePolicies:    make(map[string]*ResourcePolicy),
		tableRestores:       make(map[string]*TableRestoreStatus),
		snapshotCopyConfigs: make(map[string]*SnapshotCopyConfig),
		hsmClientCerts:      make(map[string]*HsmClientCertificate),
		hsmConfigs:          make(map[string]*HsmConfiguration),
		scheduledActions:    make(map[string]*ScheduledAction),
		customDomains:       make(map[string]*CustomDomainAssociation),
		endpointAccesses:    make(map[string]*EndpointAccess),
		integrations:        make(map[string]*Integration),
		idcApplications:     make(map[string]*IdcApplication),
		slNamespaces:        make(map[string]*Namespace),
		slWorkgroups:        make(map[string]*Workgroup),
		slSnapshots:         make(map[string]*ServerlessSnapshot),
		slUsageLimits:       make(map[string]*ServerlessUsageLimit),
		slScheduledActions:  make(map[string]*ServerlessScheduledAction),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("redshift"),
	}
}

// Reset clears all backend state while preserving configuration.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, c := range b.clusters {
		c.Tags.Close()
	}

	b.clusters = make(map[string]*Cluster)
	b.reservedNodes = make(map[string]*ReservedNode)
	b.partners = make(map[string]*Partner)
	b.dataShares = make(map[string]*DataShare)
	b.securityGroups = make(map[string]*ClusterSecurityGroup)
	b.snapshots = make(map[string]*Snapshot)
	b.endpointAuths = make(map[string]*EndpointAuthorization)
	b.activeResizes = make(map[string]*ResizeProgress)
	b.parameterGroups = make(map[string]*ClusterParameterGroup)
	b.subnetGroups = make(map[string]*ClusterSubnetGroup)
	b.loggingStatuses = make(map[string]*LoggingStatus)
	b.eventSubscriptions = make(map[string]*EventSubscription)
	b.events = make(map[string]*Event)
	b.snapshotCopyGrants = make(map[string]*SnapshotCopyGrant)
	b.snapshotSchedules = make(map[string]*SnapshotSchedule)
	b.usageLimits = make(map[string]*UsageLimit)
	b.authProfiles = make(map[string]*AuthenticationProfile)
	b.resourcePolicies = make(map[string]*ResourcePolicy)
	b.tableRestores = make(map[string]*TableRestoreStatus)
	b.snapshotCopyConfigs = make(map[string]*SnapshotCopyConfig)
	b.hsmClientCerts = make(map[string]*HsmClientCertificate)
	b.hsmConfigs = make(map[string]*HsmConfiguration)
	b.scheduledActions = make(map[string]*ScheduledAction)
	b.customDomains = make(map[string]*CustomDomainAssociation)
	b.endpointAccesses = make(map[string]*EndpointAccess)
	b.integrations = make(map[string]*Integration)
	b.idcApplications = make(map[string]*IdcApplication)
	b.slNamespaces = make(map[string]*Namespace)
	b.slWorkgroups = make(map[string]*Workgroup)
	b.slSnapshots = make(map[string]*ServerlessSnapshot)
	b.slUsageLimits = make(map[string]*ServerlessUsageLimit)
	b.slScheduledActions = make(map[string]*ServerlessScheduledAction)
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// SetDNSRegistrar wires a DNS server so Redshift cluster hostnames are auto-registered.
func (b *InMemoryBackend) SetDNSRegistrar(dns DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	defer b.mu.Unlock()
	b.dnsRegistrar = dns
}

// cloneCluster returns a deep copy of a Cluster, excluding the live Tags pointer.
// The caller receives a value copy with a nil Tags field; use Tags.Clone() to get tag data.
func cloneCluster(c *Cluster) Cluster {
	cp := *c
	// Tags is a live pointer; callers that need tag data must call c.Tags.Clone() separately.
	// Setting to nil prevents callers from accidentally mutating the backend via the copy.
	cp.Tags = nil

	return cp
}

// CreateCluster creates a new Redshift cluster.
func (b *InMemoryBackend) CreateCluster(id, nodeType, dbName, masterUser string) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if !clusterIDRegex.MatchString(id) || strings.HasSuffix(id, "-") || strings.Contains(id, "--") {
		return nil, fmt.Errorf(
			"%w: ClusterIdentifier %q is invalid (must start with a letter, "+
				"contain only lowercase letters/digits/hyphens, not end with a hyphen, "+
				"not contain consecutive hyphens, max 63 chars)",
			ErrInvalidParameter, id,
		)
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if _, exists := b.clusters[id]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}

	if nodeType == "" {
		nodeType = defaultNodeType
	}
	if dbName == "" {
		dbName = defaultDBName
	}
	if masterUser == "" {
		masterUser = defaultMasterUsername
	}

	endpoint := fmt.Sprintf("%s.%s.%s.redshift.amazonaws.com", id, b.accountID, b.region)

	initialStatus := clusterStatusAvailable
	if b.clusterActivationDelay > 0 {
		initialStatus = "creating"
	}

	cluster := &Cluster{
		ClusterIdentifier: id,
		NodeType:          nodeType,
		ClusterType:       clusterTypeMultiNode,
		Endpoint:          endpoint,
		Status:            initialStatus,
		DBName:            dbName,
		MasterUsername:    masterUser,
		Port:              defaultPort,
		NumberOfNodes:     1,
		Tags:              tags.New("redshift.cluster." + id + ".tags"),
	}
	b.clusters[id] = cluster

	if b.clusterActivationDelay > 0 {
		delay := b.clusterActivationDelay
		go func() {
			time.Sleep(delay)
			b.mu.Lock("CreateCluster.activate")
			defer b.mu.Unlock()
			if c, ok := b.clusters[id]; ok && c.Status == "creating" {
				c.Status = clusterStatusAvailable
			}
		}()
	}

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	cp := cloneCluster(cluster)

	return &cp, nil
}

// DeleteCluster removes the cluster with the given identifier.
func (b *InMemoryBackend) DeleteCluster(id string) (*Cluster, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}

	cp := cloneCluster(cluster)
	cluster.Tags.Close()
	delete(b.clusters, id)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(cp.Endpoint)
	}

	return &cp, nil
}

// DescribeClusters returns clusters. If id is non-empty, returns only that cluster.
// When marker and maxRecords are used, returns a page of results sorted by ClusterIdentifier.
func (b *InMemoryBackend) DescribeClusters(id, marker string, maxRecords int) ([]Cluster, string, error) {
	b.mu.RLock("DescribeClusters")
	defer b.mu.RUnlock()

	if id != "" {
		c, exists := b.clusters[id]
		if !exists {
			return nil, "", fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}

		return []Cluster{cloneCluster(c)}, "", nil
	}

	ids := make([]string, 0, len(b.clusters))
	for k := range b.clusters {
		ids = append(ids, k)
	}

	sort.Strings(ids)

	// Advance past the marker (exclusive — marker is the last ID on the previous page).
	if marker != "" {
		cut := 0
		for cut < len(ids) && ids[cut] <= marker {
			cut++
		}

		ids = ids[cut:]
	}

	nextMarker := ""
	if maxRecords > 0 && len(ids) > maxRecords {
		ids = ids[:maxRecords]
		nextMarker = ids[len(ids)-1]
	}

	clusters := make([]Cluster, 0, len(ids))
	for _, k := range ids {
		clusters = append(clusters, cloneCluster(b.clusters[k]))
	}

	return clusters, nextMarker, nil
}

// DescribeTags returns all tags across all clusters.
func (b *InMemoryBackend) DescribeTags() map[string]map[string]string {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	result := make(map[string]map[string]string, len(b.clusters))
	for id, c := range b.clusters {
		result[id] = c.Tags.Clone()
	}

	return result
}

// CreateTags adds or updates tags on the specified cluster.
func (b *InMemoryBackend) CreateTags(clusterID string, kv map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	c, exists := b.clusters[clusterID]
	if !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	c.Tags.Merge(kv)

	return nil
}

// DeleteTags removes tag keys from the specified cluster.
func (b *InMemoryBackend) DeleteTags(clusterID string, keys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	c, exists := b.clusters[clusterID]
	if !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	c.Tags.DeleteKeys(keys)

	return nil
}

// --- Helper key functions ---

func endpointAuthKey(clusterID, grantee string) string {
	return clusterID + "/" + grantee
}

func partnerKey(clusterID, databaseName, partnerName string) string {
	return clusterID + "/" + databaseName + "/" + partnerName
}

// AcceptReservedNodeExchange exchanges an existing reserved node for a new offering.
func (b *InMemoryBackend) AcceptReservedNodeExchange(reservedNodeID, targetOfferingID string) (*ReservedNode, error) {
	if reservedNodeID == "" {
		return nil, fmt.Errorf("%w: ReservedNodeId is required", ErrInvalidParameter)
	}
	if targetOfferingID == "" {
		return nil, fmt.Errorf("%w: TargetReservedNodeOfferingId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptReservedNodeExchange")
	defer b.mu.Unlock()

	existing, exists := b.reservedNodes[reservedNodeID]
	if !exists {
		return nil, fmt.Errorf("%w: reserved node %s not found", ErrReservedNodeNotFound, reservedNodeID)
	}

	exchanged := &ReservedNode{
		ReservedNodeID:         existing.ReservedNodeID,
		ReservedNodeOfferingID: targetOfferingID,
		NodeType:               existing.NodeType,
		StartTime:              time.Now(),
		Duration:               existing.Duration,
		FixedPrice:             existing.FixedPrice,
		UsagePrice:             existing.UsagePrice,
		CurrencyCode:           existing.CurrencyCode,
		NodeCount:              existing.NodeCount,
		State:                  "active",
		OfferingType:           existing.OfferingType,
	}
	b.reservedNodes[reservedNodeID] = exchanged

	cp := *exchanged

	return &cp, nil
}

// AddPartner adds a partner integration to the specified cluster database.
func (b *InMemoryBackend) AddPartner(accountID, clusterID, databaseName, partnerName string) (*Partner, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}
	if databaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", ErrInvalidParameter)
	}
	if partnerName == "" {
		return nil, fmt.Errorf("%w: PartnerIntegrationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AddPartner")
	defer b.mu.Unlock()

	if _, exists := b.clusters[clusterID]; !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	key := partnerKey(clusterID, databaseName, partnerName)
	partner := &Partner{
		AccountID:         accountID,
		ClusterIdentifier: clusterID,
		DatabaseName:      databaseName,
		PartnerName:       partnerName,
		Status:            partnerStatusActive,
		StatusMessage:     "",
	}
	b.partners[key] = partner

	cp := *partner

	return &cp, nil
}

// AssociateDataShareConsumer associates a consumer with a data share.
func (b *InMemoryBackend) AssociateDataShareConsumer(
	dataShareArn, consumerArn, consumerRegion string,
	_ bool,
) (*DataShare, error) {
	if dataShareArn == "" {
		return nil, fmt.Errorf("%w: DataShareArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateDataShareConsumer")
	defer b.mu.Unlock()

	ds, exists := b.dataShares[dataShareArn]
	if !exists {
		return nil, fmt.Errorf("%w: data share %s not found", ErrDataShareNotFound, dataShareArn)
	}

	assoc := DataShareAssociation{
		ConsumerIdentifier: consumerArn,
		ConsumerRegion:     consumerRegion,
		CreatedDate:        time.Now(),
		StatusChangeDate:   time.Now(),
		Status:             dataShareStatusActive,
		Type:               "CONSUMER",
	}
	ds.DataShareAssociations = append(ds.DataShareAssociations, assoc)

	return cloneDataShare(ds), nil
}

// AuthorizeClusterSecurityGroupIngress adds an ingress rule to a cluster security group.
func (b *InMemoryBackend) AuthorizeClusterSecurityGroupIngress(
	groupName, cidrIP, ec2GroupName, ec2GroupOwnerID string,
) (*ClusterSecurityGroup, error) {
	if groupName == "" {
		return nil, fmt.Errorf("%w: ClusterSecurityGroupName is required", ErrInvalidParameter)
	}
	if cidrIP == "" && ec2GroupName == "" {
		return nil, fmt.Errorf("%w: CIDRIP or EC2SecurityGroupName is required", ErrInvalidParameter)
	}

	b.mu.Lock("AuthorizeClusterSecurityGroupIngress")
	defer b.mu.Unlock()

	sg, exists := b.securityGroups[groupName]
	if !exists {
		return nil, fmt.Errorf("%w: security group %s not found", ErrSecurityGroupNotFound, groupName)
	}

	if cidrIP != "" {
		sg.IPRanges = append(sg.IPRanges, IPRange{CIDRIP: cidrIP, Status: ingressStatusAuthorized})
	}
	if ec2GroupName != "" {
		sg.EC2SecurityGroups = append(sg.EC2SecurityGroups, EC2SecurityGroup{
			EC2SecurityGroupName:    ec2GroupName,
			EC2SecurityGroupOwnerID: ec2GroupOwnerID,
			Status:                  ingressStatusAuthorized,
		})
	}

	return cloneSecurityGroup(sg), nil
}

// AuthorizeDataShare authorizes a data share to a consumer.
func (b *InMemoryBackend) AuthorizeDataShare(dataShareArn, consumerIdentifier string) (*DataShare, error) {
	if dataShareArn == "" {
		return nil, fmt.Errorf("%w: DataShareArn is required", ErrInvalidParameter)
	}
	if consumerIdentifier == "" {
		return nil, fmt.Errorf("%w: ConsumerIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("AuthorizeDataShare")
	defer b.mu.Unlock()

	ds, exists := b.dataShares[dataShareArn]
	if !exists {
		return nil, fmt.Errorf("%w: data share %s not found", ErrDataShareNotFound, dataShareArn)
	}

	assoc := DataShareAssociation{
		ConsumerIdentifier: consumerIdentifier,
		CreatedDate:        time.Now(),
		StatusChangeDate:   time.Now(),
		Status:             dataShareStatusAuthorized,
		Type:               "CONSUMER",
	}
	ds.DataShareAssociations = append(ds.DataShareAssociations, assoc)

	return cloneDataShare(ds), nil
}

// AuthorizeEndpointAccess authorizes an account to create a VPC endpoint to the cluster.
func (b *InMemoryBackend) AuthorizeEndpointAccess(
	clusterID, grantee string,
	vpcIDs []string,
) (*EndpointAuthorization, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}
	if grantee == "" {
		return nil, fmt.Errorf("%w: Account is required", ErrInvalidParameter)
	}

	b.mu.Lock("AuthorizeEndpointAccess")
	defer b.mu.Unlock()

	if _, exists := b.clusters[clusterID]; !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	key := endpointAuthKey(clusterID, grantee)
	if _, exists := b.endpointAuths[key]; exists {
		return nil, fmt.Errorf("%w: endpoint authorization already exists for cluster %s and account %s",
			ErrEndpointAuthAlreadyExists, clusterID, grantee)
	}

	allowedVPCs := make([]string, len(vpcIDs))
	copy(allowedVPCs, vpcIDs)

	auth := &EndpointAuthorization{
		Grantor:           b.accountID,
		Grantee:           grantee,
		ClusterIdentifier: clusterID,
		AuthorizeTime:     time.Now(),
		ClusterStatus:     clusterStatusAvailable,
		Status:            endpointAuthStatusAuthorized,
		AllowedAllVPCs:    len(vpcIDs) == 0,
		AllowedVPCs:       allowedVPCs,
		EndpointCount:     0,
	}
	b.endpointAuths[key] = auth

	return cloneEndpointAuth(auth), nil
}

// AuthorizeSnapshotAccess grants another account restore access to a snapshot.
func (b *InMemoryBackend) AuthorizeSnapshotAccess(snapshotID, accountWithRestoreAccess string) (*Snapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: SnapshotIdentifier is required", ErrInvalidParameter)
	}
	if accountWithRestoreAccess == "" {
		return nil, fmt.Errorf("%w: AccountWithRestoreAccess is required", ErrInvalidParameter)
	}

	b.mu.Lock("AuthorizeSnapshotAccess")
	defer b.mu.Unlock()

	snap, exists := b.snapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	snap.AccountsWithRestoreAccess = append(snap.AccountsWithRestoreAccess, AccountWithRestoreAccess{
		AccountID: accountWithRestoreAccess,
	})

	return cloneSnapshot(snap), nil
}

// BatchDeleteClusterSnapshots deletes multiple cluster snapshots. It returns the list of errors for
// snapshots that could not be deleted and the list of successfully deleted snapshot identifiers.
func (b *InMemoryBackend) BatchDeleteClusterSnapshots(identifiers []string) ([]SnapshotBatchError, []string) {
	b.mu.Lock("BatchDeleteClusterSnapshots")
	defer b.mu.Unlock()

	var batchErrors []SnapshotBatchError

	var deleted []string

	for _, id := range identifiers {
		if _, exists := b.snapshots[id]; !exists {
			batchErrors = append(batchErrors, SnapshotBatchError{
				SnapshotIdentifier: id,
				FailureCode:        errClusterSnapshotNotFound,
				FailureReason:      fmt.Sprintf("snapshot %s not found", id),
			})

			continue
		}

		delete(b.snapshots, id)
		deleted = append(deleted, id)
	}

	return batchErrors, deleted
}

// BatchModifyClusterSnapshots modifies the retention period for a list of snapshots.
// The force parameter is accepted for API compatibility but has no effect in the in-memory backend.
// Returns errors and the list of successfully modified snapshot identifiers.
func (b *InMemoryBackend) BatchModifyClusterSnapshots(
	identifiers []string,
	retentionPeriod int,
	_ bool,
) ([]SnapshotBatchError, []string) {
	b.mu.Lock("BatchModifyClusterSnapshots")
	defer b.mu.Unlock()

	var batchErrors []SnapshotBatchError

	var modified []string

	for _, id := range identifiers {
		snap, exists := b.snapshots[id]
		if !exists {
			batchErrors = append(batchErrors, SnapshotBatchError{
				SnapshotIdentifier: id,
				FailureCode:        errClusterSnapshotNotFound,
				FailureReason:      fmt.Sprintf("snapshot %s not found", id),
			})

			continue
		}

		snap.ManualSnapshotRetentionPeriod = retentionPeriod
		modified = append(modified, id)
	}

	return batchErrors, modified
}

// CancelResize cancels an active resize operation for a cluster and returns the final resize status.
func (b *InMemoryBackend) CancelResize(clusterID string) (*ResizeProgress, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelResize")
	defer b.mu.Unlock()

	if _, exists := b.clusters[clusterID]; !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	resize, exists := b.activeResizes[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: no active resize for cluster %s", ErrResizeNotFound, clusterID)
	}

	if !resize.AllowCancelResize {
		return nil, fmt.Errorf(
			"%w: resize for cluster %s cannot be cancelled at this stage",
			ErrResizeNotCancellable,
			clusterID,
		)
	}

	cp := *resize
	cp.Status = resizeStatusCancelled
	delete(b.activeResizes, clusterID)

	return &cp, nil
}

// --- Deep-copy helpers ---

func cloneDataShare(ds *DataShare) *DataShare {
	cp := *ds
	cp.DataShareAssociations = make([]DataShareAssociation, len(ds.DataShareAssociations))
	copy(cp.DataShareAssociations, ds.DataShareAssociations)

	return &cp
}

func cloneSecurityGroup(sg *ClusterSecurityGroup) *ClusterSecurityGroup {
	cp := *sg
	cp.IPRanges = make([]IPRange, len(sg.IPRanges))
	copy(cp.IPRanges, sg.IPRanges)
	cp.EC2SecurityGroups = make([]EC2SecurityGroup, len(sg.EC2SecurityGroups))
	copy(cp.EC2SecurityGroups, sg.EC2SecurityGroups)

	return &cp
}

func cloneEndpointAuth(ea *EndpointAuthorization) *EndpointAuthorization {
	cp := *ea
	cp.AllowedVPCs = make([]string, len(ea.AllowedVPCs))
	copy(cp.AllowedVPCs, ea.AllowedVPCs)

	return &cp
}

func cloneSnapshot(snap *Snapshot) *Snapshot {
	cp := *snap
	cp.AccountsWithRestoreAccess = make([]AccountWithRestoreAccess, len(snap.AccountsWithRestoreAccess))
	copy(cp.AccountsWithRestoreAccess, snap.AccountsWithRestoreAccess)

	return &cp
}

// --- Internal seed helpers (used by tests) ---

// AddReservedNodeInternal seeds a reserved node directly into the backend.
func (b *InMemoryBackend) AddReservedNodeInternal(node *ReservedNode) {
	b.mu.Lock("AddReservedNodeInternal")
	defer b.mu.Unlock()
	b.reservedNodes[node.ReservedNodeID] = node
}

// AddDataShareInternal seeds a data share directly into the backend.
func (b *InMemoryBackend) AddDataShareInternal(ds *DataShare) {
	b.mu.Lock("AddDataShareInternal")
	defer b.mu.Unlock()
	b.dataShares[ds.DataShareArn] = ds
}

// AddSecurityGroupInternal seeds a cluster security group directly into the backend.
func (b *InMemoryBackend) AddSecurityGroupInternal(sg *ClusterSecurityGroup) {
	b.mu.Lock("AddSecurityGroupInternal")
	defer b.mu.Unlock()
	b.securityGroups[sg.ClusterSecurityGroupName] = sg
}

// AddSnapshotInternal seeds a snapshot directly into the backend.
func (b *InMemoryBackend) AddSnapshotInternal(snap *Snapshot) {
	b.mu.Lock("AddSnapshotInternal")
	defer b.mu.Unlock()
	b.snapshots[snap.SnapshotIdentifier] = snap
}

// AddActiveResizeInternal seeds an active resize directly into the backend.
func (b *InMemoryBackend) AddActiveResizeInternal(clusterID string, resize *ResizeProgress) {
	b.mu.Lock("AddActiveResizeInternal")
	defer b.mu.Unlock()
	b.activeResizes[clusterID] = resize
}

// AddParameterGroupInternal seeds a parameter group directly into the backend.
func (b *InMemoryBackend) AddParameterGroupInternal(pg *ClusterParameterGroup) {
	b.mu.Lock("AddParameterGroupInternal")
	defer b.mu.Unlock()
	b.parameterGroups[pg.ParameterGroupName] = pg
}

// AddSubnetGroupInternal seeds a subnet group directly into the backend.
func (b *InMemoryBackend) AddSubnetGroupInternal(sg *ClusterSubnetGroup) {
	b.mu.Lock("AddSubnetGroupInternal")
	defer b.mu.Unlock()
	b.subnetGroups[sg.ClusterSubnetGroupName] = sg
}
