package redshift

import (
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	ErrClusterNotFound           = errors.New("ClusterNotFound")
	ErrClusterAlreadyExists      = errors.New("ClusterAlreadyExists")
	ErrInvalidParameter          = errors.New("InvalidParameterValue")
	ErrReservedNodeNotFound      = errors.New("ReservedNodeNotFound")
	ErrReservedNodeAlreadyExists = errors.New("ReservedNodeAlreadyExists")
	ErrPartnerNotFound           = errors.New("PartnerNotFound")
	ErrDataShareNotFound         = errors.New("DataShareNotFound")
	ErrSecurityGroupNotFound     = errors.New("ClusterSecurityGroupNotFound")
	ErrSnapshotNotFound          = errors.New("ClusterSnapshotNotFound")
	ErrEndpointAuthNotFound      = errors.New("EndpointAuthorizationNotFound")
	ErrEndpointAuthAlreadyExists = errors.New("EndpointAuthorizationAlreadyExists")
	ErrResizeNotFound            = errors.New("ResizeNotFound")
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
	SnapshotIdentifier            string                     `json:"snapshotIdentifier"`
	ClusterIdentifier             string                     `json:"clusterIdentifier"`
	Status                        string                     `json:"status"`
	AccountsWithRestoreAccess     []AccountWithRestoreAccess `json:"accountsWithRestoreAccess"`
	ManualSnapshotRetentionPeriod int                        `json:"manualSnapshotRetentionPeriod"`
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

// Cluster represents a Redshift cluster.
type Cluster struct {
	Tags              *tags.Tags `json:"tags,omitempty"`
	ClusterIdentifier string     `json:"clusterIdentifier"`
	NodeType          string     `json:"nodeType"`
	Endpoint          string     `json:"endpoint"`
	Status            string     `json:"status"`
	DBName            string     `json:"dbName"`
	MasterUsername    string     `json:"masterUsername"`
}

// InMemoryBackend is the in-memory store for Redshift clusters.
type InMemoryBackend struct {
	dnsRegistrar   DNSRegistrar
	clusters       map[string]*Cluster
	reservedNodes  map[string]*ReservedNode
	partners       map[string]*Partner
	dataShares     map[string]*DataShare
	securityGroups map[string]*ClusterSecurityGroup
	snapshots      map[string]*Snapshot
	endpointAuths  map[string]*EndpointAuthorization
	activeResizes  map[string]*ResizeProgress
	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:       make(map[string]*Cluster),
		reservedNodes:  make(map[string]*ReservedNode),
		partners:       make(map[string]*Partner),
		dataShares:     make(map[string]*DataShare),
		securityGroups: make(map[string]*ClusterSecurityGroup),
		snapshots:      make(map[string]*Snapshot),
		endpointAuths:  make(map[string]*EndpointAuthorization),
		activeResizes:  make(map[string]*ResizeProgress),
		accountID:      accountID,
		region:         region,
		mu:             lockmetrics.New("redshift"),
	}
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

// CreateCluster creates a new Redshift cluster.
func (b *InMemoryBackend) CreateCluster(id, nodeType, dbName, masterUser string) (*Cluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if _, exists := b.clusters[id]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}

	if nodeType == "" {
		nodeType = "dc2.large"
	}
	if dbName == "" {
		dbName = "dev"
	}
	if masterUser == "" {
		masterUser = "admin"
	}

	endpoint := fmt.Sprintf("%s.%s.%s.redshift.amazonaws.com", id, b.accountID, b.region)
	cluster := &Cluster{
		ClusterIdentifier: id,
		NodeType:          nodeType,
		Endpoint:          endpoint,
		Status:            "available",
		DBName:            dbName,
		MasterUsername:    masterUser,
		Tags:              tags.New("redshift.cluster." + id + ".tags"),
	}
	b.clusters[id] = cluster

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	cp := *cluster

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

	cp := *cluster
	cluster.Tags.Close()
	delete(b.clusters, id)

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(cp.Endpoint)
	}

	return &cp, nil
}

// DescribeClusters returns clusters. If id is non-empty, returns only that cluster.
func (b *InMemoryBackend) DescribeClusters(id string) ([]Cluster, error) {
	b.mu.RLock("DescribeClusters")
	defer b.mu.RUnlock()

	if id != "" {
		c, exists := b.clusters[id]
		if !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}

		return []Cluster{*c}, nil
	}

	clusters := make([]Cluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		clusters = append(clusters, *c)
	}

	return clusters, nil
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
		Status:            "Active",
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
		Status:             "ACTIVE",
		Type:               "CONSUMER",
	}
	ds.DataShareAssociations = append(ds.DataShareAssociations, assoc)

	cp := *ds
	cp.DataShareAssociations = make([]DataShareAssociation, len(ds.DataShareAssociations))
	copy(cp.DataShareAssociations, ds.DataShareAssociations)

	return &cp, nil
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
		sg.IPRanges = append(sg.IPRanges, IPRange{CIDRIP: cidrIP, Status: "authorized"})
	}
	if ec2GroupName != "" {
		sg.EC2SecurityGroups = append(sg.EC2SecurityGroups, EC2SecurityGroup{
			EC2SecurityGroupName:    ec2GroupName,
			EC2SecurityGroupOwnerID: ec2GroupOwnerID,
			Status:                  "authorized",
		})
	}

	cp := *sg
	cp.IPRanges = make([]IPRange, len(sg.IPRanges))
	copy(cp.IPRanges, sg.IPRanges)
	cp.EC2SecurityGroups = make([]EC2SecurityGroup, len(sg.EC2SecurityGroups))
	copy(cp.EC2SecurityGroups, sg.EC2SecurityGroups)

	return &cp, nil
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
		Status:             "AUTHORIZED",
		Type:               "CONSUMER",
	}
	ds.DataShareAssociations = append(ds.DataShareAssociations, assoc)

	cp := *ds
	cp.DataShareAssociations = make([]DataShareAssociation, len(ds.DataShareAssociations))
	copy(cp.DataShareAssociations, ds.DataShareAssociations)

	return &cp, nil
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
		ClusterStatus:     "active",
		Status:            "Authorized",
		AllowedAllVPCs:    len(vpcIDs) == 0,
		AllowedVPCs:       allowedVPCs,
		EndpointCount:     0,
	}
	b.endpointAuths[key] = auth

	cp := *auth
	cp.AllowedVPCs = make([]string, len(auth.AllowedVPCs))
	copy(cp.AllowedVPCs, auth.AllowedVPCs)

	return &cp, nil
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

	cp := *snap
	cp.AccountsWithRestoreAccess = make([]AccountWithRestoreAccess, len(snap.AccountsWithRestoreAccess))
	copy(cp.AccountsWithRestoreAccess, snap.AccountsWithRestoreAccess)

	return &cp, nil
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
				FailureCode:        "ClusterSnapshotNotFound",
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
// If force is true, snapshots with no manual retention period override are still modified.
// Returns errors and the list of successfully modified snapshot identifiers.
func (b *InMemoryBackend) BatchModifyClusterSnapshots(
	identifiers []string,
	retentionPeriod int,
	force bool,
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
				FailureCode:        "ClusterSnapshotNotFound",
				FailureReason:      fmt.Sprintf("snapshot %s not found", id),
			})

			continue
		}

		snap.ManualSnapshotRetentionPeriod = retentionPeriod
		modified = append(modified, id)

		_ = force
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

	cp := *resize
	cp.Status = "CANCELLED"
	delete(b.activeResizes, clusterID)

	return &cp, nil
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
