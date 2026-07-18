package directoryservice

import (
	"time"
)

// storedVpcSettings holds VPC settings for serialization.
type storedVpcSettings struct {
	VpcID             string   `json:"vpcId"`
	SubnetIDs         []string `json:"subnetIds"`
	SecurityGroupIDs  []string `json:"securityGroupIds"`
	AvailabilityZones []string `json:"availabilityZones"`
}

// storedDirectory holds a directory with all fields.
type storedDirectory struct {
	// region is the AWS region this directory belongs to. It is the outer
	// half of the composite key ("region|DirectoryID") used by the backend's
	// flat store.Table[storedDirectory] (see store_setup.go), which replaces
	// the old map[string]map[string]*storedDirectory nesting (outer key =
	// region). Unexported so it never appears in wire responses (those are
	// built by toDirectory, never by marshaling storedDirectory directly),
	// but persistence.go must carry it through a DTO explicitly since
	// json.Marshal never sees unexported fields.
	region      string
	LaunchTime  time.Time          `json:"launchTime"`
	Tags        map[string]string  `json:"tags"`
	VpcSettings *storedVpcSettings `json:"vpcSettings,omitempty"`
	DirectoryID string             `json:"directoryId"`
	Name        string             `json:"name"`
	ShortName   string             `json:"shortName"`
	Description string             `json:"description"`
	Alias       string             `json:"alias"`
	AccessURL   string             `json:"accessUrl"`
	DirType     string             `json:"type"`
	Stage       string             `json:"stage"`
	Size        string             `json:"size"`
	Edition     string             `json:"edition"`
	SsoEnabled  bool               `json:"ssoEnabled"`
}

func (d *storedDirectory) toDirectory() Directory {
	dir := Directory{
		LaunchTime:  d.LaunchTime,
		DirectoryID: d.DirectoryID,
		Name:        d.Name,
		ShortName:   d.ShortName,
		Description: d.Description,
		Alias:       d.Alias,
		AccessURL:   d.AccessURL,
		Type:        DirectoryType(d.DirType),
		Stage:       DirectoryStage(d.Stage),
		Size:        DirectorySize(d.Size),
		Edition:     DirectoryEdition(d.Edition),
		SsoEnabled:  d.SsoEnabled,
	}
	if d.VpcSettings != nil {
		dir.VpcSettings = &DirectoryVpcSettings{
			VpcID:             d.VpcSettings.VpcID,
			SubnetIDs:         d.VpcSettings.SubnetIDs,
			SecurityGroupIDs:  d.VpcSettings.SecurityGroupIDs,
			AvailabilityZones: d.VpcSettings.AvailabilityZones,
		}
	}

	return dir
}

// storedSnapshot holds a snapshot with all fields.
type storedSnapshot struct {
	// region is the AWS region this snapshot belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region      string
	StartTime   time.Time `json:"startTime"`
	SnapshotID  string    `json:"snapshotId"`
	DirectoryID string    `json:"directoryId"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	SnapType    string    `json:"type"`
}

func (s *storedSnapshot) toSnapshot() Snapshot {
	return Snapshot{
		StartTime:   s.StartTime,
		SnapshotID:  s.SnapshotID,
		DirectoryID: s.DirectoryID,
		Name:        s.Name,
		Status:      SnapshotStatus(s.Status),
		Type:        SnapshotType(s.SnapType),
	}
}

// --- Stored types for the extended (Appendix A) resource families ---

type storedIpRoute struct { //nolint:revive,staticcheck // existing issue.
	AddedDateTime time.Time `json:"addedDateTime"`
	DirectoryID   string    `json:"directoryId"`
	CidrIP        string    `json:"cidrIp"`
	Description   string    `json:"description"`
	IPRouteStatus string    `json:"ipRouteStatus"`
}

// storedRegion holds an AWS Directory Service "Region" (multi-region
// replication) resource. Its own region field below is the AWS *request*
// region (the outer half of the store.Table composite key; see
// storedDirectory.region), which is distinct from RegionName (the DS
// replication-region resource attribute itself).
type storedRegion struct {
	region      string
	LaunchTime  time.Time `json:"launchTime"`
	DirectoryID string    `json:"directoryId"`
	RegionName  string    `json:"regionName"`
	RegionType  string    `json:"regionType"`
	Status      string    `json:"status"`
}

type storedSchemaExtension struct {
	// region is the AWS region this schema extension belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region      string
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime"`
	ExtensionID string    `json:"extensionId"`
	DirectoryID string    `json:"directoryId"`
	Description string    `json:"description"`
	Status      string    `json:"status"`
}

type storedConditionalForwarder struct {
	// region is the AWS region this conditional forwarder belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region           string
	DirectoryID      string   `json:"directoryId"`
	RemoteDomainName string   `json:"remoteDomainName"`
	ReplicationScope string   `json:"replicationScope"`
	DNSIPAddrs       []string `json:"dnsIpAddrs"`
}

type storedLogSubscription struct {
	// region is the AWS region this log subscription belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region                      string
	SubscriptionCreatedDateTime time.Time `json:"subscriptionCreatedDateTime"`
	DirectoryID                 string    `json:"directoryId"`
	LogGroupName                string    `json:"logGroupName"`
}

type storedEventTopic struct {
	// region is the AWS region this event topic belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region          string
	CreatedDateTime time.Time `json:"createdDateTime"`
	DirectoryID     string    `json:"directoryId"`
	TopicName       string    `json:"topicName"`
	TopicARN        string    `json:"topicArn"`
	Status          string    `json:"status"`
}

type storedDomainController struct {
	// region is the AWS region this domain controller belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region           string
	LaunchTime       time.Time `json:"launchTime"`
	ControllerID     string    `json:"controllerId"`
	DirectoryID      string    `json:"directoryId"`
	Status           string    `json:"status"`
	AvailabilityZone string    `json:"availabilityZone"`
}

type storedTrust struct {
	// region is the AWS region this trust belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region               string
	CreatedDateTime      time.Time `json:"createdDateTime"`
	LastUpdatedDateTime  time.Time `json:"lastUpdatedDateTime"`
	StateLastUpdatedTime time.Time `json:"stateLastUpdatedDateTime"`
	TrustID              string    `json:"trustId"`
	DirectoryID          string    `json:"directoryId"`
	RemoteDomainName     string    `json:"remoteDomainName"`
	TrustDirection       string    `json:"trustDirection"`
	TrustType            string    `json:"trustType"`
	TrustState           string    `json:"trustState"`
	SelectiveAuth        string    `json:"selectiveAuth"`
	TrustStateReason     string    `json:"trustStateReason"`
}

type storedSharedDirectory struct {
	// region is the AWS region this shared directory belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region              string
	CreatedDateTime     time.Time `json:"createdDateTime"`
	LastUpdatedDateTime time.Time `json:"lastUpdatedDateTime"`
	SharedDirectoryID   string    `json:"sharedDirectoryId"`
	OwnerDirectoryID    string    `json:"ownerDirectoryId"`
	OwnerAccountID      string    `json:"ownerAccountId"`
	SharedAccountID     string    `json:"sharedAccountId"`
	ShareMethod         string    `json:"shareMethod"`
	ShareStatus         string    `json:"shareStatus"`
	ShareNotes          string    `json:"shareNotes"`
}

type storedCertificate struct {
	// region is the AWS region this certificate belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region             string
	RegisteredDateTime time.Time `json:"registeredDateTime"`
	ExpiryDateTime     time.Time `json:"expiryDateTime"`
	CertificateID      string    `json:"certificateId"`
	DirectoryID        string    `json:"directoryId"`
	CertData           string    `json:"certData"`
	CommonName         string    `json:"commonName"`
	CertType           string    `json:"certType"`
	State              string    `json:"state"`
}

type storedLDAPSSetting struct {
	// region is the AWS region this LDAPS setting belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region                    string
	LastUpdatedDateTime       time.Time `json:"lastUpdatedDateTime"`
	CertificateExpiryDateTime time.Time `json:"certificateExpiryDateTime"`
	DirectoryID               string    `json:"directoryId"`
	LDAPSType                 string    `json:"ldapsType"`
	CertificateID             string    `json:"certificateId"`
	State                     string    `json:"state"`
}

type storedClientAuthSetting struct {
	// region is the AWS region this client auth setting belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region              string
	LastUpdatedDateTime time.Time `json:"lastUpdatedDateTime"`
	DirectoryID         string    `json:"directoryId"`
	AuthType            string    `json:"authType"`
	Status              string    `json:"status"`
}

type storedRadiusSettings struct {
	// region is the AWS region these RADIUS settings belong to; see
	// storedDirectory.region for the composite-key rationale.
	region                 string
	DirectoryID            string   `json:"directoryId"`
	AuthenticationProtocol string   `json:"authenticationProtocol"`
	DisplayLabel           string   `json:"displayLabel"`
	SharedSecret           string   `json:"sharedSecret"`
	RadiusServers          []string `json:"radiusServers"`
	RadiusPort             int32    `json:"radiusPort"`
	RadiusRetries          int32    `json:"radiusRetries"`
	RadiusTimeout          int32    `json:"radiusTimeout"`
	UseSameUsername        bool     `json:"useSameUsername"`
}

// storedADAssessment reuses its existing exported Region field (already
// populated with the AWS request region for the API's own
// ADAssessmentInfo.Region output) as the composite-key region component; see
// adAssessmentKeyFn in store_setup.go.
type storedADAssessment struct {
	StartTime    time.Time `json:"startTime"`
	AssessmentID string    `json:"assessmentId"`
	DirectoryID  string    `json:"directoryId"`
	Status       string    `json:"status"`
	AssessType   string    `json:"assessmentType"`
	Region       string    `json:"region"`
}

type storedDirectorySetting struct {
	LastUpdatedDateTime time.Time `json:"lastUpdatedDateTime"`
	DirectoryID         string    `json:"directoryId"`
	Name                string    `json:"name"`
	AllowedValues       string    `json:"allowedValues"`
	AppliedValue        string    `json:"appliedValue"`
	RequestedValue      string    `json:"requestedValue"`
	Status              string    `json:"status"`
}

type storedUpdateInfo struct {
	StartTime           time.Time `json:"startTime"`
	LastUpdatedDateTime time.Time `json:"lastUpdatedDateTime"`
	DirectoryID         string    `json:"directoryId"`
	UpdateType          string    `json:"updateType"`
	Status              string    `json:"status"`
	NewValue            string    `json:"newValue"`
	PreviousValue       string    `json:"previousValue"`
	InitiatedBy         string    `json:"initiatedBy"`
	Region              string    `json:"region"`
}

type storedHybridADUpdate struct {
	// region is the AWS region this hybrid AD update belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region      string
	RequestID   string `json:"requestId"`
	DirectoryID string `json:"directoryId"`
	Status      string `json:"status"`
}

// --- Domain (wire-facing) types for the extended resource families ---

// IpRoute domain type.
type IpRoute struct { //nolint:revive,staticcheck // existing issue.
	DirectoryID string
	CidrIP      string
	Description string
	AddedTime   time.Time
	Status      string
}

// RegionDescription domain type.
type RegionDescription struct {
	LaunchTime  time.Time
	DirectoryID string
	RegionName  string
	RegionType  string
	Status      string
}

// SchemaExtension domain type.
type SchemaExtension struct {
	StartTime   time.Time
	EndTime     time.Time
	ExtensionID string
	DirectoryID string
	Description string
	Status      string
}

// ConditionalForwarder domain type.
type ConditionalForwarder struct {
	DirectoryID      string
	RemoteDomainName string
	ReplicationScope string
	DNSIPAddrs       []string
}

// LogSubscription domain type.
type LogSubscription struct {
	CreatedTime  time.Time
	DirectoryID  string
	LogGroupName string
}

// EventTopic domain type.
type EventTopic struct {
	CreatedDateTime time.Time
	DirectoryID     string
	TopicName       string
	TopicARN        string
	Status          string
}

// DomainController domain type.
type DomainController struct {
	LaunchTime       time.Time
	ControllerID     string
	DirectoryID      string
	Status           string
	AvailabilityZone string
}

// TrustInfo domain type.
type TrustInfo struct {
	CreatedDateTime      time.Time
	LastUpdatedDateTime  time.Time
	StateLastUpdatedTime time.Time
	TrustID              string
	DirectoryID          string
	RemoteDomainName     string
	TrustDirection       string
	TrustType            string
	TrustState           string
	SelectiveAuth        string
	TrustStateReason     string
}

// SharedDirInfo domain type.
type SharedDirInfo struct {
	CreatedDateTime     time.Time
	LastUpdatedDateTime time.Time
	SharedDirectoryID   string
	OwnerDirectoryID    string
	OwnerAccountID      string
	SharedAccountID     string
	ShareMethod         string
	ShareStatus         string
	ShareNotes          string
}

// CertInfo domain type (for list).
type CertInfo struct {
	ExpiryDateTime time.Time
	CertificateID  string
	CommonName     string
	CertType       string
	State          string
}

// CertDetail domain type (for describe).
type CertDetail struct {
	RegisteredDateTime time.Time
	ExpiryDateTime     time.Time
	CertificateID      string
	DirectoryID        string
	CommonName         string
	CertType           string
	State              string
	CertData           string
}

// LDAPSSetting domain type.
type LDAPSSetting struct {
	LastUpdatedDateTime       time.Time
	CertificateExpiryDateTime time.Time
	DirectoryID               string
	LDAPSType                 string
	CertificateID             string
	State                     string
}

// ClientAuthInfo domain type.
type ClientAuthInfo struct {
	LastUpdatedDateTime time.Time
	DirectoryID         string
	AuthType            string
	Status              string
}

// RadiusSettingsInput is used for Enable/Update RADIUS.
type RadiusSettingsInput struct {
	AuthenticationProtocol string
	DisplayLabel           string
	SharedSecret           string
	RadiusServers          []string
	RadiusPort             int32
	RadiusRetries          int32
	RadiusTimeout          int32
	UseSameUsername        bool
}

// RadiusInfo is returned by describe (currently inferred from directory).
type RadiusInfo struct {
	DirectoryID            string
	AuthenticationProtocol string
	RadiusServers          []string
	RadiusPort             int32
	RadiusRetries          int32
	RadiusTimeout          int32
	UseSameUsername        bool
}

// DirectoryDataAccessStatus domain type.
type DirectoryDataAccessStatus struct {
	DirectoryID string
	Enabled     bool
}

// CAEnrollmentPolicy domain type.
type CAEnrollmentPolicy struct {
	DirectoryID string
	Enabled     bool
}

// ADAssessmentInfo domain type.
type ADAssessmentInfo struct {
	StartTime    time.Time
	AssessmentID string
	DirectoryID  string
	Status       string
	AssessType   string
	Region       string
}

// DirectorySetting domain type.
type DirectorySetting struct {
	Name          string
	Value         string
	AllowedValues string
}

// SettingEntry domain type.
type SettingEntry struct {
	LastUpdatedDateTime time.Time
	DirectoryID         string
	Name                string
	AllowedValues       string
	AppliedValue        string
	RequestedValue      string
	Status              string
}

// UpdateInfoEntry domain type.
type UpdateInfoEntry struct {
	StartTime           time.Time
	LastUpdatedDateTime time.Time
	DirectoryID         string
	UpdateType          string
	Status              string
	NewValue            string
	PreviousValue       string
	InitiatedBy         string
	Region              string
}

// ComputerInfo is returned by CreateComputer.
type ComputerInfo struct {
	ComputerID   string
	ComputerName string
}

// HybridADUpdateEntry domain type.
type HybridADUpdateEntry struct {
	RequestID   string
	DirectoryID string
	Status      string
}
