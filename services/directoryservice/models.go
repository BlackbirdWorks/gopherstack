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

// storedConnectSettings holds AD Connector settings for serialization.
type storedConnectSettings struct {
	CustomerUserName string   `json:"customerUserName"`
	VpcID            string   `json:"vpcId"`
	SubnetIDs        []string `json:"subnetIds"`
	CustomerDNSIPs   []string `json:"customerDnsIps"`
	CustomerDNSIPsV6 []string `json:"customerDnsIpsV6"`
}

// storedDirectory holds a directory with all fields.
type storedDirectory struct {
	LaunchTime                 time.Time              `json:"launchTime"`
	StageLastUpdatedDateTime   time.Time              `json:"stageLastUpdatedDateTime"`
	ConnectSettings            *storedConnectSettings `json:"connectSettings,omitempty"`
	Tags                       map[string]string      `json:"tags"`
	VpcSettings                *storedVpcSettings     `json:"vpcSettings,omitempty"`
	DirType                    string                 `json:"type"`
	Size                       string                 `json:"size"`
	Name                       string                 `json:"name"`
	ShortName                  string                 `json:"shortName"`
	Description                string                 `json:"description"`
	Alias                      string                 `json:"alias"`
	AccessURL                  string                 `json:"accessUrl"`
	region                     string
	Stage                      string   `json:"stage"`
	DirectoryID                string   `json:"directoryId"`
	Edition                    string   `json:"edition"`
	NetworkType                string   `json:"networkType"`
	DNSIPAddrs                 []string `json:"dnsIpAddrs"`
	DNSIPv6Addrs               []string `json:"dnsIpv6Addrs"`
	HybridDNSIPs               []string `json:"hybridDnsIps,omitempty"`
	HybridInstanceIDs          []string `json:"hybridInstanceIds,omitempty"`
	DesiredNumberOfDomainCtrls int32    `json:"desiredNumberOfDomainControllers"`
	SsoEnabled                 bool     `json:"ssoEnabled"`
	IsHybridAD                 bool     `json:"isHybridAd,omitempty"`
}

func (d *storedDirectory) toDirectory() Directory {
	dir := Directory{
		LaunchTime:               d.LaunchTime,
		StageLastUpdatedDateTime: d.StageLastUpdatedDateTime,
		DirectoryID:              d.DirectoryID,
		Name:                     d.Name,
		ShortName:                d.ShortName,
		Description:              d.Description,
		Alias:                    d.Alias,
		AccessURL:                d.AccessURL,
		Type:                     DirectoryType(d.DirType),
		Stage:                    DirectoryStage(d.Stage),
		Size:                     DirectorySize(d.Size),
		Edition:                  DirectoryEdition(d.Edition),
		NetworkType:              NetworkType(d.NetworkType),
		DNSIPAddrs:               d.DNSIPAddrs,
		DNSIPv6Addrs:             d.DNSIPv6Addrs,
		SsoEnabled:               d.SsoEnabled,
	}
	if d.VpcSettings != nil {
		dir.VpcSettings = &DirectoryVpcSettings{
			VpcID:             d.VpcSettings.VpcID,
			SubnetIDs:         d.VpcSettings.SubnetIDs,
			SecurityGroupIDs:  d.VpcSettings.SecurityGroupIDs,
			AvailabilityZones: d.VpcSettings.AvailabilityZones,
		}
	}
	if d.ConnectSettings != nil {
		dir.ConnectSettings = &DirectoryConnectSettingsDescription{
			CustomerUserName: d.ConnectSettings.CustomerUserName,
			VpcID:            d.ConnectSettings.VpcID,
			SubnetIDs:        d.ConnectSettings.SubnetIDs,
			ConnectIPs:       synthesizeDNSIPAddrs(d.DirectoryID),
		}
	}
	if DirectoryType(d.DirType) == DirectoryTypeMicrosoftAD {
		desired := d.DesiredNumberOfDomainCtrls
		dir.DesiredNumberOfDomainControllers = &desired
	}
	if d.IsHybridAD {
		dir.HybridSettings = &HybridSettingsDescription{
			SelfManagedDNSIPAddrs:  d.HybridDNSIPs,
			SelfManagedInstanceIDs: d.HybridInstanceIDs,
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
	region                     string
	VpcSettings                *storedVpcSettings `json:"vpcSettings"`
	LaunchTime                 time.Time          `json:"launchTime"`
	StatusLastUpdatedDateTime  time.Time          `json:"statusLastUpdatedDateTime"`
	DirectoryID                string             `json:"directoryId"`
	RegionName                 string             `json:"regionName"`
	RegionType                 string             `json:"regionType"`
	Status                     string             `json:"status"`
	DesiredNumberOfDomainCtrls int32              `json:"desiredNumberOfDomainControllers"`
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
	DNSIPv6Addrs     []string `json:"dnsIpv6Addrs"`
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
	region                    string
	LaunchTime                time.Time `json:"launchTime"`
	StatusLastUpdatedDateTime time.Time `json:"statusLastUpdatedDateTime"`
	ControllerID              string    `json:"controllerId"`
	DirectoryID               string    `json:"directoryId"`
	Status                    string    `json:"status"`
	AvailabilityZone          string    `json:"availabilityZone"`
	DNSIPAddr                 string    `json:"dnsIpAddr"`
	DNSIPv6Addr               string    `json:"dnsIpv6Addr"`
	SubnetID                  string    `json:"subnetId"`
	VpcID                     string    `json:"vpcId"`
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
	OCSPUrl            string    `json:"ocspUrl"`
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
//
// SourceDirectory{Name,ShortName,Description,Edition} are internal-only
// bookkeeping (never surfaced by DescribeADAssessment/ListADAssessments --
// they are not real Assessment/AssessmentSummary members), snapshotted from
// the assessed directory at StartADAssessment time. CreateHybridAD's real
// input is just {AssessmentId, SecretArn, Tags} -- AWS derives the new hybrid
// directory's descriptive fields from the assessment's own
// AssessmentConfiguration (DnsName etc.); this backend still derives them
// from the snapshotted source directory instead (see hybrid_ad.go) because
// CreateHybridAD only supports assessments of an existing directory.
//
// DNSName/CustomerDNSIPs/InstanceIDs/SecurityGroupIDs/VPCID/SubnetIDs mirror
// the real, optional StartADAssessmentInput.AssessmentConfiguration
// (types.AssessmentConfiguration) captured verbatim from the request when
// the caller supplies one (gopherstack-10hx follow-up: the AssessmentConfiguration
// input-capture gap). LastUpdateDateTime is stamped equal to StartTime, same
// as the rest of this backend's synchronous-completion ops (see
// assessmentStatusSuccess).
type storedADAssessment struct {
	StartTime                  time.Time `json:"startTime"`
	LastUpdateDateTime         time.Time `json:"lastUpdateDateTime"`
	AssessmentID               string    `json:"assessmentId"`
	DirectoryID                string    `json:"directoryId"`
	Status                     string    `json:"status"`
	AssessType                 string    `json:"assessmentType"`
	Region                     string    `json:"region"`
	DNSName                    string    `json:"dnsName,omitempty"`
	VPCID                      string    `json:"vpcId,omitempty"`
	SourceDirectoryName        string    `json:"sourceDirectoryName,omitempty"`
	SourceDirectoryShortName   string    `json:"sourceDirectoryShortName,omitempty"`
	SourceDirectoryDescription string    `json:"sourceDirectoryDescription,omitempty"`
	SourceDirectoryEdition     string    `json:"sourceDirectoryEdition,omitempty"`
	CustomerDNSIPs             []string  `json:"customerDnsIps,omitempty"`
	InstanceIDs                []string  `json:"instanceIds,omitempty"`
	SecurityGroupIDs           []string  `json:"securityGroupIds,omitempty"`
	SubnetIDs                  []string  `json:"subnetIds,omitempty"`
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

// storedHybridADUpdate is one HybridUpdateInfoEntry-shaped update-activity
// record (types.HybridUpdateInfoEntry), logged once per UpdateHybridAD call
// per UpdateType touched (HybridAdministratorAccount and/or
// SelfManagedInstances -- a single call can log up to two entries). RequestID
// is this backend's own store key, not a real HybridUpdateInfoEntry member --
// unlike the fabricated top-level "RequestId" this shape used to leak onto
// the wire (see PARITY.md), it is never surfaced by DescribeHybridADUpdate.
type storedHybridADUpdate struct {
	// region is the AWS region this hybrid AD update belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region              string
	RequestID           string    `json:"requestId"`
	DirectoryID         string    `json:"directoryId"`
	AssessmentID        string    `json:"assessmentId"`
	UpdateType          string    `json:"updateType"`
	InitiatedBy         string    `json:"initiatedBy"`
	Status              string    `json:"status"`
	StatusReason        string    `json:"statusReason,omitempty"`
	StartTime           time.Time `json:"startTime"`
	LastUpdatedDateTime time.Time `json:"lastUpdatedDateTime"`
	NewDNSIPs           []string  `json:"newDnsIps,omitempty"`
	NewInstanceIDs      []string  `json:"newInstanceIds,omitempty"`
	PreviousDNSIPs      []string  `json:"previousDnsIps,omitempty"`
	PreviousInstanceIDs []string  `json:"previousInstanceIds,omitempty"`
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
	LaunchTime                 time.Time
	StatusLastUpdatedDateTime  time.Time
	VpcSettings                *DirectoryVpcSettings
	DirectoryID                string
	RegionName                 string
	RegionType                 string
	Status                     string
	DesiredNumberOfDomainCtrls int32
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
	DNSIPv6Addrs     []string
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

// DomainController domain type. StatusReason is never populated: this
// backend's domain controllers always report Status "Active" and never enter
// a failed state that would produce a real status-reason message (see
// PARITY.md).
type DomainController struct {
	StatusReason              *string
	LaunchTime                time.Time
	StatusLastUpdatedDateTime time.Time
	ControllerID              string
	DirectoryID               string
	Status                    string
	AvailabilityZone          string
	DNSIPAddr                 string
	DNSIPv6Addr               string
	SubnetID                  string
	VpcID                     string
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
	OCSPUrl            string
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

// CAEnrollmentPolicy domain type (aws-sdk-go-v2/service/directoryservice's
// DescribeCAEnrollmentPolicyOutput). Status takes the wire values of
// types.CaEnrollmentPolicyStatus: InProgress/Success/Failed/Disabling/
// Disabled/Impaired (verified against types/enums.go -- not the
// "Enabled"/"Disabled" pair the pre-fix shape used).
type CAEnrollmentPolicy struct {
	LastUpdatedDateTime time.Time `json:"lastUpdatedDateTime"`
	DirectoryID         string    `json:"directoryId"`
	Status              string    `json:"status"`
	StatusReason        string    `json:"statusReason"`
	PcaConnectorArn     string    `json:"pcaConnectorArn"`
}

// ADAssessmentConfiguration mirrors the real, optional
// StartADAssessmentInput.AssessmentConfiguration (types.AssessmentConfiguration).
// When a caller supplies AssessmentConfiguration at all, DNSName/CustomerDNSIPs/
// InstanceIDs/VPCID/SubnetIDs are required members (matching the real SDK's
// validateAssessmentConfiguration); SecurityGroupIDs is optional.
type ADAssessmentConfiguration struct {
	DNSName          string
	VPCID            string
	CustomerDNSIPs   []string
	InstanceIDs      []string
	SecurityGroupIDs []string
	SubnetIDs        []string
}

// ADAssessmentInfo domain type for both DescribeADAssessment (full Assessment
// shape) and ListADAssessments (AssessmentSummary shape, a strict subset --
// see handler_ad_assessments.go for which fields each op actually puts on the
// wire). DNSName/CustomerDNSIPs/InstanceIDs/SecurityGroupIDs/VPCID/SubnetIDs
// come from the real, optional StartADAssessmentInput.AssessmentConfiguration
// (types.AssessmentConfiguration) when the caller supplies it; they are empty
// for assessments started without one (e.g. UpdateHybridAD's
// internally-triggered assessment, which has no AssessmentConfiguration input
// in the real API either). StatusCode/StatusReason/Version are real Assessment
// members this backend cannot honestly populate: AWS documents them as
// assessment-engine-internal output (a detailed status code, a human-readable
// status/error message, and the assessment-framework version) with no request
// input and no documented deterministic default -- same class of gap as
// Directory.OsVersion (see PARITY.md). Left always empty rather than invented.
type ADAssessmentInfo struct {
	StartTime          time.Time
	LastUpdateDateTime time.Time
	AssessmentID       string
	DirectoryID        string
	Status             string
	AssessType         string
	Region             string
	DNSName            string
	VPCID              string
	StatusCode         string
	StatusReason       string
	Version            string
	CustomerDNSIPs     []string
	InstanceIDs        []string
	SecurityGroupIDs   []string
	SubnetIDs          []string
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

// HybridADUpdateEntry mirrors types.HybridUpdateInfoEntry, the real per-entry
// shape DescribeHybridADUpdate returns (nested under UpdateActivities.
// HybridAdministratorAccount/SelfManagedInstances). NewDNSIPs/NewInstanceIDs
// and PreviousDNSIPs/PreviousInstanceIDs mirror the real NewValue/
// PreviousValue (types.HybridUpdateValue{DnsIps,InstanceIds}).
type HybridADUpdateEntry struct {
	StartTime           time.Time
	LastUpdatedDateTime time.Time
	AssessmentID        string
	InitiatedBy         string
	Status              string
	StatusReason        string
	NewDNSIPs           []string
	NewInstanceIDs      []string
	PreviousDNSIPs      []string
	PreviousInstanceIDs []string
}
