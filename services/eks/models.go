package eks

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	statusActive      = "ACTIVE"
	statusInProgress  = "InProgress"
	statusCancelled   = "Cancelled"
	defaultK8sVersion = "1.32"
	priorK8sVersion   = "1.31"
)

const (
	// typeVersionRollback is the only UpdateType real EKS currently allows
	// CancelUpdate to act on (Kubernetes version rollback on EKS Auto Mode
	// clusters) -- verified against aws-sdk-go-v2/service/eks's CancelUpdate
	// doc comment and types.UpdateTypeVersionRollback.
	typeVersionRollback = "VersionRollback"

	cancellationStatusSuccessful = "Successful"
)

const (
	keyNetworking               = "networking"
	keyCompatibilities          = "compatibilities"
	keyClusterVersion           = "clusterVersion"
	keyDefaultVersion           = "defaultVersion"
	keyEndOfStandardSupportDate = "endOfStandardSupportDate"
	keyEndOfExtendedSupportDate = "endOfExtendedSupportDate"
)

const (
	keyAddonName              = "addonName"
	keyAddonVersions          = "addonVersions"
	keyAddonVersion           = "addonVersion"
	typeUpgradeReadiness      = "UPGRADE_READINESS"
	typeVersionUpdate         = "VersionUpdate"
	connectorActivationWindow = 24 * time.Hour
)

const (
	statusPassing    = "PASSING"
	statusCreating   = "CREATING"
	statusFailed     = "FAILED"
	statusDegraded   = "DEGRADED"
	statusDeleting   = "DELETING"
	statusSuccessful = "Successful"
)

// VpcConfig captures the cluster VPC configuration returned by AWS.
type VpcConfig struct {
	ClusterSecurityGroupID string   `json:"clusterSecurityGroupId,omitempty"`
	VpcID                  string   `json:"vpcId,omitempty"`
	SubnetIDs              []string `json:"subnetIds,omitempty"`
	SecurityGroupIDs       []string `json:"securityGroupIds,omitempty"`
	PublicAccessCIDRs      []string `json:"publicAccessCidrs,omitempty"`
	EndpointPrivateAccess  bool     `json:"endpointPrivateAccess"`
	EndpointPublicAccess   bool     `json:"endpointPublicAccess"`
}

// AccessConfig holds the cluster authentication mode configuration.
type AccessConfig struct {
	AuthenticationMode                      string `json:"authenticationMode,omitempty"`
	BootstrapClusterCreatorAdminPermissions bool   `json:"bootstrapClusterCreatorAdminPermissions"`
}

// ComputeConfig holds the EKS Auto Mode compute configuration.
type ComputeConfig struct {
	NodeRoleARN string   `json:"nodeRoleArn,omitempty"`
	NodePools   []string `json:"nodePools,omitempty"`
	Enabled     bool     `json:"enabled"`
}

// BlockStorageConfig holds EKS Auto Mode block storage settings.
type BlockStorageConfig struct {
	Enabled bool `json:"enabled"`
}

// StorageConfig holds the EKS Auto Mode storage configuration.
type StorageConfig struct {
	BlockStorage *BlockStorageConfig `json:"blockStorage,omitempty"`
}

// ElasticLoadBalancingConfig holds EKS Auto Mode load balancer settings.
type ElasticLoadBalancingConfig struct {
	Enabled bool `json:"enabled"`
}

// KubernetesNetworkConfig captures cluster networking parameters. The real
// SDK's KubernetesNetworkConfigRequest/KubernetesNetworkConfigResponse
// (eks@v1.90.4 types/types.go:1597,1645) both declare ElasticLoadBalancing as
// a sibling of IpFamily/ServiceIpv4Cidr/ServiceIpv6Cidr under ONE
// "kubernetesNetworkConfig" wire key -- there is no separate top-level
// "networkingConfig" object in real AWS. gopherstack-tp8x: a prior version of
// this type split ElasticLoadBalancing into a second, separately-named
// top-level Cluster.NetworkingConfig field/JSON key that a real client never
// reads or sends.
type KubernetesNetworkConfig struct {
	ElasticLoadBalancing *ElasticLoadBalancingConfig `json:"elasticLoadBalancing,omitempty"`
	IPFamily             string                      `json:"ipFamily,omitempty"`
	ServiceIPv4CIDR      string                      `json:"serviceIpv4Cidr,omitempty"`
	ServiceIPv6CIDR      string                      `json:"serviceIpv6Cidr,omitempty"`
}

// ClusterLogEntry represents one log-type group in the structured logging config.
type ClusterLogEntry struct {
	Types   []string `json:"types"`
	Enabled bool     `json:"enabled"`
}

// activationExpiry is ConnectorConfig.ActivationExpiry's persisted
// representation. It behaves like a time.Time (use .Time() to get one) but
// carries its own UnmarshalJSON so Restore tolerates every shape this field
// has ever been snapshotted in:
//   - a bare RFC3339 string (every snapshot written before gopherstack-wf8f,
//     back when the Go field type was plain string)
//   - the RFC3339Nano string encoding/json's default time.Time
//     MarshalJSON produces (what this type itself currently persists as)
//   - an epoch-seconds JSON number, defensively, in case persistence ever
//     switches to the wire's numeric convention (see MarshalJSON below --
//     not the case today, but this keeps that door open for free)
//
// LANDMINE: do not simplify this back to a bare time.Time. gopherstack-wf8f
// (2026-09-11) retyped the WIRE-facing shape from string to time.Time to
// fix a real bug (real types.ConnectorConfigResponse.ActivationExpiry
// deserializes an epoch-seconds JSON number on the wire, not RFC3339 --
// see connectorConfigToJSON in handler_clusters.go), but the snapshot
// representation is a separate concern from the wire shape (this codebase
// snapshots domain structs via plain encoding/json, not through the wire
// builders) -- this type is what lets that wire-shape fix stay purely
// additive to backendSnapshot instead of forcing an eksSnapshotVersion
// bump that would discard every user's persisted eks state on restore.
type activationExpiry time.Time

// Time returns the wrapped time.Time.
func (a activationExpiry) Time() time.Time { return time.Time(a) }

// IsZero reports whether the wrapped time.Time is the zero value.
func (a activationExpiry) IsZero() bool { return time.Time(a).IsZero() }

func (a activationExpiry) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(a))
}

func (a *activationExpiry) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}

	var t time.Time
	if err := json.Unmarshal(data, &t); err == nil {
		*a = activationExpiry(t)

		return nil
	}

	var epochSeconds float64
	if err := json.Unmarshal(data, &epochSeconds); err != nil {
		return fmt.Errorf("activationExpiry: unsupported JSON value %s: %w", data, err)
	}

	sec := int64(epochSeconds)
	nsec := int64((epochSeconds - float64(sec)) * float64(time.Second))
	*a = activationExpiry(time.Unix(sec, nsec).UTC())

	return nil
}

// ConnectorConfig holds metadata for externally-registered clusters.
// ActivationExpiry's wire emission is epoch seconds, built via .Time().Unix()
// in the handler layer (connectorConfigToJSON) -- see activationExpiry's doc
// comment for why the persisted (snapshot) shape is a separate concern.
type ConnectorConfig struct {
	ActivationExpiry activationExpiry `json:"activationExpiry"`
	ActivationCode   string           `json:"activationCode,omitempty"`
	ActivationID     string           `json:"activationId,omitempty"`
	Provider         string           `json:"provider,omitempty"`
	RoleARN          string           `json:"roleArn,omitempty"`
}

// Cluster represents an EKS cluster.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateCluster.
type Cluster struct {
	CreatedAt               time.Time                `json:"createdAt"`
	Tags                    *tags.Tags               `json:"tags,omitempty"`
	VpcConfig               *VpcConfig               `json:"resourcesVpcConfig,omitempty"`
	KubernetesNetworkConfig *KubernetesNetworkConfig `json:"kubernetesNetworkConfig,omitempty"`
	AccessConfig            *AccessConfig            `json:"accessConfig,omitempty"`
	ComputeConfig           *ComputeConfig           `json:"computeConfig,omitempty"`
	StorageConfig           *StorageConfig           `json:"storageConfig,omitempty"`
	ConnectorConfig         *ConnectorConfig         `json:"connectorConfig,omitempty"`
	ARN                     string                   `json:"arn"`
	Name                    string                   `json:"name"`
	Endpoint                string                   `json:"endpoint,omitempty"`
	OIDCIssuer              string                   `json:"oidcIssuer,omitempty"`
	Version                 string                   `json:"version"`
	Status                  string                   `json:"status"`
	RoleARN                 string                   `json:"roleArn,omitempty"`
	AccountID               string                   `json:"accountId"`
	Region                  string                   `json:"region"`
	PlatformVersion         string                   `json:"platformVersion,omitempty"`
	CertificateAuthority    string                   `json:"certificateAuthority,omitempty"`
	ClusterLogging          []ClusterLogEntry        `json:"clusterLogging,omitempty"`
	EncryptionConfig        []EncryptionConfig       `json:"encryptionConfig,omitempty"`
}

// NodegroupTaint represents a Kubernetes taint applied to managed nodes.
type NodegroupTaint struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect"`
}

// RemoteAccess captures SSH remote-access configuration for a node group.
type RemoteAccess struct {
	EC2SSHKey            string   `json:"ec2SshKey,omitempty"`
	SourceSecurityGroups []string `json:"sourceSecurityGroups,omitempty"`
}

// LaunchTemplate captures the launch-template reference for a node group.
type LaunchTemplate struct {
	ID      string `json:"id,omitempty"`
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// AutoScalingGroup holds the name of an ASG backing the node group.
type AutoScalingGroup struct {
	Name string `json:"name"`
}

// NodegroupResources captures AWS resources backing the node group.
type NodegroupResources struct {
	AutoScalingGroups []AutoScalingGroup `json:"autoScalingGroups,omitempty"`
}

// NodegroupUpdateConfig holds the nodegroup update strategy settings.
type NodegroupUpdateConfig struct {
	MaxUnavailable           *int32 `json:"maxUnavailable,omitempty"`
	MaxUnavailablePercentage *int32 `json:"maxUnavailablePercentage,omitempty"`
}

// Nodegroup represents an EKS managed node group.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via TagResource / CreateNodegroup.
type Nodegroup struct {
	CreatedAt      time.Time              `json:"createdAt"`
	Tags           *tags.Tags             `json:"tags,omitempty"`
	Labels         map[string]string      `json:"labels,omitempty"`
	RemoteAccess   *RemoteAccess          `json:"remoteAccess,omitempty"`
	LaunchTemplate *LaunchTemplate        `json:"launchTemplate,omitempty"`
	Resources      *NodegroupResources    `json:"resources,omitempty"`
	UpdateConfig   *NodegroupUpdateConfig `json:"updateConfig,omitempty"`
	CapacityType   string                 `json:"capacityType,omitempty"`
	Region         string                 `json:"region"`
	ARN            string                 `json:"nodegroupArn"`
	NodeRole       string                 `json:"nodeRole,omitempty"`
	Status         string                 `json:"status"`
	AMIType        string                 `json:"amiType,omitempty"`
	NodegroupName  string                 `json:"nodegroupName"`
	ClusterName    string                 `json:"clusterName"`
	Version        string                 `json:"version,omitempty"`
	ReleaseVersion string                 `json:"releaseVersion,omitempty"`
	AccountID      string                 `json:"accountId"`
	Taints         []NodegroupTaint       `json:"taints,omitempty"`
	InstanceTypes  []string               `json:"instanceTypes,omitempty"`
	Subnets        []string               `json:"subnets,omitempty"`
	DesiredSize    int32                  `json:"desiredSize"`
	MinSize        int32                  `json:"minSize"`
	MaxSize        int32                  `json:"maxSize"`
	DiskSize       int32                  `json:"diskSize,omitempty"`
}

// AccessEntry represents an EKS access entry that grants a principal access to a cluster.
type AccessEntry struct {
	CreatedAt        time.Time  `json:"createdAt"`
	ModifiedAt       time.Time  `json:"modifiedAt"`
	Tags             *tags.Tags `json:"tags,omitempty"`
	PrincipalARN     string     `json:"principalArn"`
	ClusterName      string     `json:"clusterName"`
	ARN              string     `json:"accessEntryArn"`
	Type             string     `json:"type"`
	Username         string     `json:"username,omitempty"`
	KubernetesGroups []string   `json:"kubernetesGroups,omitempty"`
}

// AccessPolicyAssociation represents an access policy associated with an access entry.
type AccessPolicyAssociation struct {
	AssociatedAt time.Time      `json:"associatedAt"`
	AccessScope  map[string]any `json:"accessScope,omitempty"`
	PolicyARN    string         `json:"policyArn"`
	ClusterName  string         `json:"clusterName"`
	PrincipalARN string         `json:"principalArn"`
}

// EncryptionConfig represents a cluster encryption configuration.
type EncryptionConfig struct {
	Provider  map[string]string `json:"provider,omitempty"`
	Resources []string          `json:"resources,omitempty"`
}

// IdentityProviderConfig represents an identity provider configuration for a cluster.
//
// OIDC holds the flat string-valued OIDC fields (issuerUrl, clientId,
// usernameClaim, usernamePrefix, groupsClaim, groupsPrefix); RequiredClaims
// is kept separate since the real
// aws-sdk-go-v2/service/eks.OidcIdentityProviderConfig.RequiredClaims is a
// nested map, not a flat string like the other OIDC fields.
type IdentityProviderConfig struct {
	CreatedAt      time.Time         `json:"createdAt"`
	Tags           *tags.Tags        `json:"tags,omitempty"`
	OIDC           map[string]string `json:"oidc,omitempty"`
	RequiredClaims map[string]string `json:"requiredClaims,omitempty"`
	ClusterName    string            `json:"clusterName"`
	Name           string            `json:"name"`
	ARN            string            `json:"arn"`
	Type           string            `json:"type"`
	Status         string            `json:"status"`
}

// AddonHealth represents the health status of an EKS managed add-on.
type AddonHealth struct {
	Issues []map[string]string `json:"issues,omitempty"`
}

// Addon represents an EKS managed add-on.
type Addon struct {
	CreatedAt               time.Time    `json:"createdAt"`
	Health                  *AddonHealth `json:"health,omitempty"`
	Tags                    *tags.Tags   `json:"tags,omitempty"`
	ARN                     string       `json:"addonArn"`
	ClusterName             string       `json:"clusterName"`
	AddonName               string       `json:"addonName"`
	AddonVersion            string       `json:"addonVersion,omitempty"`
	MarketplaceVersion      string       `json:"marketplaceVersion,omitempty"`
	Status                  string       `json:"status"`
	ServiceAccountRoleARN   string       `json:"serviceAccountRoleArn,omitempty"`
	Configuration           string       `json:"configurationValues,omitempty"`
	ResolveConflicts        string       `json:"resolveConflicts,omitempty"`
	Namespace               string       `json:"namespace,omitempty"`
	PodIdentityAssociations []string     `json:"podIdentityAssociations,omitempty"`
}

// CapabilityIssue represents a single health issue affecting a Capability.
type CapabilityIssue struct {
	Code        string   `json:"code,omitempty"`
	Message     string   `json:"message,omitempty"`
	ResourceIDs []string `json:"resourceIds,omitempty"`
}

// CapabilityHealth mirrors aws-sdk-go-v2/service/eks/types.CapabilityHealth.
type CapabilityHealth struct {
	Issues []CapabilityIssue `json:"issues"`
}

// SsoIdentity mirrors aws-sdk-go-v2/service/eks/types.SsoIdentity
// (types.go:3200) -- both Id and Type are required members.
type SsoIdentity struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// ArgoCdRoleMapping mirrors aws-sdk-go-v2/service/eks/types.ArgoCdRoleMapping
// (types.go:474) -- Role and Identities are both required members.
type ArgoCdRoleMapping struct {
	Role       string        `json:"role"`
	Identities []SsoIdentity `json:"identities"`
}

// ArgoCdAwsIdcConfig is this backend's stored form of the union of
// aws-sdk-go-v2/service/eks/types.ArgoCdAwsIdcConfigRequest (types.go:348,
// IdcInstanceArn/IdcRegion) and ArgoCdAwsIdcConfigResponse (types.go:365,
// adds IdcManagedApplicationArn). IdcManagedApplicationArn is real AWS's
// server-computed ARN of the IAM Identity Center managed application EKS
// creates for the capability -- neither the SDK doc comment nor
// https://docs.aws.amazon.com/eks/latest/userguide/capabilities.html
// (WebFetch'd 2026-09-11) document a derivable ARN pattern, so it is left
// empty here rather than fabricated (see PARITY.md gaps).
type ArgoCdAwsIdcConfig struct {
	IdcInstanceArn           string `json:"idcInstanceArn,omitempty"`
	IdcManagedApplicationArn string `json:"idcManagedApplicationArn,omitempty"`
	IdcRegion                string `json:"idcRegion,omitempty"`
}

// ArgoCdNetworkAccessConfig mirrors
// aws-sdk-go-v2/service/eks/types.ArgoCdNetworkAccessConfigRequest/Response
// (types.go:446,461) -- both carry only VpceIDs.
type ArgoCdNetworkAccessConfig struct {
	VpceIDs []string `json:"vpceIds,omitempty"`
}

// ArgoCdConfig is this backend's stored form of the union of
// aws-sdk-go-v2/service/eks/types.ArgoCdConfigRequest (types.go:386) and
// ArgoCdConfigResponse (types.go:418). ServerURL is real AWS's
// server-computed Argo CD web/API URL -- no derivation pattern is documented
// in the SDK doc comment or the EKS user guide's capabilities pages
// (WebFetch'd 2026-09-11), so it is left empty here rather than fabricated
// (see PARITY.md gaps).
type ArgoCdConfig struct {
	AwsIdc           *ArgoCdAwsIdcConfig        `json:"awsIdc,omitempty"`
	NetworkAccess    *ArgoCdNetworkAccessConfig `json:"networkAccess,omitempty"`
	Namespace        string                     `json:"namespace,omitempty"`
	ServerURL        string                     `json:"serverUrl,omitempty"`
	RbacRoleMappings []ArgoCdRoleMapping        `json:"rbacRoleMappings,omitempty"`
}

// CapabilityConfiguration is this backend's typed replacement for the
// former untyped map[string]any Configuration passthrough (gopherstack-wf8f
// item 1). Only ArgoCd is modeled: aws-sdk-go-v2/service/eks@v1.98.0's
// CapabilityConfigurationRequest/Response (types.go:645,655) carry only an
// ArgoCd member -- this pinned SDK version has no typed Configuration
// schema for ACK or KRO capabilities at all despite CapabilityType having
// ACK/ARGOCD/KRO values (confirmed: types.go has zero "Ack"/"Kro"-prefixed
// struct types beyond the CapabilityType enum values themselves). A
// CreateCapability/UpdateCapability for an ACK or KRO capability therefore
// carries no Configuration on the wire in this SDK version, matching real
// AWS's own shape.
type CapabilityConfiguration struct {
	ArgoCd *ArgoCdConfig `json:"argoCd,omitempty"`
}

// Capability represents an EKS capability. Capabilities are cluster-scoped:
// CapabilityName is unique per cluster, not globally (verified against
// aws-sdk-go-v2/service/eks -- CreateCapabilityInput requires ClusterName,
// CapabilityName, Type, RoleArn, and DeletePropagationPolicy; the route is
// /clusters/{clusterName}/capabilities[/{capabilityName}]).
type Capability struct {
	CreatedAt               time.Time                `json:"createdAt"`
	ModifiedAt              time.Time                `json:"modifiedAt"`
	Tags                    *tags.Tags               `json:"tags,omitempty"`
	Configuration           *CapabilityConfiguration `json:"configuration,omitempty"`
	Health                  *CapabilityHealth        `json:"health,omitempty"`
	ClusterName             string                   `json:"clusterName"`
	CapabilityName          string                   `json:"capabilityName"`
	ARN                     string                   `json:"arn"`
	Type                    string                   `json:"type,omitempty"`
	RoleARN                 string                   `json:"roleArn,omitempty"`
	DeletePropagationPolicy string                   `json:"deletePropagationPolicy,omitempty"`
	Version                 string                   `json:"version,omitempty"`
	Status                  string                   `json:"status"`
}

// SubscriptionTerm holds the term duration/unit for an EKS Anywhere
// subscription (required on create -- verified against
// aws-sdk-go-v2/service/eks's CreateEksAnywhereSubscriptionInput.Term).
type SubscriptionTerm struct {
	Unit     string `json:"unit,omitempty"`
	Duration int32  `json:"duration,omitempty"`
}

// AnywhereSubscription represents an EKS Anywhere subscription.
type AnywhereSubscription struct {
	CreatedAt       time.Time         `json:"createdAt"`
	EffectiveDate   time.Time         `json:"effectiveDate"`
	ExpirationDate  time.Time         `json:"expirationDate"`
	Tags            *tags.Tags        `json:"tags,omitempty"`
	Term            *SubscriptionTerm `json:"term,omitempty"`
	ID              string            `json:"id"`
	ARN             string            `json:"arn"`
	Name            string            `json:"name"`
	Status          string            `json:"status"`
	LicenseType     string            `json:"licenseType,omitempty"`
	LicenseQuantity int32             `json:"licenseQuantity,omitempty"`
	AutoRenew       bool              `json:"autoRenew"`
}

// FargateProfileSelector is a namespace/labels selector for a Fargate profile.
type FargateProfileSelector struct {
	Labels    map[string]string `json:"labels,omitempty"`
	Namespace string            `json:"namespace"`
}

// FargateProfileIssue represents a single health issue reported for a Fargate profile.
type FargateProfileIssue struct {
	Code        string   `json:"code,omitempty"`
	Message     string   `json:"message,omitempty"`
	ResourceIDs []string `json:"resourceIds,omitempty"`
}

// FargateProfileHealth mirrors aws-sdk-go-v2/service/eks/types.FargateProfileHealth.
type FargateProfileHealth struct {
	Issues []FargateProfileIssue `json:"issues"`
}

// FargateProfile represents an EKS Fargate profile.
type FargateProfile struct {
	CreatedAt           time.Time                `json:"createdAt"`
	Tags                *tags.Tags               `json:"tags,omitempty"`
	Health              *FargateProfileHealth    `json:"health,omitempty"`
	Subnets             []string                 `json:"subnets,omitempty"`
	ClusterName         string                   `json:"clusterName"`
	FargateProfileName  string                   `json:"fargateProfileName"`
	ARN                 string                   `json:"fargateProfileArn"`
	PodExecutionRoleARN string                   `json:"podExecutionRoleArn,omitempty"`
	Status              string                   `json:"status"`
	Selectors           []FargateProfileSelector `json:"selectors,omitempty"`
}

// PodIdentityAssociation represents an EKS pod identity association.
type PodIdentityAssociation struct {
	CreatedAt          time.Time  `json:"createdAt"`
	ModifiedAt         time.Time  `json:"modifiedAt"`
	Tags               *tags.Tags `json:"tags,omitempty"`
	ClusterName        string     `json:"clusterName"`
	AssociationID      string     `json:"associationId"`
	ARN                string     `json:"associationArn"`
	Namespace          string     `json:"namespace"`
	ServiceAccount     string     `json:"serviceAccount"`
	RoleARN            string     `json:"roleArn,omitempty"`
	OwnerARN           string     `json:"ownerArn,omitempty"`
	ExternalID         string     `json:"externalId,omitempty"`
	Policy             string     `json:"policy,omitempty"`
	DisableSessionTags bool       `json:"disableSessionTags"`
}

// PodIdentityAssociationSpec is one entry of CreateAddonInput's or
// UpdateAddonInput's PodIdentityAssociations, matching
// types.AddonPodIdentityAssociations (RoleArn + ServiceAccount only -- no
// namespace).
type PodIdentityAssociationSpec struct {
	RoleARN        string
	ServiceAccount string
}

// Insight represents an EKS cluster insight.
// Insight represents an EKS cluster insight, derived honestly from state
// this backend actually has (see insights.go's deriveUpgradeReadinessInsights)
// -- gopherstack-wf8f item 2. ClusterName is backend-internal routing only:
// neither types.Insight nor types.InsightSummary carries it on the wire (the
// cluster is already identified by the URL path) -- see insightToJSON/
// insightToSummaryJSON, which both omit it.
type Insight struct {
	LastRefreshTime time.Time         `json:"lastRefreshTime"`
	LastTransition  time.Time         `json:"lastTransitionTime"`
	AdditionalInfo  map[string]string `json:"additionalInfo,omitempty"`
	ID              string            `json:"id"`
	ClusterName     string            `json:"clusterName"`
	Category        string            `json:"category"`
	Status          string            `json:"status"`
	// StatusReason mirrors types.InsightStatus.Reason ("Explanation on the
	// reasoning for the status of the resource") -- distinct from
	// Recommendation (types.Insight.Recommendation, "how to remediate").
	// Previously conflated: insightToJSON used Recommendation for both.
	StatusReason string `json:"statusReason,omitempty"`
	// KubernetesVersion mirrors types.Insight/InsightSummary.KubernetesVersion
	// ("The Kubernetes minor version associated with an insight if
	// applicable") -- honestly derivable now that insights are computed
	// from the cluster's real Version field, unlike the prior fabricated
	// model.
	KubernetesVersion string `json:"kubernetesVersion,omitempty"`
	// Name mirrors types.Insight/InsightSummary.Name -- a human-readable
	// label for the check this insight represents (e.g. "Kubernetes
	// version end of standard support"), analogous to how real EKS names
	// its own generated insight checks.
	Name           string `json:"name,omitempty"`
	Description    string `json:"description,omitempty"`
	Recommendation string `json:"recommendation,omitempty"`
}

// InsightsRefresh represents the cluster-level (singleton -- there is no
// per-refresh id in the real API) EKS insights refresh operation state.
type InsightsRefresh struct {
	StartedAt   time.Time `json:"startedAt"`
	EndedAt     time.Time `json:"endedAt,omitzero"`
	ClusterName string    `json:"clusterName"`
	Status      string    `json:"status"`
	Message     string    `json:"message,omitempty"`
}

// UpdateParam represents a single parameter changed by an EKS update operation.
type UpdateParam struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// UpdateError represents an error encountered during an EKS update.
type UpdateError struct {
	ErrorCode    string   `json:"errorCode"`
	ErrorMessage string   `json:"errorMessage"`
	ResourceIDs  []string `json:"resourceIds,omitempty"`
}

// Cancellation represents the latest cancellation state of an Update, present
// only when a cancellation was attempted (e.g. via CancelUpdate).
type Cancellation struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// Update represents an EKS update record. NodegroupName is backend-internal
// (not part of the real Update wire shape) -- it exists only so ListUpdates
// can honor ListUpdatesInput.NodegroupName. It carries a real json tag
// (gopherstack-34g03): verified against the pinned SDK
// (aws-sdk-go-v2/service/eks@v1.98.0 types/types.go:3257-3282) that real
// types.Update has no such member, and updateToJSON (handler_updates.go) --
// the actual wire converter for DescribeUpdate/ListUpdates -- builds the
// response map by hand and never includes it, so a real tag changes nothing
// about the wire. b.updates is registered directly on b.registry
// (store_setup.go) and Snapshot/Restore marshal Update as-is, so json:"-"
// here only dropped the field from persistence, leaving it empty on every
// restored Update and emptying the nodegroupName filter
// (handler_updates.go:286) for any pre-restart update.
type Update struct {
	CreatedAt     time.Time     `json:"createdAt"`
	Cancellation  *Cancellation `json:"cancellation,omitempty"`
	ID            string        `json:"id"`
	ClusterName   string        `json:"clusterName"`
	NodegroupName string        `json:"nodegroupName,omitempty"`
	Status        string        `json:"status"`
	Type          string        `json:"type"`
	Params        []UpdateParam `json:"params,omitempty"`
	Errors        []UpdateError `json:"errors,omitempty"`
}
