package ocicontainerengine

import (
	"encoding/json"
	"time"
)

// Cluster, node pool, node and work request states.
const (
	stateActive   = "ACTIVE"
	stateCreating = "CREATING"
	stateUpdating = "UPDATING"
	stateDeleting = "DELETING"
	stateDeleted  = "DELETED"
	stateFailed   = "FAILED"
)

// Cluster types and pod network (CNI) types.
const (
	typeBasic    = "BASIC_CLUSTER"
	typeEnhanced = "ENHANCED_CLUSTER"
	cniFlannel   = "FLANNEL_OVERLAY"
	cniNative    = "OCI_VCN_IP_NATIVE"
)

// Tags are the shared freeform and defined tag sets of a resource.
type Tags struct {
	FreeformTags map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags  map[string]map[string]any `json:"definedTags,omitempty"`
}

func (t *Tags) merge(in Tags) {
	if in.FreeformTags != nil {
		t.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		t.DefinedTags = in.DefinedTags
	}
}

// EndpointConfig is the cluster API endpoint placement.
type EndpointConfig struct {
	IsPublicIPEnabled  *bool                     `json:"isPublicIpEnabled,omitempty"`
	SecurityAttributes map[string]map[string]any `json:"securityAttributes,omitempty"`
	SubnetID           string                    `json:"subnetId,omitempty"`
	NsgIDs             []string                  `json:"nsgIds,omitempty"`
}

// Endpoints are the reachable Kubernetes API server addresses.
type Endpoints struct {
	Kubernetes          string `json:"kubernetes,omitempty"`
	PublicEndpoint      string `json:"publicEndpoint,omitempty"`
	PrivateEndpoint     string `json:"privateEndpoint,omitempty"`
	VcnHostnameEndpoint string `json:"vcnHostnameEndpoint,omitempty"`
}

// NetworkConfig is kubernetesNetworkConfig.
type NetworkConfig struct {
	PodsCidr     string `json:"podsCidr,omitempty"`
	ServicesCidr string `json:"servicesCidr,omitempty"`
}

// AddOns is the options.addOns block.
type AddOns struct {
	IsKubernetesDashboardEnabled *bool `json:"isKubernetesDashboardEnabled,omitempty"`
	IsTillerEnabled              *bool `json:"isTillerEnabled,omitempty"`
}

// Admission is options.admissionControllerOptions.
type Admission struct {
	IsPodSecurityPolicyEnabled *bool `json:"isPodSecurityPolicyEnabled,omitempty"`
}

// TagCfg carries the tags applied to provisioned resources (persistent volumes, load balancers).
type TagCfg struct {
	Tags
	BackendNsgIDs []string `json:"backendNsgIds,omitempty"`
}

// ClusterOptions is the cluster create and update options block.
type ClusterOptions struct {
	K8sNetwork        *NetworkConfig  `json:"kubernetesNetworkConfig,omitempty"`
	AddOns            *AddOns         `json:"addOns,omitempty"`
	Admission         *Admission      `json:"admissionControllerOptions,omitempty"`
	PersistentVolume  *TagCfg         `json:"persistentVolumeConfig,omitempty"`
	ServiceLb         *TagCfg         `json:"serviceLbConfig,omitempty"`
	OIDCAuth          json.RawMessage `json:"openIdConnectTokenAuthenticationConfig,omitempty"`
	OIDCDiscovery     json.RawMessage `json:"openIdConnectDiscovery,omitempty"`
	ServiceLbSubnetID []string        `json:"serviceLbSubnetIds,omitempty"`
	IPFamilies        []string        `json:"ipFamilies,omitempty"`
}

// KeyDetail references an image signing key.
type KeyDetail struct {
	KmsKeyID string `json:"kmsKeyId"`
}

// ImagePolicy is imagePolicyConfig.
type ImagePolicy struct {
	IsPolicyEnabled *bool       `json:"isPolicyEnabled,omitempty"`
	KeyDetails      []KeyDetail `json:"keyDetails,omitempty"`
}

// PodNet is a pod network option (cluster level: cniType only).
type PodNet struct {
	MaxPodsPerNode *int     `json:"maxPodsPerNode,omitempty"`
	CniType        string   `json:"cniType"`
	PodNsgIDs      []string `json:"podNsgIds,omitempty"`
	PodSubnetIDs   []string `json:"podSubnetIds,omitempty"`
}

// Cluster is a stored OKE cluster.
type Cluster struct {
	TimeCreated      time.Time       `json:"timeCreated"`
	Tags             Tags            `json:"tags"`
	TimeUpdated      *time.Time      `json:"timeUpdated,omitempty"`
	TimeDeleted      *time.Time      `json:"timeDeleted,omitempty"`
	TransitionAt     *time.Time      `json:"transitionAt,omitempty"`
	MigrateScheduled *time.Time      `json:"migrateScheduled,omitempty"`
	EndpointConfig   *EndpointConfig `json:"endpointConfig,omitempty"`
	Endpoints        Endpoints       `json:"endpoints"`
	VcnID            string          `json:"vcnId,omitempty"`
	Type             string          `json:"type"`
	DeletedByWR      string          `json:"deletedByWr,omitempty"`
	ID               string          `json:"id"`
	Name             string          `json:"name"`
	CompartmentID    string          `json:"compartmentId"`
	UpdatedByWR      string          `json:"updatedByWr,omitempty"`
	Version          string          `json:"version"`
	KmsKeyID         string          `json:"kmsKeyId,omitempty"`
	CreatedByWR      string          `json:"createdByWr"`
	State            string          `json:"state"`
	Details          string          `json:"details,omitempty"`
	Etag             string          `json:"etag"`
	CAPEM            string          `json:"caPem"`
	Token            string          `json:"token"`
	MigrateState     string          `json:"migrateState,omitempty"`
	Options          ClusterOptions  `json:"options"`
	ImagePolicy      ImagePolicy     `json:"imagePolicy"`
	PodNetworks      []PodNet        `json:"podNetworks"`
	Seq              int64           `json:"seq"`
}

// Placement is one nodeConfigDetails placement configuration.
type Placement struct {
	PreemptibleConfig json.RawMessage `json:"preemptibleNodeConfig,omitempty"`
	AvailabilityDom   string          `json:"availabilityDomain"`
	SubnetID          string          `json:"subnetId"`
	CapacityReserveID string          `json:"capacityReservationId,omitempty"`
	FaultDomains      []string        `json:"faultDomains,omitempty"`
}

// NodeConfig is nodeConfigDetails.
type NodeConfig struct {
	IsPvEncryption *bool   `json:"isPvEncryptionInTransitEnabled,omitempty"`
	PodNet         *PodNet `json:"nodePoolPodNetworkOptionDetails,omitempty"`
	Tags
	KmsKeyID   string      `json:"kmsKeyId,omitempty"`
	Placements []Placement `json:"placementConfigs"`
	NsgIDs     []string    `json:"nsgIds,omitempty"`
	Size       int         `json:"size"`
}

// NodeSource is nodeSourceDetails.
type NodeSource struct {
	BootVolumeGBs *int64 `json:"bootVolumeSizeInGBs,omitempty"`
	SourceType    string `json:"sourceType"`
	ImageID       string `json:"imageId"`
}

// ShapeConfig is nodeShapeConfig.
type ShapeConfig struct {
	Ocpus       *float64 `json:"ocpus,omitempty"`
	MemoryInGBs *float64 `json:"memoryInGBs,omitempty"`
}

// KeyValue is an initial node label.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Node is one worker node of a pool.
type Node struct {
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	Tags         Tags       `json:"tags"`
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	Version      string     `json:"version"`
	AD           string     `json:"availabilityDomain"`
	SubnetID     string     `json:"subnetId"`
	FaultDomain  string     `json:"faultDomain"`
	PrivateIP    string     `json:"privateIp,omitempty"`
	State        string     `json:"state"`
}

// NodePool is a stored node pool.
type NodePool struct {
	TimeCreated    time.Time         `json:"timeCreated"`
	TransitionAt   *time.Time        `json:"transitionAt,omitempty"`
	NodeConfig     *NodeConfig       `json:"nodeConfig,omitempty"`
	NodeSource     *NodeSource       `json:"nodeSource,omitempty"`
	ShapeConfig    *ShapeConfig      `json:"shapeConfig,omitempty"`
	QuantityPerSub *int              `json:"quantityPerSubnet,omitempty"`
	Tags           Tags              `json:"tags"`
	NodeMetadata   map[string]string `json:"nodeMetadata,omitempty"`
	Eviction       json.RawMessage   `json:"eviction,omitempty"`
	Cycling        json.RawMessage   `json:"cycling,omitempty"`
	ID             string            `json:"id"`
	ClusterID      string            `json:"clusterId"`
	CompartmentID  string            `json:"compartmentId"`
	Name           string            `json:"name"`
	Version        string            `json:"version"`
	Shape          string            `json:"shape"`
	ImageName      string            `json:"imageName,omitempty"`
	SSHKey         string            `json:"sshKey,omitempty"`
	State          string            `json:"state"`
	Etag           string            `json:"etag"`
	Labels         []KeyValue        `json:"labels,omitempty"`
	SubnetIDs      []string          `json:"subnetIds,omitempty"`
	Nodes          []*Node           `json:"nodes"`
	Seq            int64             `json:"seq"`
}

// WorkRequest records an asynchronous operation. FinishAt is nil while an engine start is pending.
type WorkRequest struct {
	Accepted      time.Time  `json:"accepted"`
	FinishAt      *time.Time `json:"finishAt,omitempty"`
	ID            string     `json:"id"`
	Operation     string     `json:"operation"`
	CompartmentID string     `json:"compartmentId"`
	EntityType    string     `json:"entityType"`
	EntityID      string     `json:"entityId"`
	EntityURI     string     `json:"entityUri"`
	Action        string     `json:"action"`
	Failure       string     `json:"failure,omitempty"`
	Seq           int64      `json:"seq"`
}
