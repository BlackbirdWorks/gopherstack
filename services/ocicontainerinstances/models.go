package ocicontainerinstances

import (
	"encoding/json"
	"time"
)

// Lifecycle states shared by container instances and containers.
const (
	stateCreating = "CREATING"
	stateUpdating = "UPDATING"
	stateActive   = "ACTIVE"
	stateInactive = "INACTIVE"
	stateDeleting = "DELETING"
	stateDeleted  = "DELETED"
	stateFailed   = "FAILED"
)

// Restart policies.
const (
	restartAlways    = "ALWAYS"
	restartNever     = "NEVER"
	restartOnFailure = "ON_FAILURE"
)

// Volume, health check and pull secret kinds.
const (
	volEmptyDir    = "EMPTYDIR"
	volConfigFile  = "CONFIGFILE"
	hcHTTP         = "HTTP"
	hcTCP          = "TCP"
	secretBasic    = "BASIC"
	secretVault    = "VAULT"
	healthHealthy  = "HEALTHY"
	healthUnhealty = "UNHEALTHY"
	healthUnknown  = "UNKNOWN"
	actionKill     = "KILL"
	actionNone     = "NONE"
	backingMemory  = "MEMORY"
	backingEphem   = "EPHEMERAL_STORAGE"
)

// ConfigFile is one file of a CONFIGFILE volume; Data is base64.
type ConfigFile struct {
	FileName string `json:"fileName"`
	Data     string `json:"data"`
	Path     string `json:"path,omitempty"`
}

// Volume is a container instance volume.
type Volume struct {
	Name         string       `json:"name"`
	Type         string       `json:"type"`
	BackingStore string       `json:"backingStore,omitempty"`
	Configs      []ConfigFile `json:"configs,omitempty"`
}

// VolumeMount mounts a volume into a container.
type VolumeMount struct {
	MountPath  string `json:"mountPath"`
	VolumeName string `json:"volumeName"`
	SubPath    string `json:"subPath,omitempty"`
	ReadOnly   *bool  `json:"isReadOnly,omitempty"`
	Partition  *int   `json:"partition,omitempty"`
}

// HealthHeader is an HTTP health check header.
type HealthHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// HealthCheck is a container health check and its observed status.
type HealthCheck struct {
	LastChecked   *time.Time     `json:"lastChecked,omitempty"`
	Name          string         `json:"name,omitempty"`
	Type          string         `json:"type"`
	Path          string         `json:"path,omitempty"`
	Status        string         `json:"status"`
	StatusDetails string         `json:"statusDetails,omitempty"`
	FailureAction string         `json:"failureAction"`
	Headers       []HealthHeader `json:"headers,omitempty"`
	Port          int            `json:"port"`
	InitialDelay  int            `json:"initialDelayInSeconds"`
	Interval      int            `json:"intervalInSeconds"`
	FailureThresh int            `json:"failureThreshold"`
	SuccessThresh int            `json:"successThreshold"`
	Timeout       int            `json:"timeoutInSeconds"`
}

// ResourceConfig caps a container's resources.
type ResourceConfig struct {
	VcpusLimit       *float32 `json:"vcpusLimit,omitempty"`
	MemoryLimitInGBs *float32 `json:"memoryLimitInGBs,omitempty"`
}

// Container is one container of a container instance.
type Container struct {
	Created       time.Time         `json:"timeCreated"`
	Updated       *time.Time        `json:"timeUpdated,omitempty"`
	Terminated    *time.Time        `json:"timeTerminated,omitempty"`
	ExitCode      *int              `json:"exitCode,omitempty"`
	Resource      *ResourceConfig   `json:"resourceConfig,omitempty"`
	Env           map[string]string `json:"environmentVariables,omitempty"`
	Freeform      map[string]string `json:"freeformTags,omitempty"`
	Defined       map[string]any    `json:"definedTags,omitempty"`
	ID            string            `json:"id"`
	DisplayName   string            `json:"displayName"`
	ImageURL      string            `json:"imageUrl"`
	State         string            `json:"lifecycleState"`
	Details       string            `json:"lifecycleDetails,omitempty"`
	WorkingDir    string            `json:"workingDirectory,omitempty"`
	Security      json.RawMessage   `json:"securityContext,omitempty"`
	Command       []string          `json:"command,omitempty"`
	Args          []string          `json:"arguments,omitempty"`
	Mounts        []VolumeMount     `json:"volumeMounts,omitempty"`
	Health        []*HealthCheck    `json:"healthChecks,omitempty"`
	RestartCount  int               `json:"restartCount"`
	NoResourcePrn bool              `json:"isResourcePrincipalDisabled"`
}

// PullSecret is an image pull secret; BASIC credentials never leave the backend.
type PullSecret struct {
	Type     string `json:"type"`
	Endpoint string `json:"registryEndpoint"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	SecretID string `json:"secretId,omitempty"`
}

// DNSConfig is a container instance DNS configuration.
type DNSConfig struct {
	Nameservers []string `json:"nameservers,omitempty"`
	Searches    []string `json:"searches,omitempty"`
	Options     []string `json:"options,omitempty"`
}

// Vnic is a container instance VNIC.
type Vnic struct {
	VnicID   string `json:"vnicId"`
	SubnetID string `json:"subnetId"`
}

// ShapeConfig is the compute size of an instance.
type ShapeConfig struct {
	Processor string  `json:"processorDescription"`
	Ocpus     float32 `json:"ocpus"`
	Memory    float32 `json:"memoryInGBs"`
	Bandwidth float32 `json:"networkingBandwidthInGbps"`
}

// Instance is a container instance.
type Instance struct {
	Created      time.Time         `json:"timeCreated"`
	Updated      *time.Time        `json:"timeUpdated,omitempty"`
	TransitionAt *time.Time        `json:"transitionAt,omitempty"`
	Graceful     *int64            `json:"gracefulShutdownTimeoutInSeconds,omitempty"`
	DNS          *DNSConfig        `json:"dnsConfig,omitempty"`
	Freeform     map[string]string `json:"freeformTags,omitempty"`
	Defined      map[string]any    `json:"definedTags,omitempty"`
	Security     json.RawMessage   `json:"securityContext,omitempty"`
	ID           string            `json:"id"`
	DisplayName  string            `json:"displayName"`
	Compartment  string            `json:"compartmentId"`
	AD           string            `json:"availabilityDomain"`
	FaultDomain  string            `json:"faultDomain,omitempty"`
	State        string            `json:"lifecycleState"`
	Details      string            `json:"lifecycleDetails,omitempty"`
	Shape        string            `json:"shape"`
	Restart      string            `json:"containerRestartPolicy"`
	Etag         string            `json:"etag"`
	Target       string            `json:"target,omitempty"`
	RetryToken   string            `json:"retryToken,omitempty"`
	ShapeConfig  ShapeConfig       `json:"shapeConfig"`
	Containers   []*Container      `json:"containers"`
	Volumes      []Volume          `json:"volumes,omitempty"`
	Vnics        []Vnic            `json:"vnics"`
	PullSecrets  []PullSecret      `json:"imagePullSecrets,omitempty"`
}

// WorkRequest is an asynchronous operation record.
type WorkRequest struct {
	Accepted     time.Time  `json:"timeAccepted"`
	FinishAt     *time.Time `json:"finishAt,omitempty"`
	FinishedAt   *time.Time `json:"timeFinished,omitempty"`
	ID           string     `json:"id"`
	Type         string     `json:"operationType"`
	Compartment  string     `json:"compartmentId"`
	ResourceID   string     `json:"resourceId"`
	ResourceType string     `json:"resourceType"`
	Action       string     `json:"actionType"`
	Status       string     `json:"status"`
	ErrCode      string     `json:"errorCode,omitempty"`
	ErrMsg       string     `json:"errorMessage,omitempty"`
	Seq          int64      `json:"seq"`
}
