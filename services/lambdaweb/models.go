package lambdaweb

import (
	"maps"
	"slices"
	"time"
)

// Resource type names used in error payloads and ARNs.
const (
	resFunction = "web-function"
	resRevision = "revision"
	resEndpoint = "endpoint"
	resPolicy   = "resource-policy"
)

// Lifecycle states shared by the SDK enums (types.FunctionState, RevisionState, EndpointState).
const (
	statePending  = "Pending"
	stateActive   = "Active"
	stateFailed   = "Failed"
	stateDeleting = "Deleting"
)

// Endpoint update statuses (types.EndpointUpdateStatus).
const (
	updateInProgress = "InProgress"
	updateSuccessful = "Successful"
)

// Endpoint types, auth types and deployment modes (types enums).
const (
	endpointHomeRegion  = "HomeRegion"
	endpointMultiRegion = "MultiRegion"
	endpointPerRegion   = "PerRegion"

	authApplicationManaged = "ApplicationManaged"
	authIam                = "IamAuth"

	modeLatestRevision = "LatestRevision"
	modeDisabled       = "Disabled"
)

// RevisionWeight routes Weight percent of an endpoint's traffic to a revision.
type RevisionWeight struct {
	RevisionID string
	Weight     int32
}

// S3Object locates a revision's deployment package.
type S3Object struct {
	Bucket    string
	Key       string
	VersionID string
}

// LoggingConfig is a revision's logging configuration.
type LoggingConfig struct {
	ApplicationLogLevel string
	LogGroup            string
	SystemLogLevel      string
}

// ServiceConfig is a revision's runtime configuration.
type ServiceConfig struct {
	EnvironmentVariables         map[string]string
	Logging                      *LoggingConfig
	ExecutionRoleArn             string
	EgressNetworkConnectorArn    string
	MaxConcurrencyPerEnvironment int32
	TimeoutSeconds               int32
}

// RevisionError is one error found while creating a revision.
type RevisionError struct {
	Attribute    string
	ErrorCode    string
	ErrorMessage string
}

// Function is a web function.
type Function struct {
	CreatedAt    time.Time
	UpdatedAt    time.Time
	PendingUntil time.Time
	Tags         map[string]string
	Region       string
	AccountID    string
	Name         string
	ARN          string
	State        string
	StateReason  string
	Policy       string
	PolicyRevID  string
}

// Revision is an immutable snapshot of a web function's code and configuration.
type Revision struct {
	PendingUntil  time.Time
	CreatedAt     time.Time
	S3            S3Object
	ARN           string
	Region        string
	AccountID     string
	FunctionName  string
	ID            string
	Description   string
	KmsKeyArn     string
	Runtime       string
	State         string
	StateReason   string
	ServiceConfig ServiceConfig
	Errors        []RevisionError
}

// Endpoint is a network entry point routing traffic to a function's revisions.
type Endpoint struct {
	CreatedAt          time.Time
	UpdatedAt          time.Time
	PendingUntil       time.Time
	UpdatePendingUntil time.Time
	RegionDomains      map[string]string
	MaxEnvironments    *int32
	RateLimit          *int32
	AccountID          string
	Description        string
	Region             string
	UpdateReason       string
	FunctionName       string
	Name               string
	ARN                string
	UpdateStatus       string
	Type               string
	AuthType           string
	AutoDeploymentMode string
	DomainName         string
	State              string
	StateReason        string
	Weights            []RevisionWeight
	Regions            []string
}

// AccountSettings reports the Region's quotas and usage.
type AccountSettings struct {
	MaxEndpointsPerFunction int32
	MaxRevisionsPerFunction int32
	MaxTotalArmVCpus        int32
	MaxTotalRateLimit       int32
	FunctionCount           int32
}

// ResourcePolicy is a function's resource-based policy.
type ResourcePolicy struct {
	Policy     string
	RevisionID string
}

func (f *Function) clone() *Function {
	cp := *f
	cp.Tags = maps.Clone(f.Tags)

	return &cp
}

func (r *Revision) clone() *Revision {
	cp := *r
	cp.Errors = slices.Clone(r.Errors)
	cp.ServiceConfig.EnvironmentVariables = maps.Clone(r.ServiceConfig.EnvironmentVariables)

	if r.ServiceConfig.Logging != nil {
		lc := *r.ServiceConfig.Logging
		cp.ServiceConfig.Logging = &lc
	}

	return &cp
}

func (e *Endpoint) clone() *Endpoint {
	cp := *e
	cp.Regions = slices.Clone(e.Regions)
	cp.Weights = slices.Clone(e.Weights)
	cp.RegionDomains = maps.Clone(e.RegionDomains)

	if e.MaxEnvironments != nil {
		v := *e.MaxEnvironments
		cp.MaxEnvironments = &v
	}

	if e.RateLimit != nil {
		v := *e.RateLimit
		cp.RateLimit = &v
	}

	return &cp
}
