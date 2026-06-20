// Package codebuild provides an in-memory implementation of the AWS CodeBuild service.
package codebuild

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	buildStatusSucceeded  = "SUCCEEDED"
	buildStatusInProgress = "IN_PROGRESS"
	buildStatusStopped    = "STOPPED"
	phaseSubmitted        = "SUBMITTED"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same name already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when request input fails validation.
	ErrValidation = awserr.New("InvalidInputException", awserr.ErrInvalidParameter)
)

// SourceAuth represents authentication for a CodeBuild source.
type SourceAuth struct {
	Type     string `json:"type,omitempty"`
	Resource string `json:"resource,omitempty"`
}

// ProjectSource represents the source configuration for a CodeBuild project.
type ProjectSource struct {
	Auth              SourceAuth `json:"auth,omitzero"`
	Type              string     `json:"type"`
	Location          string     `json:"location,omitempty"`
	Buildspec         string     `json:"buildspec,omitempty"`
	SourceIdentifier  string     `json:"sourceIdentifier,omitempty"`
	GitCloneDepth     int32      `json:"gitCloneDepth,omitempty"`
	InsecureSsl       bool       `json:"insecureSsl,omitempty"`
	ReportBuildStatus bool       `json:"reportBuildStatus,omitempty"`
}

// ProjectSourceVersion pairs a source identifier with a specific version.
type ProjectSourceVersion struct {
	SourceIdentifier string `json:"sourceIdentifier"`
	SourceVersion    string `json:"sourceVersion"`
}

// ProjectArtifacts represents the artifacts configuration for a CodeBuild project.
type ProjectArtifacts struct {
	Type                 string `json:"type"`
	Location             string `json:"location,omitempty"`
	Path                 string `json:"path,omitempty"`
	NamespaceType        string `json:"namespaceType,omitempty"`
	Name                 string `json:"name,omitempty"`
	Packaging            string `json:"packaging,omitempty"`
	ArtifactIdentifier   string `json:"artifactIdentifier,omitempty"`
	BucketOwnerAccess    string `json:"bucketOwnerAccess,omitempty"`
	OverrideArtifactName bool   `json:"overrideArtifactName,omitempty"`
	EncryptionDisabled   bool   `json:"encryptionDisabled,omitempty"`
}

// EnvironmentVariable represents an environment variable for a build environment.
type EnvironmentVariable struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type,omitempty"` // PLAINTEXT|PARAMETER_STORE|SECRETS_MANAGER
}

// RegistryCredential holds credentials for a private Docker registry.
type RegistryCredential struct {
	Credential         string `json:"credential"`
	CredentialProvider string `json:"credentialProvider"`
}

// ProjectEnvironment represents the build environment for a CodeBuild project.
type ProjectEnvironment struct {
	RegistryCredential       *RegistryCredential   `json:"registryCredential,omitempty"`
	Type                     string                `json:"type"`
	Image                    string                `json:"image"`
	ComputeType              string                `json:"computeType"`
	Certificate              string                `json:"certificate,omitempty"`
	ImagePullCredentialsType string                `json:"imagePullCredentialsType,omitempty"`
	EnvironmentVariables     []EnvironmentVariable `json:"environmentVariables,omitempty"`
	PrivilegedMode           bool                  `json:"privilegedMode,omitempty"`
}

// ProjectCache represents the cache configuration for a CodeBuild project.
type ProjectCache struct {
	Type     string   `json:"type"` // NO_CACHE|S3|LOCAL
	Location string   `json:"location,omitempty"`
	Modes    []string `json:"modes,omitempty"`
}

// CloudWatchLogsConfig represents CloudWatch Logs configuration.
type CloudWatchLogsConfig struct {
	Status     string `json:"status"` // ENABLED|DISABLED
	GroupName  string `json:"groupName,omitempty"`
	StreamName string `json:"streamName,omitempty"`
}

// S3LogsConfig represents S3 log configuration.
type S3LogsConfig struct {
	Status             string `json:"status"` // ENABLED|DISABLED
	Location           string `json:"location,omitempty"`
	BucketOwnerAccess  string `json:"bucketOwnerAccess,omitempty"`
	EncryptionDisabled bool   `json:"encryptionDisabled,omitempty"`
}

// LogsConfig represents the logs configuration for a CodeBuild project.
type LogsConfig struct {
	CloudWatchLogs CloudWatchLogsConfig `json:"cloudWatchLogs,omitzero"`
	S3Logs         S3LogsConfig         `json:"s3Logs,omitzero"`
}

// VpcConfig represents VPC configuration for a CodeBuild project.
type VpcConfig struct {
	VpcID            string   `json:"vpcId,omitempty"`
	Subnets          []string `json:"subnets,omitempty"`
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`
}

// BatchRestrictions represents restrictions on batch builds.
type BatchRestrictions struct {
	ComputeTypesAllowed  []string `json:"computeTypesAllowed,omitempty"`
	MaximumBuildsAllowed int32    `json:"maximumBuildsAllowed,omitempty"`
}

// BuildBatchConfig represents batch build configuration for a project.
type BuildBatchConfig struct {
	ServiceRole      string            `json:"serviceRole,omitempty"`
	BatchReportMode  string            `json:"batchReportMode,omitempty"`
	Restrictions     BatchRestrictions `json:"restrictions,omitzero"`
	TimeoutInMins    int32             `json:"timeoutInMins,omitempty"`
	CombineArtifacts bool              `json:"combineArtifacts,omitempty"`
}

// ProjectBadge represents the build badge for a project.
type ProjectBadge struct {
	BadgeRequestURL string `json:"badgeRequestUrl,omitempty"`
	BadgeEnabled    bool   `json:"badgeEnabled,omitempty"`
}

// FileSystemLocation represents an EFS file system mount for a project.
type FileSystemLocation struct {
	Identifier   string `json:"identifier,omitempty"`
	Location     string `json:"location,omitempty"`
	Type         string `json:"type,omitempty"` // EFS
	MountPoint   string `json:"mountPoint,omitempty"`
	MountOptions string `json:"mountOptions,omitempty"`
}

// Project represents an in-memory AWS CodeBuild project.
type Project struct {
	Cache                   *ProjectCache          `json:"cache,omitempty"`
	Tags                    map[string]string      `json:"tags,omitempty"`
	Badge                   *ProjectBadge          `json:"badge,omitempty"`
	BuildBatchConfig        *BuildBatchConfig      `json:"buildBatchConfig,omitempty"`
	VpcConfig               *VpcConfig             `json:"vpcConfig,omitempty"`
	LogsConfig              *LogsConfig            `json:"logsConfig,omitempty"`
	Name                    string                 `json:"name"`
	Description             string                 `json:"description,omitempty"`
	ServiceRole             string                 `json:"serviceRole,omitempty"`
	EncryptionKey           string                 `json:"encryptionKey,omitempty"`
	Arn                     string                 `json:"arn"`
	Visibility              string                 `json:"projectVisibility,omitempty"`
	ResourceAccessRole      string                 `json:"resourceAccessRole,omitempty"`
	Artifacts               ProjectArtifacts       `json:"artifacts"`
	Source                  ProjectSource          `json:"source"`
	FileSystemLocations     []FileSystemLocation   `json:"fileSystemLocations,omitempty"`
	SecondarySourceVersions []ProjectSourceVersion `json:"secondarySourceVersions,omitempty"`
	SecondaryArtifacts      []ProjectArtifacts     `json:"secondaryArtifacts,omitempty"`
	SecondarySources        []ProjectSource        `json:"secondarySources,omitempty"`
	Environment             ProjectEnvironment     `json:"environment"`
	Created                 float64                `json:"created,omitempty"`
	LastModified            float64                `json:"lastModified,omitempty"`
	TimeoutInMinutes        int32                  `json:"timeoutInMinutes,omitempty"`
	QueuedTimeoutInMinutes  int32                  `json:"queuedTimeoutInMinutes,omitempty"`
	ConcurrentBuildLimit    int32                  `json:"concurrentBuildLimit,omitempty"`
	AutoRetryLimit          int32                  `json:"autoRetryLimit,omitempty"`
}

// BuildPhaseContext represents a context entry within a build phase.
type BuildPhaseContext struct {
	Message    string `json:"message,omitempty"`
	StatusCode string `json:"statusCode,omitempty"`
}

// BuildPhase represents a single phase in the build lifecycle.
type BuildPhase struct {
	PhaseType         string              `json:"phaseType"`
	PhaseStatus       string              `json:"phaseStatus,omitempty"`
	Contexts          []BuildPhaseContext `json:"contexts,omitempty"`
	StartTime         float64             `json:"startTime,omitempty"`
	EndTime           float64             `json:"endTime,omitempty"`
	DurationInSeconds float64             `json:"durationInSeconds,omitempty"`
}

// BuildLogs represents the log locations for a build.
type BuildLogs struct {
	CloudWatchLogsArn string `json:"cloudWatchLogsArn,omitempty"`
	S3LogsArn         string `json:"s3LogsArn,omitempty"`
	GroupName         string `json:"groupName,omitempty"`
	StreamName        string `json:"streamName,omitempty"`
	S3Location        string `json:"s3Location,omitempty"`
	DeepLink          string `json:"deepLink,omitempty"`
}

// Build represents an in-memory AWS CodeBuild build execution.
type Build struct {
	Source                 *ProjectSource      `json:"source,omitempty"`
	Tags                   map[string]string   `json:"tags,omitempty"`
	Logs                   *BuildLogs          `json:"logs,omitempty"`
	Artifacts              *ProjectArtifacts   `json:"artifacts,omitempty"`
	Environment            *ProjectEnvironment `json:"environment,omitempty"`
	CurrentPhase           string              `json:"currentPhase,omitempty"`
	Initiator              string              `json:"initiator,omitempty"`
	Arn                    string              `json:"arn"`
	ProjectName            string              `json:"projectName"`
	BuildStatus            string              `json:"buildStatus"`
	ServiceRole            string              `json:"serviceRole,omitempty"`
	ResolvedSourceVersion  string              `json:"resolvedSourceVersion,omitempty"`
	ID                     string              `json:"id"`
	EncryptionKey          string              `json:"encryptionKey,omitempty"`
	Phases                 []BuildPhase        `json:"phases,omitempty"`
	BuildNumber            int64               `json:"buildNumber,omitempty"`
	StartTime              float64             `json:"startTime,omitempty"`
	EndTime                float64             `json:"endTime,omitempty"`
	TimeoutInMinutes       int32               `json:"timeoutInMinutes,omitempty"`
	QueuedTimeoutInMinutes int32               `json:"queuedTimeoutInMinutes,omitempty"`
	BuildComplete          bool                `json:"buildComplete,omitempty"`
}

// ReportExportConfig represents the export configuration for a CodeBuild report group.
type ReportExportConfig struct {
	ExportConfigType string `json:"exportConfigType,omitempty"`
}

// ReportGroup represents an in-memory AWS CodeBuild report group.
type ReportGroup struct {
	Tags         map[string]string  `json:"tags,omitempty"`
	ExportConfig ReportExportConfig `json:"exportConfig"`
	Arn          string             `json:"arn"`
	Name         string             `json:"name"`
	Type         string             `json:"type"`
	Status       string             `json:"status"`
	Created      float64            `json:"created,omitempty"`
	LastModified float64            `json:"lastModified,omitempty"`
}

// Report represents an in-memory AWS CodeBuild report.
type Report struct {
	Arn            string  `json:"arn"`
	ReportGroupArn string  `json:"reportGroupArn,omitempty"`
	ExecutionID    string  `json:"executionId,omitempty"`
	Type           string  `json:"type,omitempty"`
	Status         string  `json:"status"`
	Created        float64 `json:"created,omitempty"`
	Expired        float64 `json:"expired,omitempty"`
}

// FleetStatus represents the operational status of a compute fleet.
type FleetStatus struct {
	StatusCode string `json:"statusCode,omitempty"`
	Context    string `json:"context,omitempty"`
	Message    string `json:"message,omitempty"`
}

// ScalingConfiguration represents the scaling settings for a compute fleet.
type ScalingConfiguration struct {
	ScalingType     string `json:"scalingType,omitempty"`
	MaxCapacity     int32  `json:"maxCapacity,omitempty"`
	DesiredCapacity int32  `json:"desiredCapacity,omitempty"`
}

// Fleet represents an in-memory AWS CodeBuild compute fleet.
type Fleet struct {
	Tags                 map[string]string     `json:"tags,omitempty"`
	Status               *FleetStatus          `json:"status,omitempty"`
	ScalingConfiguration *ScalingConfiguration `json:"scalingConfiguration,omitempty"`
	Arn                  string                `json:"arn"`
	Name                 string                `json:"name"`
	FleetServiceRole     string                `json:"fleetServiceRole,omitempty"`
	OverflowBehavior     string                `json:"overflowBehavior,omitempty"` // QUEUE|ON_DEMAND
	ComputeType          string                `json:"computeType,omitempty"`
	EnvironmentType      string                `json:"environmentType,omitempty"`
	BaseCapacity         int32                 `json:"baseCapacity"`
	Created              float64               `json:"created,omitempty"`
	LastModified         float64               `json:"lastModified,omitempty"`
}

// BuildBatch represents an in-memory AWS CodeBuild build batch.
type BuildBatch struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ID               string            `json:"id"`
	Arn              string            `json:"arn"`
	ProjectName      string            `json:"projectName"`
	BuildBatchStatus string            `json:"buildBatchStatus"`
	StartTime        float64           `json:"startTime,omitempty"`
	EndTime          float64           `json:"endTime,omitempty"`
}

// CommandExecution represents an in-memory AWS CodeBuild command execution.
type CommandExecution struct {
	ID                    string  `json:"id"`
	SandboxID             string  `json:"sandboxId"`
	SandboxArn            string  `json:"sandboxArn,omitempty"`
	Command               string  `json:"command,omitempty"`
	Type                  string  `json:"type,omitempty"` // SHELL
	Status                string  `json:"status"`
	StandardOutputContent string  `json:"standardOutputContent,omitempty"`
	StandardErrorContent  string  `json:"standardErrorContent,omitempty"`
	ExitCode              int32   `json:"exitCode,omitempty"`
	StartTime             float64 `json:"startTime,omitempty"`
	EndTime               float64 `json:"endTime,omitempty"`
}

// Sandbox represents an in-memory AWS CodeBuild sandbox.
type Sandbox struct {
	ID          string  `json:"id"`
	Arn         string  `json:"arn"`
	ProjectName string  `json:"projectName,omitempty"`
	Status      string  `json:"status"` // QUEUED|PROVISIONING|READY|STARTING|STOPPED
	StartTime   float64 `json:"startTime,omitempty"`
	EndTime     float64 `json:"endTime,omitempty"`
}

// Webhook represents an in-memory AWS CodeBuild webhook.
type Webhook struct {
	ProjectName  string `json:"projectName"`
	URL          string `json:"url,omitempty"`
	BranchFilter string `json:"branchFilter,omitempty"`
	BuildType    string `json:"buildType,omitempty"`
}

// SourceCredentials represents imported source credentials.
type SourceCredentials struct {
	Arn        string `json:"arn"`
	ServerType string `json:"serverType"`
	AuthType   string `json:"authType"`
}

// CodeCoverage represents a code coverage entry returned by DescribeCodeCoverages.
type CodeCoverage struct {
	FilePath       string  `json:"filePath,omitempty"`
	BranchCoverage float64 `json:"branchCoverage,omitempty"`
	LineCoverage   float64 `json:"lineCoverage,omitempty"`
}

// TestCase represents a test case entry returned by DescribeTestCases.
type TestCase struct {
	Name     string  `json:"name,omitempty"`
	Status   string  `json:"status,omitempty"`
	Duration float64 `json:"duration,omitempty"`
}

// InMemoryBackend is a thread-safe in-memory store for CodeBuild resources.
type InMemoryBackend struct {
	projects            map[string]*Project
	builds              map[string]*Build
	buildsByProject     map[string]map[string]struct{} // project name → set of build full IDs
	projectARNIndex     map[string]string              // ARN → project name
	buildARNIndex       map[string]string              // ARN → build ID
	fleets              map[string]*Fleet              // name → Fleet
	fleetARNIndex       map[string]string              // ARN → name
	reportGroups        map[string]*ReportGroup        // name → ReportGroup
	reportGroupARNIndex map[string]string              // ARN → name
	reports             map[string]*Report             // ARN → Report
	buildBatches        map[string]*BuildBatch         // ID → BuildBatch
	commandExecutions   map[string]*CommandExecution   // ID → CommandExecution
	sandboxes           map[string]*Sandbox            // ID → Sandbox
	webhooks            map[string]*Webhook            // projectName → Webhook
	resourcePolicies    map[string]string              // ARN → policy JSON
	sourceCredentials   map[string]*SourceCredentials  // ARN → creds
	sandboxesByProject  map[string]map[string]struct{} // project name → sandbox ID set
	batchesByProject    map[string]map[string]struct{} // project name → batch ID set
	commandsBySandbox   map[string]map[string]struct{} // sandboxID → commandExecution ID set
	reportsByGroup      map[string]map[string]struct{} // reportGroupARN → report ARN set
	mu                  *lockmetrics.RWMutex
	accountID           string
	region              string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		projects:            make(map[string]*Project),
		builds:              make(map[string]*Build),
		buildsByProject:     make(map[string]map[string]struct{}),
		projectARNIndex:     make(map[string]string),
		buildARNIndex:       make(map[string]string),
		fleets:              make(map[string]*Fleet),
		fleetARNIndex:       make(map[string]string),
		reportGroups:        make(map[string]*ReportGroup),
		reportGroupARNIndex: make(map[string]string),
		reports:             make(map[string]*Report),
		buildBatches:        make(map[string]*BuildBatch),
		commandExecutions:   make(map[string]*CommandExecution),
		sandboxes:           make(map[string]*Sandbox),
		webhooks:            make(map[string]*Webhook),
		resourcePolicies:    make(map[string]string),
		sourceCredentials:   make(map[string]*SourceCredentials),
		sandboxesByProject:  make(map[string]map[string]struct{}),
		batchesByProject:    make(map[string]map[string]struct{}),
		commandsBySandbox:   make(map[string]map[string]struct{}),
		reportsByGroup:      make(map[string]map[string]struct{}),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("codebuild"),
	}
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state in the backend, resetting it to a pristine empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.projects = make(map[string]*Project)
	b.builds = make(map[string]*Build)
	b.buildsByProject = make(map[string]map[string]struct{})
	b.projectARNIndex = make(map[string]string)
	b.buildARNIndex = make(map[string]string)
	b.fleets = make(map[string]*Fleet)
	b.fleetARNIndex = make(map[string]string)
	b.reportGroups = make(map[string]*ReportGroup)
	b.reportGroupARNIndex = make(map[string]string)
	b.reports = make(map[string]*Report)
	b.buildBatches = make(map[string]*BuildBatch)
	b.commandExecutions = make(map[string]*CommandExecution)
	b.sandboxes = make(map[string]*Sandbox)
	b.webhooks = make(map[string]*Webhook)
	b.resourcePolicies = make(map[string]string)
	b.sourceCredentials = make(map[string]*SourceCredentials)
	b.sandboxesByProject = make(map[string]map[string]struct{})
	b.batchesByProject = make(map[string]map[string]struct{})
	b.commandsBySandbox = make(map[string]map[string]struct{})
	b.reportsByGroup = make(map[string]map[string]struct{})
}

func (b *InMemoryBackend) buildProjectARN(name string) string {
	return arn.Build("codebuild", b.region, b.accountID, "project/"+name)
}

func (b *InMemoryBackend) buildBuildARN(projectName, buildID string) string {
	return arn.Build("codebuild", b.region, b.accountID, "build/"+projectName+":"+buildID)
}

func randomID() string {
	return uuid.NewString()[:8]
}

// lookupByNameOrARN finds a project by name or by its ARN.
func (b *InMemoryBackend) lookupByNameOrARN(nameOrARN string) (*Project, bool) {
	if p, ok := b.projects[nameOrARN]; ok {
		return p, true
	}

	if name, ok := b.projectARNIndex[nameOrARN]; ok {
		return b.projects[name], true
	}

	return nil, false
}

// ProjectConfig holds all configurable fields for creating or updating a project.
type ProjectConfig struct {
	Cache                   *ProjectCache
	Source                  *ProjectSource
	Artifacts               *ProjectArtifacts
	Tags                    map[string]string
	BuildBatchConfig        *BuildBatchConfig
	VpcConfig               *VpcConfig
	LogsConfig              *LogsConfig
	Environment             *ProjectEnvironment
	EncryptionKey           string
	Name                    string
	Description             string
	ServiceRole             string
	ResourceAccessRole      string
	FileSystemLocations     []FileSystemLocation
	SecondarySourceVersions []ProjectSourceVersion
	SecondaryArtifacts      []ProjectArtifacts
	SecondarySources        []ProjectSource
	TimeoutInMinutes        int32
	QueuedTimeoutInMinutes  int32
	ConcurrentBuildLimit    int32
	AutoRetryLimit          int32
}

// CreateProject creates a new CodeBuild project.
func (b *InMemoryBackend) CreateProject(cfg ProjectConfig) (*Project, error) {
	b.mu.Lock("CreateProject")
	defer b.mu.Unlock()

	if cfg.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if _, exists := b.projects[cfg.Name]; exists {
		return nil, ErrAlreadyExists
	}

	tagsCopy := make(map[string]string, len(cfg.Tags))
	maps.Copy(tagsCopy, cfg.Tags)

	now := float64(time.Now().Unix())
	p := &Project{
		Name:                    cfg.Name,
		Arn:                     b.buildProjectARN(cfg.Name),
		Description:             cfg.Description,
		ServiceRole:             cfg.ServiceRole,
		EncryptionKey:           cfg.EncryptionKey,
		ResourceAccessRole:      cfg.ResourceAccessRole,
		TimeoutInMinutes:        cfg.TimeoutInMinutes,
		QueuedTimeoutInMinutes:  cfg.QueuedTimeoutInMinutes,
		ConcurrentBuildLimit:    cfg.ConcurrentBuildLimit,
		AutoRetryLimit:          cfg.AutoRetryLimit,
		Tags:                    tagsCopy,
		SecondarySources:        cfg.SecondarySources,
		SecondaryArtifacts:      cfg.SecondaryArtifacts,
		SecondarySourceVersions: cfg.SecondarySourceVersions,
		FileSystemLocations:     cfg.FileSystemLocations,
		Cache:                   cfg.Cache,
		LogsConfig:              cfg.LogsConfig,
		VpcConfig:               cfg.VpcConfig,
		BuildBatchConfig:        cfg.BuildBatchConfig,
		Created:                 now,
		LastModified:            now,
	}

	if cfg.Source != nil {
		p.Source = *cfg.Source
	}

	if cfg.Artifacts != nil {
		p.Artifacts = *cfg.Artifacts
	}

	if cfg.Environment != nil {
		p.Environment = *cfg.Environment
	}

	b.projects[cfg.Name] = p
	b.projectARNIndex[p.Arn] = cfg.Name

	out := *p

	return &out, nil
}

// BatchGetProjects returns projects by name or ARN. Missing names are returned separately.
func (b *InMemoryBackend) BatchGetProjects(names []string) ([]*Project, []string) {
	b.mu.RLock("BatchGetProjects")
	defer b.mu.RUnlock()

	found := make([]*Project, 0, len(names))
	notFound := make([]string, 0, len(names))

	for _, name := range names {
		if p, ok := b.lookupByNameOrARN(name); ok {
			out := *p
			found = append(found, &out)
		} else {
			notFound = append(notFound, name)
		}
	}

	return found, notFound
}

// UpdateProject updates fields on an existing project.
// applyProjectOptionalFields copies non-zero optional fields from cfg into p.
func applyProjectOptionalFields(p *Project, cfg ProjectConfig) {
	if cfg.Source != nil {
		p.Source = *cfg.Source
	}

	if cfg.Artifacts != nil {
		p.Artifacts = *cfg.Artifacts
	}

	if cfg.Environment != nil {
		p.Environment = *cfg.Environment
	}

	if cfg.SecondarySources != nil {
		p.SecondarySources = cfg.SecondarySources
	}

	if cfg.SecondaryArtifacts != nil {
		p.SecondaryArtifacts = cfg.SecondaryArtifacts
	}

	if cfg.SecondarySourceVersions != nil {
		p.SecondarySourceVersions = cfg.SecondarySourceVersions
	}

	if cfg.FileSystemLocations != nil {
		p.FileSystemLocations = cfg.FileSystemLocations
	}

	if cfg.Cache != nil {
		p.Cache = cfg.Cache
	}

	if cfg.LogsConfig != nil {
		p.LogsConfig = cfg.LogsConfig
	}

	if cfg.VpcConfig != nil {
		p.VpcConfig = cfg.VpcConfig
	}

	if cfg.BuildBatchConfig != nil {
		p.BuildBatchConfig = cfg.BuildBatchConfig
	}
}

// UpdateProject updates fields on an existing project.
func (b *InMemoryBackend) UpdateProject(name string, cfg ProjectConfig) (*Project, error) {
	b.mu.Lock("UpdateProject")
	defer b.mu.Unlock()

	p, ok := b.lookupByNameOrARN(name)
	if !ok {
		return nil, ErrNotFound
	}

	if cfg.Description != "" {
		p.Description = cfg.Description
	}

	if cfg.ServiceRole != "" {
		p.ServiceRole = cfg.ServiceRole
	}

	if cfg.EncryptionKey != "" {
		p.EncryptionKey = cfg.EncryptionKey
	}

	if cfg.ResourceAccessRole != "" {
		p.ResourceAccessRole = cfg.ResourceAccessRole
	}

	if cfg.TimeoutInMinutes != 0 {
		p.TimeoutInMinutes = cfg.TimeoutInMinutes
	}

	if cfg.QueuedTimeoutInMinutes != 0 {
		p.QueuedTimeoutInMinutes = cfg.QueuedTimeoutInMinutes
	}

	if cfg.ConcurrentBuildLimit != 0 {
		p.ConcurrentBuildLimit = cfg.ConcurrentBuildLimit
	}

	if cfg.AutoRetryLimit != 0 {
		p.AutoRetryLimit = cfg.AutoRetryLimit
	}

	applyProjectOptionalFields(p, cfg)

	if len(cfg.Tags) > 0 {
		p.Tags = mergeTags(p.Tags, cfg.Tags)
	}

	p.LastModified = float64(time.Now().Unix())

	out := *p

	return &out, nil
}

// mergeTags returns a new map containing dst's entries merged with src.
func mergeTags(dst, src map[string]string) map[string]string {
	if dst == nil {
		dst = make(map[string]string, len(src))
	}

	maps.Copy(dst, src)

	return dst
}

// DeleteProject removes a project by name and all builds associated with it.
func (b *InMemoryBackend) DeleteProject(name string) error {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()

	p, ok := b.projects[name]
	if !ok {
		return ErrNotFound
	}

	delete(b.projectARNIndex, p.Arn)
	delete(b.projects, name)

	// Use the per-project build index for O(k) cleanup instead of O(n) scan.
	for id := range b.buildsByProject[name] {
		if build, ok2 := b.builds[id]; ok2 {
			delete(b.buildARNIndex, build.Arn)
			delete(b.builds, id)
		}
	}
	delete(b.buildsByProject, name)

	return nil
}

// ListProjects returns all project names in sorted order.
func (b *InMemoryBackend) ListProjects() []string {
	b.mu.RLock("ListProjects")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.projects))
	for name := range b.projects {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// StartBuild creates a new build for the given project, copying environment/source/artifacts from the project.
// envOverrides, if non-empty, replaces project-level env vars by name and appends any new ones — matching real AWS StartBuild semantics.
func (b *InMemoryBackend) StartBuild(projectName string, envOverrides []EnvironmentVariable) (*Build, error) {
	b.mu.Lock("StartBuild")
	defer b.mu.Unlock()

	proj, ok := b.projects[projectName]
	if !ok {
		return nil, ErrNotFound
	}

	buildID := randomID()
	fullID := projectName + ":" + buildID
	now := float64(time.Now().Unix())

	env := proj.Environment
	src := proj.Source
	artifacts := proj.Artifacts

	if len(envOverrides) > 0 {
		merged := make([]EnvironmentVariable, len(env.EnvironmentVariables))
		copy(merged, env.EnvironmentVariables)
		for _, ov := range envOverrides {
			replaced := false
			for i, ev := range merged {
				if ev.Name == ov.Name {
					merged[i] = ov
					replaced = true
					break
				}
			}
			if !replaced {
				merged = append(merged, ov)
			}
		}
		env.EnvironmentVariables = merged
	}

	build := &Build{
		ID:                     fullID,
		Arn:                    b.buildBuildARN(projectName, buildID),
		ProjectName:            projectName,
		BuildStatus:            buildStatusInProgress,
		StartTime:              now,
		CurrentPhase:           phaseSubmitted,
		ServiceRole:            proj.ServiceRole,
		EncryptionKey:          proj.EncryptionKey,
		TimeoutInMinutes:       proj.TimeoutInMinutes,
		QueuedTimeoutInMinutes: proj.QueuedTimeoutInMinutes,
		Environment:            &env,
		Source:                 &src,
		Artifacts:              &artifacts,
		Phases: []BuildPhase{
			{PhaseType: phaseSubmitted, PhaseStatus: "SUCCEEDED", StartTime: now, EndTime: now, DurationInSeconds: 0},
		},
	}
	b.builds[fullID] = build
	b.buildARNIndex[build.Arn] = fullID
	if b.buildsByProject[projectName] == nil {
		b.buildsByProject[projectName] = make(map[string]struct{})
	}
	b.buildsByProject[projectName][fullID] = struct{}{}

	out := *build

	return &out, nil
}

// BatchGetBuilds returns builds by ID. Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetBuilds(ids []string) ([]*Build, []string) {
	b.mu.RLock("BatchGetBuilds")
	defer b.mu.RUnlock()

	found := make([]*Build, 0, len(ids))
	notFound := make([]string, 0, len(ids))

	for _, id := range ids {
		if build, ok := b.builds[id]; ok {
			out := *build
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// StopBuild marks a build as STOPPED.
func (b *InMemoryBackend) StopBuild(id string) (*Build, error) {
	b.mu.Lock("StopBuild")
	defer b.mu.Unlock()

	build, ok := b.builds[id]
	if !ok {
		return nil, ErrNotFound
	}

	build.BuildStatus = buildStatusStopped
	build.EndTime = float64(time.Now().Unix())
	build.CurrentPhase = "COMPLETED"
	build.BuildComplete = true

	out := *build

	return &out, nil
}

// ListBuilds returns all build IDs in the backend in sorted order.
func (b *InMemoryBackend) ListBuilds() []string {
	b.mu.RLock("ListBuilds")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.builds))
	for id := range b.builds {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids
}

// BatchDeleteBuilds deletes builds by ID and returns the IDs that were deleted.
func (b *InMemoryBackend) BatchDeleteBuilds(ids []string) []string {
	b.mu.Lock("BatchDeleteBuilds")
	defer b.mu.Unlock()

	deleted := make([]string, 0, len(ids))

	for _, id := range ids {
		build, ok := b.builds[id]
		if !ok {
			continue
		}

		projectName := build.ProjectName
		delete(b.buildARNIndex, build.Arn)
		delete(b.builds, id)

		if projectBuilds, ok2 := b.buildsByProject[projectName]; ok2 {
			delete(projectBuilds, id)
		}

		deleted = append(deleted, id)
	}

	return deleted
}

// RetryBuild creates a new build for the same project as an existing build.
func (b *InMemoryBackend) RetryBuild(id string) (*Build, error) {
	b.mu.Lock("RetryBuild")
	defer b.mu.Unlock()

	existing, ok := b.builds[id]
	if !ok {
		return nil, ErrNotFound
	}

	projectName := existing.ProjectName
	if _, ok2 := b.projects[projectName]; !ok2 {
		return nil, fmt.Errorf("%w: project %s not found", ErrNotFound, projectName)
	}

	buildID := randomID()
	fullID := projectName + ":" + buildID
	build := &Build{
		ID:           fullID,
		Arn:          b.buildBuildARN(projectName, buildID),
		ProjectName:  projectName,
		BuildStatus:  buildStatusInProgress,
		StartTime:    float64(time.Now().Unix()),
		CurrentPhase: phaseSubmitted,
	}
	b.builds[fullID] = build
	b.buildARNIndex[build.Arn] = fullID

	if b.buildsByProject[projectName] == nil {
		b.buildsByProject[projectName] = make(map[string]struct{})
	}

	b.buildsByProject[projectName][fullID] = struct{}{}

	out := *build

	return &out, nil
}

// ListBuildsForProject returns all build IDs for a given project in sorted order.
func (b *InMemoryBackend) ListBuildsForProject(projectName string) ([]string, error) {
	b.mu.RLock("ListBuildsForProject")
	defer b.mu.RUnlock()

	if _, ok := b.projects[projectName]; !ok {
		return nil, ErrNotFound
	}

	buildSet := b.buildsByProject[projectName]
	ids := make([]string, 0, len(buildSet))

	for id := range buildSet {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids, nil
}

// ListTagsForResource returns the tags for a CodeBuild resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if name, ok := b.projectARNIndex[resourceARN]; ok {
		p := b.projects[name]
		out := make(map[string]string, len(p.Tags))
		maps.Copy(out, p.Tags)

		return out, nil
	}

	if id, ok := b.buildARNIndex[resourceARN]; ok {
		build := b.builds[id]
		out := make(map[string]string, len(build.Tags))
		maps.Copy(out, build.Tags)

		return out, nil
	}

	if name, ok := b.fleetARNIndex[resourceARN]; ok {
		f := b.fleets[name]
		out := make(map[string]string, len(f.Tags))
		maps.Copy(out, f.Tags)

		return out, nil
	}

	if name, ok := b.reportGroupARNIndex[resourceARN]; ok {
		rg := b.reportGroups[name]
		out := make(map[string]string, len(rg.Tags))
		maps.Copy(out, rg.Tags)

		return out, nil
	}

	return nil, ErrNotFound
}

// TagResource adds or updates tags on a CodeBuild resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	if name, ok := b.projectARNIndex[resourceARN]; ok {
		p := b.projects[name]
		if p.Tags == nil {
			p.Tags = make(map[string]string)
		}

		maps.Copy(p.Tags, tagsCopy)

		return nil
	}

	if id, ok := b.buildARNIndex[resourceARN]; ok {
		build := b.builds[id]
		if build.Tags == nil {
			build.Tags = make(map[string]string)
		}

		maps.Copy(build.Tags, tagsCopy)

		return nil
	}

	if name, ok := b.fleetARNIndex[resourceARN]; ok {
		f := b.fleets[name]
		if f.Tags == nil {
			f.Tags = make(map[string]string)
		}

		maps.Copy(f.Tags, tagsCopy)

		return nil
	}

	if name, ok := b.reportGroupARNIndex[resourceARN]; ok {
		rg := b.reportGroups[name]
		if rg.Tags == nil {
			rg.Tags = make(map[string]string)
		}

		maps.Copy(rg.Tags, tagsCopy)

		return nil
	}

	return ErrNotFound
}

// UntagResource removes tags from a CodeBuild resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if name, ok := b.projectARNIndex[resourceARN]; ok {
		p := b.projects[name]
		for _, k := range tagKeys {
			delete(p.Tags, k)
		}

		return nil
	}

	if id, ok := b.buildARNIndex[resourceARN]; ok {
		build := b.builds[id]
		for _, k := range tagKeys {
			delete(build.Tags, k)
		}

		return nil
	}

	if name, ok := b.fleetARNIndex[resourceARN]; ok {
		f := b.fleets[name]
		for _, k := range tagKeys {
			delete(f.Tags, k)
		}

		return nil
	}

	if name, ok := b.reportGroupARNIndex[resourceARN]; ok {
		rg := b.reportGroups[name]
		for _, k := range tagKeys {
			delete(rg.Tags, k)
		}

		return nil
	}

	return ErrNotFound
}

// --- Fleet operations ---

func (b *InMemoryBackend) buildFleetARN(name string) string {
	return arn.Build("codebuild", b.region, b.accountID, "fleet/"+name)
}

func (b *InMemoryBackend) buildReportGroupARN(name string) string {
	return arn.Build("codebuild", b.region, b.accountID, "report-group/"+name)
}

func (b *InMemoryBackend) buildWebhookURL(projectName string) string {
	return "https://codebuild." + b.region + ".amazonaws.com/webhooks/" + projectName
}

// CreateFleet creates a new compute fleet.
func (b *InMemoryBackend) CreateFleet(
	name string, baseCapacity int32, computeType, environmentType string, tags map[string]string,
) (*Fleet, error) {
	b.mu.Lock("CreateFleet")
	defer b.mu.Unlock()

	if _, exists := b.fleets[name]; exists {
		return nil, ErrAlreadyExists
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	f := &Fleet{
		Arn:             b.buildFleetARN(name),
		Name:            name,
		BaseCapacity:    baseCapacity,
		ComputeType:     computeType,
		EnvironmentType: environmentType,
		Status:          &FleetStatus{StatusCode: "ACTIVE"},
		Tags:            tagsCopy,
		Created:         now,
		LastModified:    now,
	}
	b.fleets[name] = f
	b.fleetARNIndex[f.Arn] = name

	out := *f

	return &out, nil
}

// BatchGetFleets returns fleets by name or ARN. Missing names are returned separately.
func (b *InMemoryBackend) BatchGetFleets(names []string) ([]*Fleet, []string) {
	b.mu.RLock("BatchGetFleets")
	defer b.mu.RUnlock()

	found := make([]*Fleet, 0, len(names))
	notFound := make([]string, 0, len(names))

	for _, nameOrARN := range names {
		name := nameOrARN
		if n, ok := b.fleetARNIndex[nameOrARN]; ok {
			name = n
		}

		if f, ok := b.fleets[name]; ok {
			out := *f
			found = append(found, &out)
		} else {
			notFound = append(notFound, nameOrARN)
		}
	}

	return found, notFound
}

// --- ReportGroup operations ---

// CreateReportGroup creates a new report group.
func (b *InMemoryBackend) CreateReportGroup(
	name, rtype string, exportConfig ReportExportConfig, tags map[string]string,
) (*ReportGroup, error) {
	b.mu.Lock("CreateReportGroup")
	defer b.mu.Unlock()

	if _, exists := b.reportGroups[name]; exists {
		return nil, ErrAlreadyExists
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	rg := &ReportGroup{
		Arn:          b.buildReportGroupARN(name),
		Name:         name,
		Type:         rtype,
		Status:       "ACTIVE",
		ExportConfig: exportConfig,
		Tags:         tagsCopy,
		Created:      now,
		LastModified: now,
	}
	b.reportGroups[name] = rg
	b.reportGroupARNIndex[rg.Arn] = name

	out := *rg

	return &out, nil
}

// BatchGetReportGroups returns report groups by ARN. Missing ARNs are returned separately.
func (b *InMemoryBackend) BatchGetReportGroups(arns []string) ([]*ReportGroup, []string) {
	b.mu.RLock("BatchGetReportGroups")
	defer b.mu.RUnlock()

	found := make([]*ReportGroup, 0, len(arns))
	notFound := make([]string, 0, len(arns))

	for _, a := range arns {
		name, ok := b.reportGroupARNIndex[a]
		if !ok {
			// also try by name for convenience
			if _, foundByName := b.reportGroups[a]; foundByName {
				name = a
				ok = true
			}
		}

		if ok {
			rg := b.reportGroups[name]
			out := *rg
			found = append(found, &out)
		} else {
			notFound = append(notFound, a)
		}
	}

	return found, notFound
}

// --- Report operations ---

// AddReportInternal seeds a Report directly into the backend (test helper).
func (b *InMemoryBackend) AddReportInternal(r *Report) {
	b.mu.Lock("AddReportInternal")
	defer b.mu.Unlock()

	b.reports[r.Arn] = r
	if r.ReportGroupArn != "" {
		if b.reportsByGroup[r.ReportGroupArn] == nil {
			b.reportsByGroup[r.ReportGroupArn] = make(map[string]struct{})
		}
		b.reportsByGroup[r.ReportGroupArn][r.Arn] = struct{}{}
	}
}

// BatchGetReports returns reports by ARN. Missing ARNs are returned separately.
func (b *InMemoryBackend) BatchGetReports(arns []string) ([]*Report, []string) {
	b.mu.RLock("BatchGetReports")
	defer b.mu.RUnlock()

	found := make([]*Report, 0, len(arns))
	notFound := make([]string, 0, len(arns))

	for _, a := range arns {
		if r, ok := b.reports[a]; ok {
			out := *r
			found = append(found, &out)
		} else {
			notFound = append(notFound, a)
		}
	}

	return found, notFound
}

// --- BuildBatch operations ---

// AddBuildBatchInternal seeds a BuildBatch directly into the backend (test helper).
func (b *InMemoryBackend) AddBuildBatchInternal(bb *BuildBatch) {
	b.mu.Lock("AddBuildBatchInternal")
	defer b.mu.Unlock()

	b.buildBatches[bb.ID] = bb
}

// BatchGetBuildBatches returns build batches by ID. Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetBuildBatches(ids []string) ([]*BuildBatch, []string) {
	b.mu.RLock("BatchGetBuildBatches")
	defer b.mu.RUnlock()

	found := make([]*BuildBatch, 0, len(ids))
	notFound := make([]string, 0, len(ids))

	for _, id := range ids {
		if bb, ok := b.buildBatches[id]; ok {
			out := *bb
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// --- CommandExecution operations ---

// AddCommandExecutionInternal seeds a CommandExecution directly into the backend (test helper).
func (b *InMemoryBackend) AddCommandExecutionInternal(ce *CommandExecution) {
	b.mu.Lock("AddCommandExecutionInternal")
	defer b.mu.Unlock()

	b.commandExecutions[ce.ID] = ce
}

// BatchGetCommandExecutions returns command executions by ID within a sandbox.
// Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetCommandExecutions(sandboxID string, ids []string) ([]*CommandExecution, []string) {
	b.mu.RLock("BatchGetCommandExecutions")
	defer b.mu.RUnlock()

	found := make([]*CommandExecution, 0, len(ids))
	notFound := make([]string, 0, len(ids))

	for _, id := range ids {
		ce, ok := b.commandExecutions[id]
		if ok && ce.SandboxID == sandboxID {
			out := *ce
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// --- Sandbox operations ---

// AddSandboxInternal seeds a Sandbox directly into the backend (test helper).
func (b *InMemoryBackend) AddSandboxInternal(s *Sandbox) {
	b.mu.Lock("AddSandboxInternal")
	defer b.mu.Unlock()

	b.sandboxes[s.ID] = s
}

// BatchGetSandboxes returns sandboxes by ID or ARN. Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetSandboxes(ids []string) ([]*Sandbox, []string) {
	b.mu.RLock("BatchGetSandboxes")
	defer b.mu.RUnlock()

	found := make([]*Sandbox, 0, len(ids))
	notFound := make([]string, 0, len(ids))

	for _, id := range ids {
		if s, ok := b.sandboxes[id]; ok {
			out := *s
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// ListBuildBatches returns all build batch IDs in sorted order.
func (b *InMemoryBackend) ListBuildBatches() []string {
	b.mu.RLock("ListBuildBatches")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.buildBatches))
	for id := range b.buildBatches {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids
}

// ListFleets returns all fleet ARNs in sorted order.
func (b *InMemoryBackend) ListFleets() []string {
	b.mu.RLock("ListFleets")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(b.fleets))
	for _, f := range b.fleets {
		arns = append(arns, f.Arn)
	}

	sort.Strings(arns)

	return arns
}

// ListReportGroups returns all report group ARNs in sorted order.
func (b *InMemoryBackend) ListReportGroups() []string {
	b.mu.RLock("ListReportGroups")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(b.reportGroups))
	for _, rg := range b.reportGroups {
		arns = append(arns, rg.Arn)
	}

	sort.Strings(arns)

	return arns
}

// ListSandboxes returns all sandbox IDs in sorted order.
func (b *InMemoryBackend) ListSandboxes() []string {
	b.mu.RLock("ListSandboxes")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.sandboxes))
	for id := range b.sandboxes {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids
}

// StartBuildBatch creates a new build batch for a project.
func (b *InMemoryBackend) StartBuildBatch(projectName string) (*BuildBatch, error) {
	b.mu.Lock("StartBuildBatch")
	defer b.mu.Unlock()

	if _, ok := b.projects[projectName]; !ok {
		return nil, ErrNotFound
	}

	id := projectName + ":" + uuid.NewString()
	bb := &BuildBatch{
		ID:               id,
		ProjectName:      projectName,
		BuildBatchStatus: buildStatusInProgress,
		StartTime:        float64(time.Now().Unix()),
	}
	b.buildBatches[id] = bb

	if b.batchesByProject[projectName] == nil {
		b.batchesByProject[projectName] = make(map[string]struct{})
	}
	b.batchesByProject[projectName][id] = struct{}{}

	out := *bb

	return &out, nil
}

// StartCommandExecution creates a new command execution in a sandbox.
func (b *InMemoryBackend) StartCommandExecution(sandboxID, command, execType string) (*CommandExecution, error) {
	b.mu.Lock("StartCommandExecution")
	defer b.mu.Unlock()

	if _, ok := b.sandboxes[sandboxID]; !ok {
		return nil, ErrNotFound
	}

	id := uuid.NewString()
	now := float64(time.Now().Unix())
	ce := &CommandExecution{
		ID:        id,
		SandboxID: sandboxID,
		Command:   command,
		Type:      execType,
		Status:    buildStatusSucceeded,
		StartTime: now,
		EndTime:   now,
	}
	b.commandExecutions[id] = ce

	if b.commandsBySandbox[sandboxID] == nil {
		b.commandsBySandbox[sandboxID] = make(map[string]struct{})
	}
	b.commandsBySandbox[sandboxID][id] = struct{}{}

	out := *ce

	return &out, nil
}

// StartSandbox creates a new sandbox for a project.
func (b *InMemoryBackend) StartSandbox(projectName string) (*Sandbox, error) {
	b.mu.Lock("StartSandbox")
	defer b.mu.Unlock()

	if _, ok := b.projects[projectName]; !ok {
		return nil, ErrNotFound
	}

	id := uuid.NewString()
	sandboxArn := arn.Build("codebuild", b.region, b.accountID, "sandbox/"+id)
	sb := &Sandbox{
		ID:          id,
		Arn:         sandboxArn,
		ProjectName: projectName,
		Status:      "READY",
		StartTime:   float64(time.Now().Unix()),
	}
	b.sandboxes[id] = sb

	if b.sandboxesByProject[projectName] == nil {
		b.sandboxesByProject[projectName] = make(map[string]struct{})
	}
	b.sandboxesByProject[projectName][id] = struct{}{}

	out := *sb

	return &out, nil
}

// --- Webhook operations ---

// --- Source Credentials operations ---

// ImportSourceCredentials imports source credentials and returns the ARN.
func (b *InMemoryBackend) ImportSourceCredentials(authType, serverType, token string) (string, error) {
	b.mu.Lock("ImportSourceCredentials")
	defer b.mu.Unlock()

	_ = token
	arnStr := "arn:aws:codebuild:" + b.region + ":" + b.accountID + ":token/" + serverType
	b.sourceCredentials[arnStr] = &SourceCredentials{
		Arn:        arnStr,
		ServerType: serverType,
		AuthType:   authType,
	}

	return arnStr, nil
}

// DeleteSourceCredentials removes source credentials by ARN.
func (b *InMemoryBackend) DeleteSourceCredentials(arnStr string) error {
	b.mu.Lock("DeleteSourceCredentials")
	defer b.mu.Unlock()

	if _, ok := b.sourceCredentials[arnStr]; !ok {
		return ErrNotFound
	}

	delete(b.sourceCredentials, arnStr)

	return nil
}

// ListSourceCredentials returns all stored source credentials.
func (b *InMemoryBackend) ListSourceCredentials() []*SourceCredentials {
	b.mu.RLock("ListSourceCredentials")
	defer b.mu.RUnlock()

	result := make([]*SourceCredentials, 0, len(b.sourceCredentials))
	for _, sc := range b.sourceCredentials {
		out := *sc
		result = append(result, &out)
	}

	return result
}

// --- Resource Policy operations ---

// PutResourcePolicy stores a resource policy for the given ARN.
func (b *InMemoryBackend) PutResourcePolicy(resourceArn, policy string) error {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	b.resourcePolicies[resourceArn] = policy

	return nil
}

// GetResourcePolicy returns the resource policy for the given ARN, or ErrNotFound if none set.
func (b *InMemoryBackend) GetResourcePolicy(resourceArn string) (string, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	if p, ok := b.resourcePolicies[resourceArn]; ok {
		return p, nil
	}

	return "", ErrNotFound
}

// DeleteResourcePolicy removes the resource policy for the given ARN (idempotent).
func (b *InMemoryBackend) DeleteResourcePolicy(resourceArn string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	delete(b.resourcePolicies, resourceArn)

	return nil
}

// --- Extended Report operations ---

// DeleteReport removes a report by ARN.
func (b *InMemoryBackend) DeleteReport(arnStr string) error {
	b.mu.Lock("DeleteReport")
	defer b.mu.Unlock()

	r, ok := b.reports[arnStr]
	if !ok {
		return ErrNotFound
	}

	if r.ReportGroupArn != "" {
		if set, ok2 := b.reportsByGroup[r.ReportGroupArn]; ok2 {
			delete(set, arnStr)
		}
	}

	delete(b.reports, arnStr)

	return nil
}

// ListReports returns all report ARNs in sorted order.
func (b *InMemoryBackend) ListReports() []string {
	b.mu.RLock("ListReports")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(b.reports))
	for a := range b.reports {
		arns = append(arns, a)
	}

	sort.Strings(arns)

	return arns
}

// ListReportsForReportGroup returns all report ARNs for the given report group ARN.
func (b *InMemoryBackend) ListReportsForReportGroup(reportGroupArn string) []string {
	b.mu.RLock("ListReportsForReportGroup")
	defer b.mu.RUnlock()

	set := b.reportsByGroup[reportGroupArn]
	arns := make([]string, 0, len(set))
	for a := range set {
		arns = append(arns, a)
	}

	sort.Strings(arns)

	return arns
}

// --- Extended ReportGroup operations ---

// DeleteReportGroup removes a report group by ARN.
func (b *InMemoryBackend) DeleteReportGroup(arnStr string) error {
	b.mu.Lock("DeleteReportGroup")
	defer b.mu.Unlock()

	name, ok := b.reportGroupARNIndex[arnStr]
	if !ok {
		return ErrNotFound
	}

	delete(b.reportGroups, name)
	delete(b.reportGroupARNIndex, arnStr)

	return nil
}

// UpdateReportGroup updates the export config of a report group.
func (b *InMemoryBackend) UpdateReportGroup(arnStr string, exportConfig *ReportExportConfig) (*ReportGroup, error) {
	b.mu.Lock("UpdateReportGroup")
	defer b.mu.Unlock()

	name, ok := b.reportGroupARNIndex[arnStr]
	if !ok {
		return nil, ErrNotFound
	}

	rg := b.reportGroups[name]
	if exportConfig != nil {
		rg.ExportConfig = *exportConfig
	}

	rg.LastModified = float64(time.Now().Unix())
	out := *rg

	return &out, nil
}

// --- Extended Webhook operations ---

// DeleteWebhook removes the webhook for a project.
func (b *InMemoryBackend) DeleteWebhook(projectName string) error {
	b.mu.Lock("DeleteWebhook")
	defer b.mu.Unlock()

	if _, ok := b.webhooks[projectName]; !ok {
		return ErrNotFound
	}

	delete(b.webhooks, projectName)

	return nil
}

// UpdateWebhook updates the branchFilter and buildType of an existing webhook.
func (b *InMemoryBackend) UpdateWebhook(projectName, branchFilter, buildType string) (*Webhook, error) {
	b.mu.Lock("UpdateWebhook")
	defer b.mu.Unlock()

	w, ok := b.webhooks[projectName]
	if !ok {
		return nil, ErrNotFound
	}

	w.BranchFilter = branchFilter
	w.BuildType = buildType
	out := *w

	return &out, nil
}

// --- Extended Fleet operations ---

// DeleteFleet removes a fleet by ARN.
func (b *InMemoryBackend) DeleteFleet(arnStr string) error {
	b.mu.Lock("DeleteFleet")
	defer b.mu.Unlock()

	name, ok := b.fleetARNIndex[arnStr]
	if !ok {
		// also try by name for convenience
		if _, okName := b.fleets[arnStr]; okName {
			name = arnStr
			ok = true
		}
	}

	if !ok {
		return ErrNotFound
	}

	f := b.fleets[name]
	delete(b.fleetARNIndex, f.Arn)
	delete(b.fleets, name)

	return nil
}

// DeleteBuildBatch removes a build batch by ID.
func (b *InMemoryBackend) DeleteBuildBatch(id string) error {
	b.mu.Lock("DeleteBuildBatch")
	defer b.mu.Unlock()

	bb, ok := b.buildBatches[id]
	if !ok {
		return ErrNotFound
	}

	if set, ok2 := b.batchesByProject[bb.ProjectName]; ok2 {
		delete(set, id)
	}

	delete(b.buildBatches, id)

	return nil
}

// UpdateFleet updates the base capacity of a fleet.
func (b *InMemoryBackend) UpdateFleet(arnStr string, baseCapacity int32) (*Fleet, error) {
	b.mu.Lock("UpdateFleet")
	defer b.mu.Unlock()

	name, ok := b.fleetARNIndex[arnStr]
	if !ok {
		return nil, ErrNotFound
	}

	f := b.fleets[name]
	f.BaseCapacity = baseCapacity
	f.LastModified = float64(time.Now().Unix())
	out := *f

	return &out, nil
}

// --- Extended BuildBatch operations ---

// RetryBuildBatch creates a new build batch with the same project as an existing one.
func (b *InMemoryBackend) RetryBuildBatch(id string) (*BuildBatch, error) {
	b.mu.Lock("RetryBuildBatch")
	defer b.mu.Unlock()

	existing, ok := b.buildBatches[id]
	if !ok {
		return nil, ErrNotFound
	}

	projectName := existing.ProjectName
	newID := projectName + ":" + uuid.NewString()
	bb := &BuildBatch{
		ID:               newID,
		ProjectName:      projectName,
		BuildBatchStatus: buildStatusInProgress,
		StartTime:        float64(time.Now().Unix()),
	}
	b.buildBatches[newID] = bb

	if b.batchesByProject[projectName] == nil {
		b.batchesByProject[projectName] = make(map[string]struct{})
	}
	b.batchesByProject[projectName][newID] = struct{}{}

	out := *bb

	return &out, nil
}

// StopBuildBatch marks a build batch as STOPPED.
func (b *InMemoryBackend) StopBuildBatch(id string) (*BuildBatch, error) {
	b.mu.Lock("StopBuildBatch")
	defer b.mu.Unlock()

	bb, ok := b.buildBatches[id]
	if !ok {
		return nil, ErrNotFound
	}

	bb.BuildBatchStatus = buildStatusStopped
	bb.EndTime = float64(time.Now().Unix())
	out := *bb

	return &out, nil
}

// ListBuildBatchesForProject returns all batch IDs for a project in sorted order.
func (b *InMemoryBackend) ListBuildBatchesForProject(projectName string) ([]string, error) {
	b.mu.RLock("ListBuildBatchesForProject")
	defer b.mu.RUnlock()

	if _, ok := b.projects[projectName]; !ok {
		return nil, ErrNotFound
	}

	set := b.batchesByProject[projectName]
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids, nil
}

// --- Extended Sandbox operations ---

// StopSandbox marks a sandbox as STOPPED.
func (b *InMemoryBackend) StopSandbox(id string) (*Sandbox, error) {
	b.mu.Lock("StopSandbox")
	defer b.mu.Unlock()

	sb, ok := b.sandboxes[id]
	if !ok {
		return nil, ErrNotFound
	}

	sb.Status = buildStatusStopped
	sb.EndTime = float64(time.Now().Unix())
	out := *sb

	return &out, nil
}

// ListSandboxesForProject returns all sandbox IDs for a project in sorted order.
func (b *InMemoryBackend) ListSandboxesForProject(projectName string) ([]string, error) {
	b.mu.RLock("ListSandboxesForProject")
	defer b.mu.RUnlock()

	if _, ok := b.projects[projectName]; !ok {
		return nil, ErrNotFound
	}

	set := b.sandboxesByProject[projectName]
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids, nil
}

// --- Extended CommandExecution operations ---

// ListCommandExecutionsForSandbox returns all command execution IDs for a sandbox in sorted order.
func (b *InMemoryBackend) ListCommandExecutionsForSandbox(sandboxID string) ([]string, error) {
	b.mu.RLock("ListCommandExecutionsForSandbox")
	defer b.mu.RUnlock()

	if _, ok := b.sandboxes[sandboxID]; !ok {
		return nil, ErrNotFound
	}

	set := b.commandsBySandbox[sandboxID]
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids, nil
}

// --- Extended Project operations ---

// UpdateProjectVisibility sets the visibility of a project by ARN.
func (b *InMemoryBackend) UpdateProjectVisibility(projectArn, visibility string) error {
	b.mu.Lock("UpdateProjectVisibility")
	defer b.mu.Unlock()

	name, ok := b.projectARNIndex[projectArn]
	if !ok {
		return ErrNotFound
	}

	b.projects[name].Visibility = visibility

	return nil
}

// InvalidateProjectCache is a no-op cache invalidation (returns ErrNotFound if project missing).
func (b *InMemoryBackend) InvalidateProjectCache(projectName string) error {
	b.mu.RLock("InvalidateProjectCache")
	defer b.mu.RUnlock()

	if _, ok := b.projects[projectName]; !ok {
		return ErrNotFound
	}

	return nil
}

// --- Misc read-only operations ---

// DescribeCodeCoverages returns an empty list (no state needed).
func (b *InMemoryBackend) DescribeCodeCoverages(_ string) ([]CodeCoverage, error) {
	return []CodeCoverage{}, nil
}

// DescribeTestCases returns an empty list (no state needed).
func (b *InMemoryBackend) DescribeTestCases(_ string) ([]TestCase, error) {
	return []TestCase{}, nil
}

// GetReportGroupTrend returns an empty stats map (no state needed).
func (b *InMemoryBackend) GetReportGroupTrend(_ string) (map[string]any, error) {
	return map[string]any{}, nil
}

// ListSharedProjects returns an empty list (no shared projects in emulator).
func (b *InMemoryBackend) ListSharedProjects() []string {
	return []string{}
}

// ListSharedReportGroups returns an empty list (no shared report groups in emulator).
func (b *InMemoryBackend) ListSharedReportGroups() []string {
	return []string{}
}

// ListCuratedEnvironmentImages returns a minimal hardcoded list of curated images.
func (b *InMemoryBackend) ListCuratedEnvironmentImages() []map[string]any {
	return []map[string]any{
		{
			"platform": "UBUNTU",
			"languages": []map[string]any{
				{
					"language": "PYTHON",
					"images": []map[string]any{
						{"name": "aws/codebuild/standard:7.0"},
					},
				},
			},
		},
	}
}

// --- Webhook operations ---

// CreateWebhook creates a webhook for a CodeBuild project.
func (b *InMemoryBackend) CreateWebhook(projectName, branchFilter, buildType string) (*Webhook, error) {
	b.mu.Lock("CreateWebhook")
	defer b.mu.Unlock()

	if _, ok := b.projects[projectName]; !ok {
		return nil, ErrNotFound
	}

	if _, exists := b.webhooks[projectName]; exists {
		return nil, ErrAlreadyExists
	}

	w := &Webhook{
		ProjectName:  projectName,
		URL:          b.buildWebhookURL(projectName),
		BranchFilter: branchFilter,
		BuildType:    buildType,
	}
	b.webhooks[projectName] = w

	out := *w

	return &out, nil
}
