package codebuild

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
	Webhook                 *Webhook               `json:"webhook,omitempty"`
	ResourceAccessRole      string                 `json:"resourceAccessRole,omitempty"`
	Description             string                 `json:"description,omitempty"`
	ServiceRole             string                 `json:"serviceRole,omitempty"`
	EncryptionKey           string                 `json:"encryptionKey,omitempty"`
	Arn                     string                 `json:"arn"`
	Visibility              string                 `json:"projectVisibility,omitempty"`
	PublicProjectAlias      string                 `json:"publicProjectAlias,omitempty"`
	Name                    string                 `json:"name"`
	SourceVersion           string                 `json:"sourceVersion,omitempty"`
	Artifacts               ProjectArtifacts       `json:"artifacts"`
	Source                  ProjectSource          `json:"source"`
	SecondarySourceVersions []ProjectSourceVersion `json:"secondarySourceVersions,omitempty"`
	SecondaryArtifacts      []ProjectArtifacts     `json:"secondaryArtifacts,omitempty"`
	SecondarySources        []ProjectSource        `json:"secondarySources,omitempty"`
	FileSystemLocations     []FileSystemLocation   `json:"fileSystemLocations,omitempty"`
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
//
// ComputeConfiguration/ProxyConfiguration/VpcConfig are real Fleet fields
// (aws-sdk-go-v2/service/codebuild/types.Fleet) NOT modeled here -- a
// genuine, newly-found completeness gap (not previously flagged in
// PARITY.md), left out of this pass as a larger feature (nested
// subnet/security-group/attribute-based-compute config validation) than a
// wire-shape fix; see PARITY.md gaps.
type Fleet struct {
	Tags                 map[string]string     `json:"tags,omitempty"`
	Status               *FleetStatus          `json:"status,omitempty"`
	ScalingConfiguration *ScalingConfiguration `json:"scalingConfiguration,omitempty"`
	Arn                  string                `json:"arn"`
	ID                   string                `json:"id"`
	Name                 string                `json:"name"`
	FleetServiceRole     string                `json:"fleetServiceRole,omitempty"`
	OverflowBehavior     string                `json:"overflowBehavior,omitempty"` // QUEUE|ON_DEMAND
	ComputeType          string                `json:"computeType,omitempty"`
	EnvironmentType      string                `json:"environmentType,omitempty"`
	ImageID              string                `json:"imageId,omitempty"`
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

// WebhookFilter represents a single filter criterion in a webhook filter group.
type WebhookFilter struct {
	Type                  string `json:"type"`
	Pattern               string `json:"pattern"`
	ExcludeMatchedPattern bool   `json:"excludeMatchedPattern,omitempty"`
}

// PullRequestBuildPolicy defines comment-based approval requirements for
// triggering builds on pull requests.
type PullRequestBuildPolicy struct {
	RequiresCommentApproval string   `json:"requiresCommentApproval"`
	ApproverRoles           []string `json:"approverRoles,omitempty"`
}

// ScopeConfiguration is the scope configuration for a global or organization webhook.
type ScopeConfiguration struct {
	Name   string `json:"name"`
	Domain string `json:"domain,omitempty"`
	Scope  string `json:"scope"`
}

// Webhook represents an in-memory AWS CodeBuild webhook.
type Webhook struct {
	ManualCreation         *bool                   `json:"manualCreation,omitempty"`
	PullRequestBuildPolicy *PullRequestBuildPolicy `json:"pullRequestBuildPolicy,omitempty"`
	ScopeConfiguration     *ScopeConfiguration     `json:"scopeConfiguration,omitempty"`
	ProjectName            string                  `json:"projectName"`
	URL                    string                  `json:"url,omitempty"`
	BranchFilter           string                  `json:"branchFilter,omitempty"`
	BuildType              string                  `json:"buildType,omitempty"`
	PayloadURL             string                  `json:"payloadUrl,omitempty"`
	Secret                 string                  `json:"secret,omitempty"`
	Status                 string                  `json:"status,omitempty"`
	StatusMessage          string                  `json:"statusMessage,omitempty"`
	FilterGroups           [][]WebhookFilter       `json:"filterGroups,omitempty"`
	LastModifiedSecret     float64                 `json:"lastModifiedSecret,omitempty"`
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
