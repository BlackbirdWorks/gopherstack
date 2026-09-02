package amplify

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Platform represents the Amplify app platform type.
type Platform string

const (
	// PlatformWEB is a static web app.
	PlatformWEB Platform = "WEB"
	// PlatformWEBCOMPUTE is a web app with server-side rendering (SSR).
	PlatformWEBCOMPUTE Platform = "WEB_COMPUTE"
	// PlatformWEBDYNAMIC is a dynamic web app.
	PlatformWEBDYNAMIC Platform = "WEB_DYNAMIC"
)

// isValidPlatform reports whether p is a real Amplify Platform enum value.
// An empty string is accepted by callers as "unspecified" and defaulted
// separately; it is not itself a valid enum value.
func isValidPlatform(p string) bool {
	switch Platform(p) {
	case PlatformWEB, PlatformWEBCOMPUTE, PlatformWEBDYNAMIC:
		return true
	default:
		return false
	}
}

// Stage represents the branch deployment stage.
//
// Real Amplify's Stage enum (aws-sdk-go-v2/service/amplify/types.Stage) is
// PRODUCTION, BETA, DEVELOPMENT, EXPERIMENTAL, PULL_REQUEST. gopherstack
// previously defined a STAGING value that does not exist in the real API;
// it has been renamed to BETA, and PULL_REQUEST was added.
type Stage string

const (
	// StageProduction is the production stage.
	StageProduction Stage = "PRODUCTION"
	// StageBeta is the beta stage.
	StageBeta Stage = "BETA"
	// StageDevelopment is the development stage.
	StageDevelopment Stage = "DEVELOPMENT"
	// StageExperimental is the experimental stage.
	StageExperimental Stage = "EXPERIMENTAL"
	// StagePullRequest is the pull-request-preview stage.
	StagePullRequest Stage = "PULL_REQUEST"
)

// isValidStage reports whether s is a real Amplify Stage enum value. An
// empty string is accepted by callers as "unspecified".
func isValidStage(s string) bool {
	switch Stage(s) {
	case StageProduction, StageBeta, StageDevelopment, StageExperimental, StagePullRequest:
		return true
	default:
		return false
	}
}

// AutoBranchCreationConfig mirrors
// aws-sdk-go-v2/service/amplify/types.AutoBranchCreationConfig: the template
// applied to branches Amplify automatically creates for this app (when
// EnableAutoBranchCreation is set) from AutoBranchCreationPatterns.
type AutoBranchCreationConfig struct {
	EnvironmentVariables       map[string]string `json:"environmentVariables,omitempty"`
	BasicAuthCredentials       string            `json:"basicAuthCredentials,omitempty"`
	BuildSpec                  string            `json:"buildSpec,omitempty"`
	Framework                  string            `json:"framework,omitempty"`
	PullRequestEnvironmentName string            `json:"pullRequestEnvironmentName,omitempty"`
	Stage                      string            `json:"stage,omitempty"`
	EnableAutoBuild            bool              `json:"enableAutoBuild,omitempty"`
	EnableBasicAuth            bool              `json:"enableBasicAuth,omitempty"`
	EnablePerformanceMode      bool              `json:"enablePerformanceMode,omitempty"`
	EnablePullRequestPreview   bool              `json:"enablePullRequestPreview,omitempty"`
}

// CacheConfig mirrors aws-sdk-go-v2/service/amplify/types.CacheConfig.
type CacheConfig struct {
	Type string `json:"type"`
}

// CustomRule mirrors aws-sdk-go-v2/service/amplify/types.CustomRule.
type CustomRule struct {
	Source    string `json:"source"`
	Target    string `json:"target"`
	Condition string `json:"condition,omitempty"`
	Status    string `json:"status,omitempty"`
}

// ProductionBranch mirrors aws-sdk-go-v2/service/amplify/types.ProductionBranch.
// It is never stored on an App record -- it is computed at read time (see
// InMemoryBackend.productionBranchFor) from the app's PRODUCTION-stage
// branch and that branch's most recent job, so it can never go stale.
type ProductionBranch struct {
	BranchName     string    `json:"branchName,omitempty"`
	LastDeployTime time.Time `json:"lastDeployTime,omitzero"`
	Status         string    `json:"status,omitempty"`
	ThumbnailURL   string    `json:"thumbnailUrl,omitempty"`
}

// App represents an Amplify application.
//
// ProductionBranch and RepositoryCloneMethod are computed at read time
// (InMemoryBackend.productionBranchFor / repositoryCloneMethod) and set only
// on the copy returned to callers -- never written back to the stored table
// record, so they can never desync from the app's actual branches/jobs or
// Repository field.
type App struct {
	CreateTime                 time.Time                 `json:"createTime"`
	UpdateTime                 time.Time                 `json:"updateTime"`
	Tags                       *tags.Tags                `json:"tags,omitzero"`
	EnvironmentVariables       map[string]string         `json:"environmentVariables,omitempty"`
	AutoBranchCreationConfig   *AutoBranchCreationConfig `json:"autoBranchCreationConfig,omitempty"`
	CacheConfig                *CacheConfig              `json:"cacheConfig,omitempty"`
	ProductionBranch           *ProductionBranch         `json:"productionBranch,omitempty"`
	DefaultDomain              string                    `json:"defaultDomain,omitzero"`
	IAMServiceRoleArn          string                    `json:"iamServiceRoleArn,omitzero"`
	Name                       string                    `json:"name"`
	Description                string                    `json:"description,omitzero"`
	Repository                 string                    `json:"repository,omitzero"`
	AppID                      string                    `json:"appId"`
	BasicAuthCredentials       string                    `json:"basicAuthCredentials,omitzero"`
	BuildSpec                  string                    `json:"buildSpec,omitzero"`
	CustomHeaders              string                    `json:"customHeaders,omitzero"`
	ARN                        string                    `json:"appArn"`
	RepositoryCloneMethod      string                    `json:"repositoryCloneMethod,omitzero"`
	ComputeRoleARN             string                    `json:"computeRoleArn,omitzero"`
	JobConfigBuildComputeType  string                    `json:"jobConfigBuildComputeType,omitzero"`
	Platform                   Platform                  `json:"platform"`
	CustomRules                []CustomRule              `json:"customRules,omitempty"`
	AutoBranchCreationPatterns []string                  `json:"autoBranchCreationPatterns,omitempty"`
	EnableBranchAutoBuild      bool                      `json:"enableBranchAutoBuild"`
	EnableBasicAuth            bool                      `json:"enableBasicAuth"`
	EnableAutoBranchCreation   bool                      `json:"enableAutoBranchCreation,omitzero"`
	EnableBranchAutoDeletion   bool                      `json:"enableBranchAutoDeletion,omitzero"`
}

// AppOptions carries the optional App fields beyond the always-present
// name/description/repository/platform/tags quintet accepted directly by
// CreateApp/UpdateApp. It is passed as a trailing variadic argument
// (opts ...AppOptions) so every pre-existing positional call site --
// throughout this package's tests and test/e2e/amplify_test.go -- keeps
// compiling unchanged while CreateApp/UpdateApp gain full field parity with
// real Amplify's CreateAppInput/UpdateAppInput.
//
// Every field is a pointer/nil-able type so "not set" is distinguishable
// from the zero value:
//   - CreateApp: a nil pointer applies the real-Amplify create-time default
//     (EnableBranchAutoBuild defaults true; every other optional bool
//     defaults false; strings/slices/maps default empty).
//   - UpdateApp: a nil pointer means "leave the existing value unchanged",
//     matching real Amplify's partial-update semantics for UpdateApp.
type AppOptions struct {
	IAMServiceRoleArn          *string
	AutoBranchCreationConfig   *AutoBranchCreationConfig
	CacheConfig                *CacheConfig
	BasicAuthCredentials       *string
	BuildSpec                  *string
	CustomHeaders              *string
	ComputeRoleARN             *string
	JobConfigBuildComputeType  *string
	EnvironmentVariables       map[string]string
	EnableBranchAutoBuild      *bool
	EnableBasicAuth            *bool
	EnableAutoBranchCreation   *bool
	EnableBranchAutoDeletion   *bool
	AutoBranchCreationPatterns []string
	CustomRules                []CustomRule
}

// Branch represents an Amplify app branch.
//
// TotalNumberOfJobs and ActiveJobID are computed at read time
// (InMemoryBackend.branchView) from the branch's actual jobs and, like
// App.ProductionBranch, only ever set on a returned copy. EnableBasicAuth/
// EnableNotification/EnablePullRequestPreview are required response members
// in real Amplify (types.Branch), so unlike the optional bool fields below
// they must never be tagged omitempty/omitzero -- omitting a required *bool
// response member would deserialize client-side as a nil pointer instead of
// a false one.
type Branch struct {
	CreateTime                 time.Time         `json:"createTime"`
	UpdateTime                 time.Time         `json:"updateTime"`
	EnvironmentVariables       map[string]string `json:"environmentVariables,omitempty"`
	Tags                       *tags.Tags        `json:"tags,omitzero"`
	ActiveJobID                string            `json:"activeJobId,omitzero"`
	BackendEnvironmentARN      string            `json:"backendEnvironmentArn,omitzero"`
	Stage                      Stage             `json:"stage,omitzero"`
	AppID                      string            `json:"appId"`
	BranchARN                  string            `json:"branchArn"`
	BranchName                 string            `json:"branchName"`
	Description                string            `json:"description,omitzero"`
	DisplayName                string            `json:"displayName,omitzero"`
	SourceBranch               string            `json:"sourceBranch,omitzero"`
	TTL                        string            `json:"ttl,omitzero"`
	Framework                  string            `json:"framework,omitzero"`
	TotalNumberOfJobs          string            `json:"totalNumberOfJobs,omitzero"`
	BuildSpec                  string            `json:"buildSpec,omitzero"`
	BasicAuthCredentials       string            `json:"basicAuthCredentials,omitzero"`
	PullRequestEnvironmentName string            `json:"pullRequestEnvironmentName,omitzero"`
	BackendStackARN            string            `json:"backendStackArn,omitzero"`
	ComputeRoleARN             string            `json:"computeRoleArn,omitzero"`
	AssociatedResources        []string          `json:"associatedResources,omitempty"`
	CustomDomains              []string          `json:"customDomains,omitempty"`
	EnableAutoBuild            bool              `json:"enableAutoBuild"`
	EnableBasicAuth            bool              `json:"enableBasicAuth"`
	EnableNotification         bool              `json:"enableNotification"`
	EnablePullRequestPreview   bool              `json:"enablePullRequestPreview"`
	EnablePerformanceMode      bool              `json:"enablePerformanceMode,omitzero"`
	EnableSkewProtection       bool              `json:"enableSkewProtection,omitzero"`
}

// BranchOptions carries the optional Branch fields beyond the
// appId/branchName/description/stage/enableAutoBuild/tags quintet accepted
// directly by CreateBranch/UpdateBranch. See AppOptions for the
// nil-means-default-on-create/nil-means-unchanged-on-update convention.
type BranchOptions struct {
	EnvironmentVariables       map[string]string
	DisplayName                *string
	Framework                  *string
	TTL                        *string
	BasicAuthCredentials       *string
	BuildSpec                  *string
	BackendEnvironmentARN      *string
	PullRequestEnvironmentName *string
	SourceBranch               *string
	ComputeRoleARN             *string
	BackendStackARN            *string
	EnableBasicAuth            *bool
	EnableNotification         *bool
	EnableSkewProtection       *bool
	EnablePullRequestPreview   *bool
	EnablePerformanceMode      *bool
}

// JobStatus represents the status of a deployment job.
type JobStatus string

const (
	// JobStatusPending is the pending status.
	JobStatusPending JobStatus = "PENDING"
	// JobStatusProvisioning is the provisioning status.
	JobStatusProvisioning JobStatus = "PROVISIONING"
	// JobStatusRunning is the running status.
	JobStatusRunning JobStatus = "RUNNING"
	// JobStatusFailed is the failed status.
	JobStatusFailed JobStatus = "FAILED"
	// JobStatusSucceed is the succeed status.
	JobStatusSucceed JobStatus = "SUCCEED"
	// JobStatusCancelling is the cancelling status.
	JobStatusCancelling JobStatus = "CANCELLING"
	// JobStatusCancelled is the cancelled status.
	JobStatusCancelled JobStatus = "CANCELLED"
)

// JobType represents the type of deployment job.
type JobType string

const (
	// JobTypeRelease is a manual release job.
	JobTypeRelease JobType = "RELEASE"
	// JobTypeRetry is a retry job.
	JobTypeRetry JobType = "RETRY"
	// JobTypeManual is a direct-upload deployment.
	JobTypeManual JobType = "MANUAL"
	// JobTypeWebHook is a webhook-triggered job.
	JobTypeWebHook JobType = "WEB_HOOK"
)

// isValidJobType reports whether t is a real Amplify JobType enum value. An
// empty string is accepted by callers as "unspecified".
func isValidJobType(t string) bool {
	switch JobType(t) {
	case JobTypeRelease, JobTypeRetry, JobTypeManual, JobTypeWebHook:
		return true
	default:
		return false
	}
}

// Job represents an Amplify deployment job.
type Job struct {
	JobID      string    `json:"jobId"`
	JobARN     string    `json:"jobArn"`
	CommitID   string    `json:"commitId,omitzero"`
	CommitMsg  string    `json:"commitMessage,omitzero"`
	Status     JobStatus `json:"status"`
	Type       JobType   `json:"jobType"`
	StartTime  time.Time `json:"startTime"`
	EndTime    time.Time `json:"endTime,omitzero"`
	CommitTime time.Time `json:"commitTime,omitzero"`
	AppID      string    `json:"appId"`
	BranchName string    `json:"branchName"`
}

// DomainStatus represents the status of a domain association.
type DomainStatus string

const (
	// DomainStatusCreating is the creating status.
	DomainStatusCreating DomainStatus = "CREATING"
	// DomainStatusPendingVerification is waiting for verification.
	DomainStatusPendingVerification DomainStatus = "PENDING_VERIFICATION"
	// DomainStatusAvailable is the available status.
	DomainStatusAvailable DomainStatus = "AVAILABLE"
	// DomainStatusFailed is the failed status.
	DomainStatusFailed DomainStatus = "FAILED"
)

// SubDomainSetting represents a subdomain setting.
type SubDomainSetting struct {
	Prefix     string `json:"prefix"`
	BranchName string `json:"branchName"`
}

// SubDomain represents a configured subdomain.
type SubDomain struct {
	SubDomainSetting SubDomainSetting `json:"subDomainSetting"`
	DNSRecord        string           `json:"dnsRecord,omitzero"`
	Verified         bool             `json:"verified"`
}

// DomainAssociation represents an Amplify custom domain association.
type DomainAssociation struct {
	AppID                            string       `json:"appId"`
	DomainName                       string       `json:"domainName"`
	ARN                              string       `json:"domainAssociationArn"`
	DomainStatus                     DomainStatus `json:"domainStatus"`
	StatusReason                     string       `json:"statusReason,omitzero"`
	CertificateVerificationDNSRecord string       `json:"certificateVerificationDNSRecord,omitzero"`
	AutoSubDomainIAMRole             string       `json:"autoSubDomainIamRole,omitzero"`
	CertificateType                  string       `json:"certificateType,omitzero"`
	CertificateCustomArn             string       `json:"certificateCustomArn,omitzero"`
	SubDomains                       []SubDomain  `json:"subDomains"`
	AutoSubDomainCreationPatterns    []string     `json:"autoSubDomainCreationPatterns,omitempty"`
	EnableAutoSubDomain              bool         `json:"enableAutoSubDomain"`
}

// Webhook represents an Amplify webhook.
type Webhook struct {
	CreateTime  time.Time `json:"createTime"`
	UpdateTime  time.Time `json:"updateTime"`
	WebhookID   string    `json:"webhookId"`
	WebhookARN  string    `json:"webhookArn"`
	AppID       string    `json:"appId"`
	BranchName  string    `json:"branchName"`
	Description string    `json:"description,omitzero"`
	WebhookURL  string    `json:"webhookUrl"`
}

// BackendEnvironment represents an Amplify backend environment.
type BackendEnvironment struct {
	CreateTime            time.Time `json:"createTime"`
	UpdateTime            time.Time `json:"updateTime"`
	EnvironmentName       string    `json:"environmentName"`
	BackendEnvironmentARN string    `json:"backendEnvironmentArn"`
	AppID                 string    `json:"appId"`
	StackName             string    `json:"stackName,omitzero"`
	DeploymentArtifacts   string    `json:"deploymentArtifacts,omitzero"`
}

// Artifact represents an Amplify build artifact. AppID/BranchName/JobID
// identify the job whose build produced it (see ListArtifacts and
// janitor.go's advanceJobs, the only producer).
type Artifact struct {
	ArtifactID       string `json:"artifactId"`
	ArtifactType     string `json:"artifactType"`
	ArtifactFileName string `json:"artifactFileName"`
	AppID            string `json:"appId"`
	BranchName       string `json:"branchName"`
	JobID            string `json:"jobId"`
}
