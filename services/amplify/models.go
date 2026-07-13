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

// Stage represents the branch deployment stage.
type Stage string

const (
	// StageProduction is the production stage.
	StageProduction Stage = "PRODUCTION"
	// StageStaging is the staging stage.
	StageStaging Stage = "STAGING"
	// StageDevelopment is the development stage.
	StageDevelopment Stage = "DEVELOPMENT"
	// StageExperimental is the experimental stage.
	StageExperimental Stage = "EXPERIMENTAL"
)

// App represents an Amplify application.
type App struct {
	Tags          *tags.Tags `json:"tags,omitzero"`
	CreateTime    time.Time  `json:"createTime"`
	UpdateTime    time.Time  `json:"updateTime"`
	AppID         string     `json:"appId"`
	ARN           string     `json:"appArn"`
	Name          string     `json:"name"`
	Description   string     `json:"description,omitzero"`
	Repository    string     `json:"repository,omitzero"`
	DefaultDomain string     `json:"defaultDomain,omitzero"`
	Platform      Platform   `json:"platform"`
}

// Branch represents an Amplify app branch.
type Branch struct {
	Tags            *tags.Tags `json:"tags,omitzero"`
	CreateTime      time.Time  `json:"createTime"`
	UpdateTime      time.Time  `json:"updateTime"`
	AppID           string     `json:"appId"`
	BranchARN       string     `json:"branchArn"`
	BranchName      string     `json:"branchName"`
	Description     string     `json:"description,omitzero"`
	Stage           Stage      `json:"stage,omitzero"`
	EnableAutoBuild bool       `json:"enableAutoBuild"`
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
	SubDomains                       []SubDomain  `json:"subDomains"`
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

// Artifact represents an Amplify build artifact.
type Artifact struct {
	ArtifactID       string `json:"artifactId"`
	ArtifactType     string `json:"artifactType"`
	ArtifactFileName string `json:"artifactFileName"`
}
