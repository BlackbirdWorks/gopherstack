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
	Tags          *tags.Tags `json:"tags,omitempty"`
	CreateTime    time.Time  `json:"createTime"`
	UpdateTime    time.Time  `json:"updateTime"`
	AppID         string     `json:"appId"`
	ARN           string     `json:"appArn"`
	Name          string     `json:"name"`
	Description   string     `json:"description,omitempty"`
	Repository    string     `json:"repository,omitempty"`
	DefaultDomain string     `json:"defaultDomain,omitempty"`
	Platform      Platform   `json:"platform"`
}

// Branch represents an Amplify app branch.
type Branch struct {
	Tags            *tags.Tags `json:"tags,omitempty"`
	CreateTime      time.Time  `json:"createTime"`
	UpdateTime      time.Time  `json:"updateTime"`
	AppID           string     `json:"appId"`
	BranchARN       string     `json:"branchArn"`
	BranchName      string     `json:"branchName"`
	Description     string     `json:"description,omitempty"`
	Stage           Stage      `json:"stage,omitempty"`
	EnableAutoBuild bool       `json:"enableAutoBuild"`
}
