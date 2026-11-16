// Package bedrockagentcore emulates the Amazon Bedrock AgentCore data plane: agent runtime invocation, memory
// events and records, code interpreter and browser sessions, and workload identity tokens.
package bedrockagentcore

import (
	"context"
	"errors"
	"net/http"
)

// Scope identifies the account and Region a request operates in.
type Scope struct{ Account, Region string }

// Errors a Control reports for runtime lookups.
var (
	ErrNotFound = errors.New("resource not found")
	ErrNotReady = errors.New("resource is not ready")
)

// AuthFailure is a request the runtime's inbound authorizer rejected.
type AuthFailure struct {
	Challenge string
	Message   string
	Status    int
}

// CodeInfo is a direct code deployment: a zip package run by a managed language runtime.
type CodeInfo struct {
	// Load returns the zip package, read from the S3 location of the runtime.
	Load       func() ([]byte, error)
	Runtime    string
	EntryPoint []string
}

// RuntimeInfo is the deployable view of an agent runtime endpoint.
type RuntimeInfo struct {
	Env       map[string]string
	Authorize func(ctx context.Context, r *http.Request) *AuthFailure
	// ProtectedResource builds the RFC 9728 metadata document; nil when the runtime has no JWT authorizer.
	ProtectedResource func(r *http.Request) map[string]any
	Code              *CodeInfo
	ID                string
	ImageURI          string
	Protocol          string
	IdleSeconds       int
	MaxLifetimeS      int
}

// StrategyInfo is one memory strategy of a memory.
type StrategyInfo struct{ ID, Name, Type string }

// MemoryInfo is the control-plane definition of a memory.
type MemoryInfo struct {
	ID         string
	Strategies []StrategyInfo
	ExpiryDays int
}

// ToolKind selects the sandbox family.
type ToolKind string

// Sandbox families.
const (
	ToolBrowser         ToolKind = "browser"
	ToolCodeInterpreter ToolKind = "codeInterpreter"
)

// ToolInfo is a browser or code interpreter definition.
type ToolInfo struct {
	ID   string
	Mode string
}

// ProviderKind selects a credential provider family.
type ProviderKind string

// Credential provider families.
const (
	ProviderAPIKey ProviderKind = "apiKey"
	ProviderOAuth2 ProviderKind = "oauth2"
)

// ProviderInfo is a credential provider with the references needed to resolve its secrets.
type ProviderInfo struct {
	Config       map[string]any
	Name         string
	Vendor       string
	SecretARN    string
	SecretJSONID string
}

// ErrInvalidFederation marks a three-legged OAuth request the control plane rejects as invalid.
var ErrInvalidFederation = errors.New("invalid user federation request")

// FederationRequest is a USER_FEDERATION GetResourceOauth2Token call.
type FederationRequest struct {
	CustomParameters map[string]string
	Provider         string
	Workload         string
	UserID           string
	ReturnURL        string
	CustomState      string
	SessionURI       string
	Scopes           []string
	Audiences        []string
	Resources        []string
	Force            bool
}

// FederationResult is the outcome: a token, or an authorization URL with the session to resume.
type FederationResult struct {
	AccessToken      string
	AuthorizationURL string
	SessionURI       string
	InProgress       bool
	Failed           bool
}

// UserIdentifier names the user of a federation session by id or by a verifiable user token.
type UserIdentifier struct{ UserID, UserToken string }

// Control resolves the control-plane resources the data plane operates on.
type Control interface {
	Runtime(sc Scope, ref, qualifier string) (RuntimeInfo, error)
	Memory(sc Scope, id string) (MemoryInfo, bool)
	Tool(sc Scope, kind ToolKind, id string) (ToolInfo, bool)
	WorkloadIdentity(sc Scope, name string) bool
	Provider(sc Scope, kind ProviderKind, name string) (ProviderInfo, bool)
	SecretValue(arn, jsonKey string) (string, bool)
	FederatedToken(ctx context.Context, sc Scope, req FederationRequest) (FederationResult, error)
	CompleteFederation(ctx context.Context, sc Scope, sessionURI string, user UserIdentifier) error
	VerifyUserJWT(ctx context.Context, token string) (string, error)
}
