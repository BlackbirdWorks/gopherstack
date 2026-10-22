// Package bedrockagentcore emulates the Amazon Bedrock AgentCore data plane: agent runtime invocation, memory
// events and records, code interpreter and browser sessions, and workload identity tokens.
package bedrockagentcore

import "errors"

// Scope identifies the account and Region a request operates in.
type Scope struct{ Account, Region string }

// Errors a Control reports for runtime lookups.
var (
	ErrNotFound = errors.New("resource not found")
	ErrNotReady = errors.New("resource is not ready")
)

// RuntimeInfo is the deployable view of an agent runtime endpoint.
type RuntimeInfo struct {
	Env          map[string]string
	ID           string
	ImageURI     string
	Protocol     string
	IdleSeconds  int
	MaxLifetimeS int
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

// Control resolves the control-plane resources the data plane operates on.
type Control interface {
	Runtime(sc Scope, ref, qualifier string) (RuntimeInfo, error)
	Memory(sc Scope, id string) (MemoryInfo, bool)
	Tool(sc Scope, kind ToolKind, id string) (ToolInfo, bool)
	WorkloadIdentity(sc Scope, name string) bool
	Provider(sc Scope, kind ProviderKind, name string) (ProviderInfo, bool)
	SecretValue(arn, jsonKey string) (string, bool)
}
