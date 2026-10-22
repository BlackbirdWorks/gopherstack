package bedrockagentcore

import "time"

// Settings holds the data-plane configuration, embedded in the root CLI command as CLI.AgentCore.
type Settings struct {
	Engine       string        `json:"engine"       env:"AGENTCORE_ENGINE"        default:"auto" name:"engine"        enum:"auto,none" help:"Container engine for agent runtimes and the code interpreter: auto (container runtime if reachable) or none."`             //nolint:lll // config struct tags are intentionally verbose
	RegistryHost string        `json:"registryHost" env:"AGENTCORE_REGISTRY_HOST" default:""     name:"registry-host"                  help:"host:port that replaces the ECR registry host when pulling agent runtime images (for example the emulated ECR registry)."` //nolint:lll // config struct tags are intentionally verbose
	PythonImage  string        `json:"pythonImage"  env:"AGENTCORE_PYTHON_IMAGE"  default:""     name:"python-image"                   help:"Code interpreter sandbox image; empty uses python:3.12-slim."`                                                             //nolint:lll // config struct tags are intentionally verbose
	IdleTimeout  time.Duration `json:"idleTimeout"  env:"AGENTCORE_IDLE_TIMEOUT"  default:"15m"  name:"idle-timeout"                   help:"Idle time after which a warm agent runtime session is stopped when the runtime sets no lifecycle timeout."`                //nolint:lll // config struct tags are intentionally verbose
}

// ConfigProvider extracts AgentCore data-plane configuration from the AppContext Config.
type ConfigProvider interface {
	GetAgentCoreSettings() Settings
	GetAgentCoreEndpointURL() string
}
