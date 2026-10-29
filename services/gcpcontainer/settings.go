package gcpcontainer

// DefaultPort is the dedicated Kubernetes Engine gRPC and REST listener's default TCP port.
const DefaultPort = 8144

// Engine modes (GKE_ENGINE).
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

// Settings holds service-level configuration, embedded in the root CLI as CLI.GCPContainer.
type Settings struct {
	Port int `json:"port" env:"GCP_CONTAINER_PORT" default:"8144" name:"port" help:"Fixed TCP port for the dedicated Kubernetes Engine gRPC and REST listener; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
