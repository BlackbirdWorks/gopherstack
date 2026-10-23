package gcpcloudbuild

const (
	// DefaultPort is the dedicated gRPC and REST listener's default TCP port.
	DefaultPort = 8136
	// EngineAuto runs build steps as containers when a container runtime is reachable.
	EngineAuto = "auto"
	// EngineNone never runs a container: builds are recorded and then fail with a clear status detail.
	EngineNone = "none"
)

// Settings holds service-level configuration, embedded in the root CLI command as CLI.GCPCloudBuild.
type Settings struct {
	Engine string `json:"engine" env:"GCP_CLOUDBUILD_ENGINE" default:"auto" name:"engine" enum:"auto,none" help:"Build execution: auto (run each build step as a container when a runtime is reachable) or none (builds fail with a status detail)."` //nolint:lll // config struct tags are intentionally verbose
	Port   int    `json:"port"   env:"GCP_CLOUDBUILD_PORT"   default:"8136" name:"port"                    help:"Fixed TCP port for the dedicated Cloud Build gRPC and REST listener; startup fails if it is unavailable."`                           //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Engine: EngineAuto, Port: DefaultPort} }
