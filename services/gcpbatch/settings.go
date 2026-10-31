package gcpbatch

const (
	// DefaultPort is the dedicated gRPC and REST listener's default TCP port.
	DefaultPort = 8147
	// EngineNone records jobs and progresses them on the clock without running any container.
	EngineNone = "none"
	// EngineDocker runs every task's runnables as containers.
	EngineDocker = "docker"
	// DefaultScriptImage runs script runnables, which Batch executes on the VM host.
	DefaultScriptImage = "debian:bookworm-slim"
)

// Settings holds service-level configuration, embedded in the root CLI command as CLI.GCPBatch.
type Settings struct {
	Engine      string `json:"engine"      env:"GCP_BATCH_ENGINE"       default:"none"                 name:"engine"       enum:"none,docker" help:"Task execution: none (jobs progress QUEUED, SCHEDULED, RUNNING, SUCCEEDED on the clock without running anything) or docker (run each task's runnables as containers)."` //nolint:lll // config struct tags are intentionally verbose
	ScriptImage string `json:"scriptImage" env:"GCP_BATCH_SCRIPT_IMAGE" default:"debian:bookworm-slim" name:"script-image"                    help:"Container image that executes script runnables when the docker engine is on (Batch runs them on the VM host)."`                                                         //nolint:lll // config struct tags are intentionally verbose
	Port        int    `json:"port"        env:"GCP_BATCH_PORT"         default:"8147"                 name:"port"                            help:"Fixed TCP port for the dedicated Batch gRPC and REST listener; startup fails if it is unavailable."`                                                                    //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Engine: EngineNone, Port: DefaultPort, ScriptImage: DefaultScriptImage}
}
