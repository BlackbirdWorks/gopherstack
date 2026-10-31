package ocicontainerinstances

// DefaultPort is the dedicated control-plane listener's default TCP port.
const DefaultPort = 8167

// Engine modes (--oci-container-instances-engine).
const (
	EngineState  = "state"
	EngineDocker = "docker"
)

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCIContainerInstances.
type Settings struct {
	Engine      string `json:"engine"      env:"OCI_CONTAINER_INSTANCES_ENGINE"       default:"state" name:"engine"       help:"Container Instances engine: state (lifecycle only, no containers run) or docker (run the containers in the container runtime)." enum:"state,docker"` //nolint:lll // config struct tags are intentionally verbose
	Port        int    `json:"port"        env:"OCI_CONTAINER_INSTANCES_PORT"         default:"8167"  name:"port"         help:"Fixed TCP port for the dedicated OCI Container Instances listener; startup fails if it is unavailable."`                                             //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool   `json:"enforceAuth" env:"OCI_CONTAINER_INSTANCES_ENFORCE_AUTH"                 name:"enforce-auth" help:"Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works."`  //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort, Engine: EngineState} }
