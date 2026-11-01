package ocicontainerengine

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 8158

// Engine modes (OKE_ENGINE).
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCIContainerEngine.
type Settings struct {
	Engine      string `json:"engine"      env:"OKE_ENGINE"                        default:"stub" name:"engine"       help:"OKE engine mode: stub (control plane only) or docker (real single-node k3s cluster per CreateCluster)."`                              //nolint:lll // config struct tags are intentionally verbose
	Port        int    `json:"port"        env:"OCI_CONTAINER_ENGINE_PORT"         default:"8158" name:"port"         help:"Fixed TCP port for the dedicated OCI Container Engine for Kubernetes (OKE) listener; startup fails if it is unavailable."`            //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool   `json:"enforceAuth" env:"OCI_CONTAINER_ENGINE_ENFORCE_AUTH"                name:"enforce-auth" help:"Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
