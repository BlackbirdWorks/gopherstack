package gcpcontainer

// DefaultPort is the dedicated Kubernetes Engine gRPC and REST listener's default TCP port.
const DefaultPort = 8144

// Settings holds service-level configuration, embedded in the root CLI as CLI.GCPContainer.
type Settings struct {
	Engine       string `json:"engine"       env:"GKE_ENGINE"        default:"stub" name:"engine"        help:"GKE engine mode: stub (control plane only, synthetic endpoint) or docker (a real single-node k3s cluster per CreateCluster)."` //nolint:lll // config struct tags are intentionally verbose
	ClusterHost  string `json:"clusterHost"  env:"GKE_CLUSTER_HOST"  default:""     name:"cluster-host"  help:"Host the docker-engine API servers are reachable on (default 127.0.0.1)."`                                                     //nolint:lll // config struct tags are intentionally verbose
	ClusterToken string `json:"clusterToken" env:"GKE_CLUSTER_TOKEN" default:""     name:"cluster-token" help:"Static cluster-admin bearer token of docker-engine clusters; required when GKE_CLUSTER_HOST is not loopback."`                 //nolint:lll // config struct tags are intentionally verbose
	K3sImage     string `json:"k3sImage"     env:"GKE_K3S_IMAGE"     default:""     name:"k3s-image"     help:"Override the rancher/k3s image used by the docker engine."`                                                                    //nolint:lll // config struct tags are intentionally verbose
	Port         int    `json:"port"         env:"GCP_CONTAINER_PORT" default:"8144" name:"port"          help:"Fixed TCP port for the dedicated Kubernetes Engine gRPC and REST listener; startup fails if it is unavailable."`              //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort, Engine: EngineStub}
}

// Engine modes.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)
