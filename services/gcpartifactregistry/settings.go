package gcpartifactregistry

// DefaultPort is the dedicated gRPC, REST and Docker Registry listener's default TCP port.
const DefaultPort = 8134

// DefaultRegistryHost is the host advertised in repositories' registry_uri.
const DefaultRegistryHost = "localhost"

// Settings holds service-level configuration, embedded in the root CLI command as CLI.GCPArtifactRegistry.
type Settings struct {
	RegistryHost string `json:"registryHost" env:"GCP_ARTIFACTREGISTRY_REGISTRY_HOST" name:"registry-host" default:"localhost" help:"Host (without the port) advertised in a repository's registry_uri and docker image URIs."`                                        //nolint:lll // config struct tags are intentionally verbose
	Port         int    `json:"port"         env:"GCP_ARTIFACTREGISTRY_PORT"          name:"port"          default:"8134"      help:"Fixed TCP port for the dedicated Artifact Registry gRPC, REST and Docker Registry listener; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort, RegistryHost: DefaultRegistryHost}
}
