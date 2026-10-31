package gcpapigateway

// DefaultPort is the dedicated API Gateway listener's default TCP port: control plane (gRPC and REST) and data plane.
const DefaultPort = 8146

// Settings holds service-level configuration, embedded in the root CLI as CLI.GCPAPIGateway.
type Settings struct {
	Port                 int  `json:"port"                 env:"GCP_APIGATEWAY_PORT"                    default:"8146"  name:"port"                    help:"Fixed TCP port for the dedicated API Gateway listener (control plane and gateway data plane); startup fails if it is unavailable."`                                                     //nolint:lll // config struct tags are intentionally verbose
	AllowExternalBackend bool `json:"allowExternalBackend" env:"GCP_APIGATEWAY_ALLOW_EXTERNAL_BACKENDS" default:"false" name:"allow-external-backends" help:"Let gateway backends and remote JWKS URIs reach hosts outside loopback; by default the data plane only dials 127.0.0.0/8 and ::1 (Cloud Run and Functions *.localhost URLs included)."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
