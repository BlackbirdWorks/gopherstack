package ociapigateway

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 8163

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCIAPIGateway.
type Settings struct {
	Port                 int  `json:"port"                 env:"OCI_APIGATEWAY_PORT"                    default:"8163" name:"port"                    help:"Fixed TCP port for the dedicated OCI API Gateway listener (control plane and data plane); startup fails if it is unavailable."`                     //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth          bool `json:"enforceAuth"          env:"OCI_APIGATEWAY_ENFORCE_AUTH"                           name:"enforce-auth"            help:"Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works."` //nolint:lll // config struct tags are intentionally verbose
	AllowExternalBackend bool `json:"allowExternalBackend" env:"OCI_APIGATEWAY_ALLOW_EXTERNAL_BACKENDS"                name:"allow-external-backends" help:"Let HTTP backends and remote JWKS URIs reach hosts outside loopback; by default the data plane only dials 127.0.0.0/8 and ::1."`                    //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
