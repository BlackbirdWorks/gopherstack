package ociloadbalancer

// DefaultPort is the dedicated control-plane listener's default TCP port.
const DefaultPort = 8165

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCILoadBalancer.
type Settings struct {
	Port                 int  `json:"port"                 env:"OCI_LOADBALANCER_PORT"                    default:"8165" name:"port"                    help:"Fixed TCP port for the dedicated OCI Load Balancer control-plane listener; startup fails if it is unavailable."`                                    //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth          bool `json:"enforceAuth"          env:"OCI_LOADBALANCER_ENFORCE_AUTH"                           name:"enforce-auth"            help:"Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works."` //nolint:lll // config struct tags are intentionally verbose
	AllowExternalBackend bool `json:"allowExternalBackend" env:"OCI_LOADBALANCER_ALLOW_EXTERNAL_BACKENDS"                name:"allow-external-backends" help:"Let load balancer listeners forward to, and health-check, backends outside loopback; by default the data plane only dials 127.0.0.0/8 and ::1."`    //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
