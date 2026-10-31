package ociartifacts

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 8166

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCIArtifacts.
type Settings struct {
	Port        int  `json:"port"        env:"OCI_ARTIFACTS_PORT"         default:"8166" name:"port"         help:"Fixed TCP port for the dedicated OCI Artifacts and Container Registry listener (control plane, generic artifact content and Docker Registry v2); startup fails if it is unavailable."`                                            //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool `json:"enforceAuth" env:"OCI_ARTIFACTS_ENFORCE_AUTH"                name:"enforce-auth" help:"Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works. Registry logins always need a namespace/user name and a non-empty auth token."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
