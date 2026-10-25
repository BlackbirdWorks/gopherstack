package ocivault

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 8151

// Settings holds service-level configuration, embedded in the root CLI
// command as CLI.OCIVault.
type Settings struct {
	Port        int  `json:"port"        env:"OCI_VAULT_PORT"         default:"8151" name:"port"         help:"Fixed TCP port for the dedicated OCI Vault (KMS, Vault secrets, Secret retrieval) listener; startup fails if it is unavailable."`     //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool `json:"enforceAuth" env:"OCI_VAULT_ENFORCE_AUTH"                name:"enforce-auth" help:"Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
