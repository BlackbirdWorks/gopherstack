package azurefile

// DefaultPort is Azure Files' fixed listener port; Azurite has no Files
// service, so this is the first free slot after the Azure data-plane block.
const DefaultPort = 10009

// Settings holds Azure Files configuration, embedded in the root CLI.
type Settings struct {
	Port       int  `json:"port"       env:"AZURE_FILE_PORT"        default:"10009" name:"port"        help:"Fixed TCP port for the dedicated Azure Files listener; startup fails if it's unavailable (no fallback pool)."` //nolint:lll // config struct tags are intentionally verbose
	VerifyAuth bool `json:"verifyAuth" env:"AZURE_FILE_VERIFY_AUTH"                 name:"verify-auth" help:"Reject Shared Key and SAS requests for the development account whose signature is wrong or expired."`          //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the default Settings.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
