package gcpidentitytoolkit

// DefaultPort is the dedicated Identity Platform listener's default TCP port (FIREBASE_AUTH_EMULATOR_HOST).
const DefaultPort = 8145

// DefaultProject is the project client calls resolve to when the request names none.
const DefaultProject = "gopherstack-local"

// Settings holds service-level configuration, embedded in the root CLI as CLI.GCPIdentityToolkit.
type Settings struct {
	DefaultProject string `json:"defaultProject" env:"GCP_IDENTITYTOOLKIT_PROJECT" default:"gopherstack-local" name:"project" help:"Project that client calls (API-key requests without a /projects/ path) resolve to."`                                                             //nolint:lll // config struct tags are intentionally verbose
	Port           int    `json:"port"           env:"GCP_IDENTITYTOOLKIT_PORT"    default:"8145"              name:"port"    help:"Fixed TCP port for the dedicated Identity Platform / Firebase Auth listener (FIREBASE_AUTH_EMULATOR_HOST); startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort, DefaultProject: DefaultProject}
}
