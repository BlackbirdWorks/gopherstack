package gcpcloudtasks

// DefaultPort is the dedicated gRPC listener's default TCP port.
const DefaultPort = 8123

// Settings holds service-level configuration, embedded in the root CLI
// command as CLI.GCPCloudTasks.
type Settings struct {
	AppEngineBaseURL string `json:"appEngineBaseUrl" env:"GCP_CLOUDTASKS_APPENGINE_URL" default:""     name:"appengine-url" help:"Base URL that AppEngineHttpRequest tasks are dispatched to; when empty they fail with FAILED_PRECONDITION."` //nolint:lll // config struct tags are intentionally verbose
	Port             int    `json:"port"             env:"GCP_CLOUDTASKS_PORT"          default:"8123" name:"port"          help:"Fixed TCP port for the dedicated Google Cloud Tasks gRPC listener; startup fails if it is unavailable."`     //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
