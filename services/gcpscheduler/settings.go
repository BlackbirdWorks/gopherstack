package gcpscheduler

// DefaultPort is the dedicated gRPC listener's default TCP port.
const DefaultPort = 8124

// Settings holds service-level configuration, embedded in the root CLI
// command as CLI.GCPScheduler.
type Settings struct {
	AppEngineBaseURL string `json:"appEngineBaseUrl" env:"GCP_SCHEDULER_APPENGINE_URL" default:""     name:"appengine-url" help:"Base URL that AppEngineHttpTarget jobs are dispatched to; when empty they fail with FAILED_PRECONDITION."`   //nolint:lll // config struct tags are intentionally verbose
	Port             int    `json:"port"             env:"GCP_SCHEDULER_PORT"          default:"8124" name:"port"          help:"Fixed TCP port for the dedicated Google Cloud Scheduler gRPC listener; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
