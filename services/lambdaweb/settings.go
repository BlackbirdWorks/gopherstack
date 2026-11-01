package lambdaweb

// Engine modes for the web function data plane.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"

	defaultRuntimeImage = "docker.io/library/node:24-slim"
)

// Settings holds service-level configuration for Lambda Web Functions. Kong picks the fields up when the struct is
// embedded in the root CLI command.
type Settings struct {
	Engine       string `json:"engine"        env:"LAMBDAWEB_ENGINE"        default:"stub"                           help:"Web function data plane: stub (control plane only) or docker (one sandbox container per serving revision)."` //nolint:lll // Kong struct tag makes this line long
	RuntimeImage string `json:"runtime_image" env:"LAMBDAWEB_RUNTIME_IMAGE" default:"docker.io/library/node:24-slim" help:"Container image that runs nodejs24.x web function revisions (needs node on PATH)."`                          //nolint:lll // Kong struct tag makes this line long
}

// ConfigProvider supplies Lambda Web Functions settings from the application config.
type ConfigProvider interface {
	GetLambdaWebSettings() Settings
}
