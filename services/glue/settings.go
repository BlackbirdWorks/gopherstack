package glue

// Engine modes for interactive sessions.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

// Settings holds service-level configuration for the Glue backend.
type Settings struct {
	Engine      string `json:"engine"       env:"GLUE_ENGINE"       default:"stub" help:"Glue interactive-session engine: stub (statements fail with an engine-disabled error) or docker (run statements in a Spark/Python container)."` //nolint:lll // Kong struct tag makes this line long
	EngineImage string `json:"engine_image" env:"GLUE_ENGINE_IMAGE" default:""     help:"Image for docker-engine sessions; empty picks the AWS Glue libs image matching the session GlueVersion."`                                       //nolint:lll // Kong struct tag makes this line long
}

// ConfigProvider supplies Glue settings from the application config.
type ConfigProvider interface {
	GetGlueSettings() Settings
}
