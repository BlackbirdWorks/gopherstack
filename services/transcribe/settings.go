package transcribe

// Engine modes for speech recognition.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

// Settings holds service-level configuration for the Transcribe backend.
type Settings struct {
	Engine      string `json:"engine"       env:"TRANSCRIBE_ENGINE"       default:"stub" help:"Transcribe engine mode: stub (jobs finish with a valid but empty transcript) or docker (a Vosk speech model transcribes the media)."` //nolint:lll // Kong struct tag makes this line long
	EngineImage string `json:"engine_image" env:"TRANSCRIBE_ENGINE_IMAGE" default:""     help:"Vosk server image for the docker engine; empty uses the pinned default."`                                                             //nolint:lll // Kong struct tag makes this line long
	ModelDir    string `json:"model_dir"    env:"TRANSCRIBE_MODEL_DIR"    default:""     help:"Directory caching downloaded Vosk models; empty uses the user cache directory."`                                                      //nolint:lll // Kong struct tag makes this line long
}

// ConfigProvider supplies Transcribe settings from the application config.
type ConfigProvider interface {
	GetTranscribeSettings() Settings
}
