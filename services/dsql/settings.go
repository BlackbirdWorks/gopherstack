package dsql

// Engine modes for the DSQL data plane.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"

	defaultEngineImage = "postgres:17"
)

// Settings holds service-level configuration for the DSQL backend. Kong picks the fields up when the struct is
// embedded in the root CLI command.
type Settings struct {
	Engine      string `json:"engine"       env:"DSQL_ENGINE"       default:"stub"          help:"DSQL data plane: stub (control plane only) or docker (one PostgreSQL container per cluster behind an IAM-token proxy)."` //nolint:lll // Kong struct tag makes this line long
	EngineImage string `json:"engine_image" env:"DSQL_ENGINE_IMAGE" default:"postgres:17" help:"PostgreSQL image backing each cluster in docker engine mode."`                                                                    //nolint:lll // Kong struct tag makes this line long
}

// ConfigProvider supplies DSQL settings from the application config.
type ConfigProvider interface {
	GetDSQLSettings() Settings
}

// CADirConfig locates the directory persisting the data-plane CA; empty keeps it in memory.
type CADirConfig interface {
	GetDSQLCADir() string
}
