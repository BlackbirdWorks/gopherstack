package azuresql

import "time"

const (
	// EngineAuto launches SQL Server containers when a container runtime is reachable.
	EngineAuto = "auto"
	// EngineNone never starts containers: servers are state-only.
	EngineNone = "none"
	// DefaultImage is the SQL Server image servers run.
	DefaultImage = "mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04"
	// DefaultStartupTimeout bounds how long a server container may take to accept connections.
	DefaultStartupTimeout = 5 * time.Minute
)

// Settings is the Kong-embedded configuration of the Azure SQL Database emulator.
type Settings struct {
	Engine         string        `json:"engine"         env:"AZURE_SQL_ENGINE"          name:"engine"          default:"auto"                                                  enum:"auto,none" help:"Database engine: auto (SQL Server containers if a container runtime is reachable, else state-only) or none."` //nolint:lll // config struct tags are intentionally verbose
	Image          string        `json:"image"          env:"AZURE_SQL_IMAGE"           name:"image"           default:"mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04"                  help:"SQL Server container image (linux/amd64 or arm64 per host)."`                                                 //nolint:lll // config struct tags are intentionally verbose
	StartupTimeout time.Duration `json:"startupTimeout" env:"AZURE_SQL_STARTUP_TIMEOUT" name:"startup-timeout" default:"5m"                                                                     help:"How long a server container may take to pull its image and accept connections."`                              //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider exists.
func DefaultSettings() Settings {
	return Settings{Engine: EngineAuto, Image: DefaultImage, StartupTimeout: DefaultStartupTimeout}
}
