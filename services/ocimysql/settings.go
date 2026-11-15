package ocimysql

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 8159

// Engine modes (OCI_MYSQL_ENGINE).
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCIMySQL.
type Settings struct {
	Engine            string `json:"engine"            env:"OCI_MYSQL_ENGINE"              default:"stub"   name:"engine"              help:"OCI MySQL engine mode: stub (control plane only) or docker (each ACTIVE DB system runs a real MySQL container)."`                     //nolint:lll // config struct tags are intentionally verbose
	Port              int    `json:"port"              env:"OCI_MYSQL_PORT"                default:"8159"   name:"port"                help:"Fixed TCP port for the dedicated OCI MySQL HeatWave Database Service listener; startup fails if it is unavailable."`                  //nolint:lll // config struct tags are intentionally verbose
	MaxManualBackups  int    `json:"maxManualBackups"  env:"OCI_MYSQL_MAX_MANUAL_BACKUPS"  default:"1200"   name:"max-manual-backups"  help:"Service limit: manual backups that may exist at once."`                                                                               //nolint:lll // config struct tags are intentionally verbose
	MaxStorageGB      int    `json:"maxStorageGb"      env:"OCI_MYSQL_MAX_STORAGE_GB"      default:"100000" name:"max-storage-gb"      help:"Service limit: data storage in GB per availability domain."`                                                                          //nolint:lll // config struct tags are intentionally verbose
	MaxShapeInstances int    `json:"maxShapeInstances" env:"OCI_MYSQL_MAX_SHAPE_INSTANCES" default:"100"    name:"max-shape-instances" help:"Service limit: DB systems of one shape per availability domain (a tenth of it for MySQL.256)."`                                       //nolint:lll // config struct tags are intentionally verbose
	MaxParallelCopies int    `json:"maxParallelCopies" env:"OCI_MYSQL_MAX_PARALLEL_COPIES" default:"5"      name:"max-parallel-copies" help:"Service limit: cross-region backup copies in progress at once."`                                                                      //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth       bool   `json:"enforceAuth"       env:"OCI_MYSQL_ENFORCE_AUTH"                         name:"enforce-auth"        help:"Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{
		Port: DefaultPort, MaxManualBackups: DefaultMaxManualBackups, MaxStorageGB: DefaultMaxStorageGB,
		MaxShapeInstances: DefaultMaxShapeInstances, MaxParallelCopies: DefaultMaxParallelCopies,
	}
}
