package ocifilestorage

// DefaultPort is the dedicated control-plane listener's default TCP port.
const DefaultPort = 8171

// Engine modes (--oci-filestorage-engine).
const (
	EngineStub = "stub"
	EngineNFS  = "nfs"
)

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCIFileStorage.
type Settings struct {
	Engine      string `json:"engine"      env:"OCI_FILESTORAGE_ENGINE"       default:"stub" name:"engine"       help:"OCI File Storage engine: stub (control plane only) or nfs (each mount target serves its exports over NFSv3 on a loopback port, backed by a directory per file system)." enum:"stub,nfs"` //nolint:lll // config struct tags are intentionally verbose
	DataDir     string `json:"dataDir"     env:"OCI_FILESTORAGE_DATA_DIR"                    name:"data-dir"     help:"Directory holding file system data in nfs mode; empty uses a temporary directory removed on shutdown."`                                                                                  //nolint:lll // config struct tags are intentionally verbose
	Port        int    `json:"port"        env:"OCI_FILESTORAGE_PORT"         default:"8171" name:"port"         help:"Fixed TCP port for the dedicated OCI File Storage control-plane listener; startup fails if it is unavailable."`                                                                          //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool   `json:"enforceAuth" env:"OCI_FILESTORAGE_ENFORCE_AUTH"                name:"enforce-auth" help:"Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works."`                                      //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort, Engine: EngineStub} }
