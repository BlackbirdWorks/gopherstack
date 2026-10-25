package ociobjectstorage

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 8150

// DefaultNamespace is the tenancy namespace served unless overridden.
const DefaultNamespace = "gopherstack"

// Settings holds service-level configuration, embedded in the root CLI
// command as CLI.OCIObjectStorage.
type Settings struct {
	Namespace   string `json:"namespace" env:"OCI_OBJECTSTORAGE_NAMESPACE" default:"gopherstack" name:"namespace" help:"Object Storage namespace the emulator serves; other namespaces return NamespaceNotFound."` //nolint:lll // config struct tags are intentionally verbose
	Port        int    `json:"port" env:"OCI_OBJECTSTORAGE_PORT" default:"8150" name:"port" help:"Fixed TCP port for the dedicated OCI Object Storage listener; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool   `json:"enforceAuth" env:"OCI_OBJECTSTORAGE_ENFORCE_AUTH" name:"enforce-auth" help:"Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort, Namespace: DefaultNamespace}
}
