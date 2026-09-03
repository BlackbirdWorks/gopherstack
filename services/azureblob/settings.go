package azureblob

import (
	"os"
	"strconv"
)

// DefaultPort mirrors Azurite's default Blob service port, so unmodified
// Azurite-targeting SDK configuration (UseDevelopmentStorage=true, default
// connection strings) works out of the box. See AZURE.md section 4/5.
const DefaultPort = 10000

// envPortOverride lets a deployment move the dedicated Blob listener off
// DefaultPort (e.g. because 10000 is already in use for something else on
// the host).
const envPortOverride = "AZURE_BLOB_PORT"

// Settings holds service-level configuration for the Azure Blob backend.
type Settings struct {
	// Port is the preferred TCP port for the dedicated Blob listener.
	// See provider.go's resolvePort for what happens when it's unavailable.
	Port int
}

// DefaultSettings returns the default Settings, honoring envPortOverride.
func DefaultSettings() Settings {
	return Settings{Port: portFromEnv()}
}

func portFromEnv() int {
	if v := os.Getenv(envPortOverride); v != "" {
		if p, err := strconv.Atoi(v); err == nil && p > 0 && p < 65536 {
			return p
		}
	}

	return DefaultPort
}
