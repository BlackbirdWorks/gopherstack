package azuretable

// DefaultPort is Azure Table's fixed, protocol-conventional TCP port. This
// follows the same pattern as services/azureblob's DefaultPort (10000) and
// services/azurequeue's DefaultPort (10001): pick one default and try to
// bind exactly that, rather than drawing from cli.go's shared
// --port-range-start/--port-range-end PortAlloc pool. The default value
// itself (10002) is Azurite's own Table service port, so unmodified
// UseDevelopmentStorage=true-style SDK configuration works out of the box;
// see AZURE.md section 4 for the full rationale, including why this
// deliberately does NOT fall back into the shared PortAlloc pool if 10002 is
// taken (StartWorker fails fast instead -- see handler.go).
const DefaultPort = 10002

// Settings holds service-level configuration for the Azure Table backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command (see cli.go's CLI.AzureTable field), mirroring
// services/azurequeue's Settings pattern.
type Settings struct {
	// Port is the fixed TCP port for the dedicated Table listener. See
	// handler.go's StartWorker for what happens when it's unavailable
	// (fails fast; no fallback pool, matching services/azureblob and
	// services/azurequeue).
	Port int `json:"port" env:"AZURE_TABLE_PORT" default:"10002" name:"port" help:"Fixed TCP port for the dedicated Azure Table listener; startup fails if it's unavailable (no fallback pool)."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the default Settings. Used when no ConfigProvider
// is available at init time (e.g. tests constructing a Provider directly).
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
