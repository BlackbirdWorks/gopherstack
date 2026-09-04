package azurequeue

// DefaultPort is Azure Queue's fixed, protocol-conventional TCP port. This
// follows the same pattern as services/azureblob's DefaultPort (10000) and,
// before that, services/iot's MQTT broker (1883): pick one default and try
// to bind exactly that, rather than drawing from cli.go's shared
// --port-range-start/--port-range-end PortAlloc pool (used for on-demand
// ephemeral resources like Lambda function URLs and ElastiCache, not fixed
// service ports) or inventing an alternative numbering scheme. The default
// value itself (10001) is Azurite's own Queue service port, so unmodified
// UseDevelopmentStorage=true-style SDK configuration works out of the box;
// see AZURE.md section 4 for the full rationale, including why this
// deliberately does NOT fall back into the shared PortAlloc pool if 10001 is
// taken (StartWorker fails fast instead -- see handler.go).
const DefaultPort = 10001

// Settings holds service-level configuration for the Azure Queue backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command (see cli.go's CLI.AzureQueue field), mirroring
// services/azureblob's Settings pattern.
type Settings struct {
	// Port is the fixed TCP port for the dedicated Queue listener. See
	// handler.go's StartWorker for what happens when it's unavailable
	// (fails fast; no fallback pool, matching services/azureblob).
	Port int `json:"port" env:"AZURE_QUEUE_PORT" default:"10001" name:"port" help:"Fixed TCP port for the dedicated Azure Queue listener; startup fails if it's unavailable (no fallback pool)."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the default Settings. Used when no ConfigProvider
// is available at init time (e.g. tests constructing a Provider directly).
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
