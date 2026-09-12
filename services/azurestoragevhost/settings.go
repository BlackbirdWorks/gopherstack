package azurestoragevhost

// DefaultPort is the fixed TCP port for the shared virtual-hosted-style
// storage listener. Unlike services/azureblob/azurequeue/azuretable's own
// fixed, protocol-conventional ports (10000/10001/10002, chosen to match
// Azurite's own defaults), this port has no real-Azure or Azurite
// equivalent -- it exists purely to satisfy terraform-provider-azurerm's
// data-plane SDK (jackofallops/giovanni), which requires Blob/Queue/Table
// to share one domain suffix (and therefore one port, since Go's
// url.URL.Host always includes the port) -- see AZURE.md section 10.8.
const DefaultPort = 10010

// Settings holds service-level configuration for the shared Azure Storage
// virtual-hosted listener. Mirrors services/azureblob's Settings pattern.
type Settings struct {
	// Port is the fixed TCP port for the dedicated virtual-hosted-style
	// storage listener. See handler.go's StartWorker for what happens when
	// it's unavailable (fails fast; no fallback pool, matching
	// services/azureblob/azurequeue/azuretable).
	Port int `json:"port" env:"AZURE_STORAGE_VHOST_PORT" default:"10010" name:"port" help:"Fixed TCP port for the shared Azure Storage virtual-hosted-style listener (Blob/Queue/Table, dispatched by Host header); startup fails if it's unavailable (no fallback pool)."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the default Settings. Used when no ConfigProvider
// is available at init time (e.g. tests constructing a Provider directly).
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
