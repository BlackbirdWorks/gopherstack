package azuresearch

import "time"

// DefaultPort is the Azure AI Search data-plane HTTPS port.
const DefaultPort = 10015

// DefaultPollInterval is how often the indexer scheduler checks for due schedules.
const DefaultPollInterval = 30 * time.Second

// Settings is the Kong-embedded configuration of the Azure AI Search emulator.
type Settings struct {
	Port int `json:"port"        env:"AZURE_SEARCH_PORT"     name:"port"     help:"Fixed TCP port for the Azure AI Search HTTPS listener; startup fails if it is unavailable."                                                                   default:"10015"` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider exists.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
