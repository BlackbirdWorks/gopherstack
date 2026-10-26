package gcpdns

const (
	// DefaultPort is the dedicated Cloud DNS REST listener's default TCP port.
	DefaultPort = 8140
	// DefaultDNSPort is the default UDP and TCP port of the authoritative DNS server.
	DefaultDNSPort = 8141
)

// Settings holds service-level configuration, embedded in the root CLI as CLI.GCPDNS.
type Settings struct {
	Port    int `json:"port"    env:"GCP_DNS_PORT"        default:"8140" name:"port"     help:"Fixed TCP port for the dedicated Cloud DNS REST listener; startup fails if it is unavailable."`                             //nolint:lll // config struct tags are intentionally verbose
	DNSPort int `json:"dnsPort" env:"GCP_DNS_SERVER_PORT" default:"8141" name:"dns-port" help:"Fixed UDP and TCP port of the authoritative DNS server that answers for public zones; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort, DNSPort: DefaultDNSPort}
}
