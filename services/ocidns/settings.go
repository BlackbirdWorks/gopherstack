package ocidns

const (
	// DefaultPort is the dedicated control-plane listener's default TCP port.
	DefaultPort = 8168
	// DefaultDNSPort is the default UDP and TCP port of the authoritative DNS server.
	DefaultDNSPort = 8169
)

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCIDNS.
type Settings struct {
	Port        int  `json:"port"        env:"OCI_DNS_PORT"         default:"8168" name:"port"         help:"Fixed TCP port for the dedicated OCI DNS control-plane listener; startup fails if it is unavailable."`                                                       //nolint:lll // config struct tags are intentionally verbose
	DNSPort     int  `json:"dnsPort"     env:"OCI_DNS_PORT_DNS"     default:"8169" name:"port-dns"     help:"Fixed UDP and TCP port of the authoritative DNS server that answers for GLOBAL zones; 0 disables the data plane; startup fails if the port is unavailable."` //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool `json:"enforceAuth" env:"OCI_DNS_ENFORCE_AUTH"                name:"enforce-auth" help:"Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works."`          //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort, DNSPort: DefaultDNSPort} }
