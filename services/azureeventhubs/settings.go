package azureeventhubs

// Default ports. The AMQP pair is offset by one from Service Bus's 5672/5671
// so both emulators can run side by side.
const (
	DefaultPort        = 10005
	DefaultAMQPPort    = 5673
	DefaultAMQPTLSPort = 5674
)

// Settings is the Kong-embedded configuration of the Event Hubs emulator.
type Settings struct {
	Port        int  `json:"port"        env:"AZURE_EVENTHUBS_PORT"          default:"10005" name:"port"          help:"Fixed TCP port for the Event Hubs HTTP (REST/Atom) listener; startup fails if it's unavailable."` //nolint:lll // config struct tags are intentionally verbose
	AMQPPort    int  `json:"amqpPort"    env:"AZURE_EVENTHUBS_AMQP_PORT"     default:"5673"  name:"amqp-port"     help:"Plain-text AMQP 1.0 listener port for Event Hubs SDKs (0 disables)."`                             //nolint:lll // config struct tags are intentionally verbose
	AMQPTLSPort int  `json:"amqpTLSPort" env:"AZURE_EVENTHUBS_AMQP_TLS_PORT" default:"5674"  name:"amqp-tls-port" help:"AMQP 1.0 over TLS listener port, self-signed dev certificate (0 disables)."`                      //nolint:lll // config struct tags are intentionally verbose
	ValidateSAS bool `json:"validateSAS" env:"AZURE_EVENTHUBS_VALIDATE_SAS"  default:"false" name:"validate-sas"  help:"Cryptographically validate Event Hubs SAS tokens against authorization-rule keys (opt-in)."`      //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider exists.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort, AMQPPort: DefaultAMQPPort, AMQPTLSPort: DefaultAMQPTLSPort}
}
