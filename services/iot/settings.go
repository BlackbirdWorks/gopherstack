package iot

// DefaultMQTTPort is the standard unencrypted MQTT port.
const DefaultMQTTPort = 1883

// Settings holds service-level configuration for the IoT backend. Kong picks the fields up when
// the struct is embedded in the root CLI command.
type Settings struct {
	MQTTPort int `json:"mqttPort" env:"IOT_MQTT_PORT" default:"1883" name:"mqtt-port" help:"TCP port for the embedded IoT MQTT broker; 0 picks a free port."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{MQTTPort: DefaultMQTTPort}
}

// ConfigProvider extracts IoT settings from the abstract AppContext Config.
type ConfigProvider interface {
	GetIoTSettings() Settings
}
