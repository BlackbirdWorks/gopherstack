package s3

import "time"

const defaultCompressionMinBytes = 1024

// DefaultSettings returns a Settings struct populated with the documented defaults.
// This is used when no ConfigProvider is available at init time.
func DefaultSettings() Settings {
	return Settings{
		DefaultRegion:       "us-east-1",
		JanitorInterval:     defaultJanitorInterval,
		CompressionMinBytes: defaultCompressionMinBytes,
	}
}

// Settings holds service-level configuration for the S3 backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	DefaultRegion       string        `env:"S3_REGION"                default:"us-east-1" help:"Default region for S3."`
	JanitorInterval     time.Duration `env:"S3_JANITOR_INTERVAL"      default:"500ms"     help:"Janitor tick interval."`
	CompressionMinBytes int           `env:"S3_COMPRESSION_MIN_BYTES" default:"1024"      help:"Minimum object size in bytes for gzip compression. Set to 0 to compress all objects regardless of size." name:"compression-min-bytes"` //nolint:lll // config struct tags are intentionally verbose
}
