package s3

import "time"

// Settings holds service-level configuration for the S3 backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	DefaultRegion       string        `env:"S3_REGION"                default:"us-east-1" help:"Default region for S3."`
	JanitorInterval     time.Duration `env:"S3_JANITOR_INTERVAL"      default:"500ms"     help:"Janitor tick interval."`
	CompressionMinBytes int           `env:"S3_COMPRESSION_MIN_BYTES" default:"1024"      help:"Minimum object size in bytes for gzip compression. Set to 0 to compress all objects regardless of size." name:"compression-min-bytes"` //nolint:lll // config struct tags are intentionally verbose
}
