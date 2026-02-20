package s3

import "time"

// Settings holds service-level configuration for the S3 backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	// JanitorInterval is the tick interval for the async-delete janitor.
	JanitorInterval time.Duration `name:"janitor-interval" env:"S3_JANITOR_INTERVAL" default:"500ms" help:"S3 janitor tick interval (e.g. 500ms, 1s)."`
}
