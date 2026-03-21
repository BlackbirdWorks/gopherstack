package batch

import "time"

// Settings holds service-level configuration for the Batch backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval   time.Duration `env:"BATCH_JANITOR_INTERVAL"     default:"1m"  help:"Janitor tick interval."`
	InactiveJobDefTTL time.Duration `env:"BATCH_INACTIVE_JOB_DEF_TTL" default:"24h" help:"TTL for inactive job definitions before they are evicted."` //nolint:lll // Kong struct tag makes this line long
}
