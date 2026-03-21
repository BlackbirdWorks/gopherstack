package athena

import "time"

// Settings holds service-level configuration for the Athena backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `env:"ATHENA_JANITOR_INTERVAL" default:"1m"  help:"Janitor tick interval."`
	ExecutionTTL    time.Duration `env:"ATHENA_EXECUTION_TTL"    default:"24h" help:"TTL for completed query executions before they are evicted."` //nolint:lll // Kong struct tag makes this line long
}
