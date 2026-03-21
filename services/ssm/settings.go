package ssm

import "time"

// Settings holds service-level configuration for the SSM backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `env:"SSM_JANITOR_INTERVAL" default:"30s" help:"Janitor tick interval."`
}
