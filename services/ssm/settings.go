package ssm

import "time"

// Settings holds service-level configuration for the SSM backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `env:"SSM_JANITOR_INTERVAL" default:"30s" help:"Janitor tick interval."`
	CommandTTL      time.Duration `env:"SSM_COMMAND_TTL"      default:"1h"  help:"TTL for SendCommand results before they expire (matches AWS 1h default)."` //nolint:lll // Kong struct tag makes this line long
}
