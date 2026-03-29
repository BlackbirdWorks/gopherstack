package sts

import "time"

// Settings holds service-level configuration for the STS backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `json:"janitor_interval" env:"STS_JANITOR_INTERVAL" default:"30s" help:"Janitor tick interval."` //nolint:lll // Kong struct tag makes this line long
}
