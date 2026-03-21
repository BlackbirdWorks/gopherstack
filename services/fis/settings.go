package fis

import "time"

// Settings holds service-level configuration for the FIS backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `env:"FIS_JANITOR_INTERVAL" default:"1m"  help:"Janitor tick interval for sweeping completed experiments."` //nolint:lll // config struct tags are intentionally verbose
	ExperimentTTL   time.Duration `env:"FIS_EXPERIMENT_TTL"   default:"24h" help:"TTL for completed experiments before they are evicted."`    //nolint:lll // config struct tags are intentionally verbose
}
