package emr

import "time"

// Settings holds service-level configuration for the EMR backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `env:"EMR_JANITOR_INTERVAL"       default:"1m" help:"Janitor tick interval for sweeping terminated clusters."` //nolint:lll // config struct tags are intentionally verbose
	TerminatedTTL   time.Duration `env:"EMR_TERMINATED_CLUSTER_TTL" default:"1h" help:"TTL for terminated clusters before they are evicted."`    //nolint:lll // config struct tags are intentionally verbose
}
