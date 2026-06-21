package fis

import "time"

// Settings holds service-level configuration for the FIS backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `json:"janitor_interval" env:"FIS_JANITOR_INTERVAL" default:"1m"  help:"Janitor tick."`
	ExperimentTTL   time.Duration `json:"experiment_ttl"   env:"FIS_EXPERIMENT_TTL"   default:"24h" help:"Done-exp TTL."`
}
