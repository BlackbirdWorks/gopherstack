package redshiftdata

import "time"

// Settings holds service-level configuration for the Redshift Data backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `json:"janitor_interval" env:"REDSHIFTDATA_JANITOR_INTERVAL" default:"1m"  help:"Janitor tick interval."`                                //nolint:lll // Kong struct tag makes this line long
	StatementTTL    time.Duration `json:"statement_ttl"    env:"REDSHIFTDATA_STATEMENT_TTL"    default:"24h" help:"TTL for completed statements before they are evicted."` //nolint:lll // Kong struct tag makes this line long
}
