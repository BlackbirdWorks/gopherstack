package xray

import "time"

// Settings holds service-level configuration for the X-Ray backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `json:"janitor_interval" env:"XRAY_JANITOR_INTERVAL" default:"1m"  help:"Janitor tick interval."`                         //nolint:lll // Kong struct tag makes this line long
	TraceTTL        time.Duration `json:"trace_ttl"        env:"XRAY_TRACE_TTL"        default:"30m" help:"TTL for stored traces before they are evicted."` //nolint:lll // Kong struct tag makes this line long
}
