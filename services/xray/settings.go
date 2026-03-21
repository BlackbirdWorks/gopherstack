package xray

import "time"

// Settings holds service-level configuration for the X-Ray backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `env:"XRAY_JANITOR_INTERVAL" default:"1m"  help:"Janitor tick interval for sweeping expired traces."` //nolint:lll // config struct tags are intentionally verbose
	TraceTTL        time.Duration `env:"XRAY_TRACE_TTL"        default:"30m" help:"TTL for X-Ray traces before they are evicted."`       //nolint:lll // config struct tags are intentionally verbose
}
