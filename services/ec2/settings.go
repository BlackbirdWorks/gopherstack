package ec2

import "time"

// Settings holds service-level configuration for the EC2 backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval  time.Duration `env:"EC2_JANITOR_INTERVAL"   default:"1m" help:"Janitor tick interval for sweeping terminated instances and cancelled spot requests."` //nolint:lll // config struct tags are intentionally verbose
	TerminatedTTL    time.Duration `env:"EC2_TERMINATED_TTL"     default:"1h" help:"TTL for terminated instances before they are evicted."`                              //nolint:lll // config struct tags are intentionally verbose
	CancelledSpotTTL time.Duration `env:"EC2_CANCELLED_SPOT_TTL" default:"6h" help:"TTL for cancelled spot instance requests before they are evicted."`                  //nolint:lll // config struct tags are intentionally verbose
}
