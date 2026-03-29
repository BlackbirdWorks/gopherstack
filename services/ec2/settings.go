package ec2

import "time"

// Settings holds service-level configuration for the EC2 backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval  time.Duration `json:"janitor_interval"   env:"EC2_JANITOR_INTERVAL"   default:"1m" help:"Janitor tick interval."`                                   //nolint:lll // Kong struct tag makes this line long
	TerminatedTTL    time.Duration `json:"terminated_ttl"     env:"EC2_TERMINATED_TTL"     default:"1h" help:"TTL for terminated instances before they are evicted."`    //nolint:lll // Kong struct tag makes this line long
	CancelledSpotTTL time.Duration `json:"cancelled_spot_ttl" env:"EC2_CANCELLED_SPOT_TTL" default:"6h" help:"TTL for cancelled spot requests before they are evicted."` //nolint:lll // Kong struct tag makes this line long
}
