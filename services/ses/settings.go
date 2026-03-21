package ses

import "time"

// Settings holds service-level configuration for the SES backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `env:"SES_JANITOR_INTERVAL" default:"1m"  help:"Janitor tick interval."`
	EmailTTL        time.Duration `env:"SES_EMAIL_TTL"        default:"24h" help:"TTL for stored sent emails before they are evicted."` //nolint:lll // Kong struct tag makes this line long
}
