package backup

import "time"

// Settings holds service-level configuration for the Backup backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `env:"BACKUP_JANITOR_INTERVAL" default:"1m"  help:"Janitor tick interval."`
	JobTTL          time.Duration `env:"BACKUP_JOB_TTL"          default:"24h" help:"TTL for completed backup jobs before they are evicted."` //nolint:lll // Kong struct tag makes this line long
}
