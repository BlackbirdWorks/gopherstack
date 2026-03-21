package backup

import "time"

// Settings holds service-level configuration for the Backup backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `env:"BACKUP_JANITOR_INTERVAL" default:"1m" help:"Janitor tick interval."`
}
