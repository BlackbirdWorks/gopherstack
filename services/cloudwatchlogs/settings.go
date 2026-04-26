package cloudwatchlogs

import "time"

// Settings holds service-level configuration for the CloudWatch Logs backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval  time.Duration `json:"janitor_interval"   env:"CLOUDWATCHLOGS_JANITOR_INTERVAL"   default:"1m" help:"Janitor tick interval."`                                                   //nolint:lll // long struct tags
	MaxRetentionDays int           `json:"max_retention_days" env:"CLOUDWATCHLOGS_MAX_RETENTION_DAYS" default:"14" help:"Global maximum log retention in days for groups without explicit policy."` //nolint:lll // long struct tags
}
