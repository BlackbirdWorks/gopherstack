package stepfunctions

import "time"

// Settings holds configurable settings for the Step Functions service.
type Settings struct {
	// ExecutionRetention is how long execution history is kept before being pruned.
	// Defaults to 24 hours for local mock stability.
	ExecutionRetention time.Duration `json:"execution_retention" name:"execution-retention" env:"SFN_EXECUTION_RETENTION" default:"24h" help:"How long to retain execution history."` //nolint:lll // tags
	// JanitorInterval is how often the background worker runs.
	JanitorInterval time.Duration `json:"janitor_interval"    name:"janitor-interval"    env:"SFN_JANITOR_INTERVAL"    default:"1m"  help:"How often the background janitor runs."` //nolint:lll,golines // tags
}

const (
	defaultExecutionRetention = 24 * time.Hour
	defaultJanitorInterval    = 1 * time.Minute
)

// DefaultSettings returns Settings with sensible defaults.
func DefaultSettings() Settings {
	return Settings{
		ExecutionRetention: defaultExecutionRetention,
		JanitorInterval:    defaultJanitorInterval,
	}
}
