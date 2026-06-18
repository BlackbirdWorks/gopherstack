package stepfunctions

import "time"

// Settings holds configurable settings for the Step Functions service.
type Settings struct {
	// ExecutionRetention is how long execution history is kept before being pruned.
	// Defaults to 24 hours for local mock stability.
	ExecutionRetention time.Duration `json:"execution_retention" name:"execution-retention" env:"SFN_EXECUTION_RETENTION" default:"24h" help:"How long to retain execution history."` //nolint:lll // tags
	// JanitorInterval is how often the background worker runs.
	JanitorInterval time.Duration `json:"janitor_interval"    name:"janitor-interval"    env:"SFN_JANITOR_INTERVAL"    default:"1m"  help:"How often the background janitor runs."` //nolint:lll,golines // tags
	// TaskTokenTTL is the maximum time a task token may remain in tasksByToken without
	// a worker response before the janitor evicts it. Defaults to 1 hour.
	TaskTokenTTL time.Duration `json:"task_token_ttl" name:"task-token-ttl" env:"SFN_TASK_TOKEN_TTL" default:"1h" help:"Max lifetime of an unreceived task token before eviction."` //nolint:lll // tags
}

const (
	defaultExecutionRetention = 24 * time.Hour
	defaultJanitorInterval    = 1 * time.Minute
	defaultTaskTokenTTL       = 1 * time.Hour
)

// DefaultSettings returns Settings with sensible defaults.
func DefaultSettings() Settings {
	return Settings{
		ExecutionRetention: defaultExecutionRetention,
		JanitorInterval:    defaultJanitorInterval,
		TaskTokenTTL:       defaultTaskTokenTTL,
	}
}
