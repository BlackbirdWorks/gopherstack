package batch

import "time"

// Settings holds service-level configuration for the Batch backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval   time.Duration `json:"janitor_interval"     env:"BATCH_JANITOR_INTERVAL"     default:"1m"  help:"Janitor tick interval."`                                    //nolint:lll // Kong struct tag makes this line long
	InactiveJobDefTTL time.Duration `json:"inactive_job_def_ttl" env:"BATCH_INACTIVE_JOB_DEF_TTL" default:"24h" help:"TTL for inactive job definitions before they are evicted."` //nolint:lll // Kong struct tag makes this line long
	CompletedJobTTL   time.Duration `json:"completed_job_ttl"    env:"BATCH_COMPLETED_JOB_TTL"    default:"24h" help:"TTL for completed or failed jobs before they are evicted."` //nolint:lll // Kong struct tag makes this line long
}
