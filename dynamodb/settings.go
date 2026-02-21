package dynamodb

import "time"

// Settings holds service-level configuration for the DynamoDB backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	// JanitorInterval is the tick interval for the async-delete janitor.
	JanitorInterval time.Duration `env:"DYNAMODB_JANITOR_INTERVAL" default:"500ms" help:"Async-delete janitor interval."`
}
