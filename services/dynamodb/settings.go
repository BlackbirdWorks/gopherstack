package dynamodb

import "time"

// Settings holds service-level configuration for the DynamoDB backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	DefaultRegion   string        `env:"DYNAMODB_REGION"           default:"us-east-1" help:"Default region for DynamoDB."`
	JanitorInterval time.Duration `env:"DYNAMODB_JANITOR_INTERVAL" default:"500ms"     help:"Janitor interval."`
	// CreateDelay is the simulated CREATING → ACTIVE transition time.
	// Set to 0 (default) for immediate table activation (no lifecycle transition).
	CreateDelay time.Duration `name:"dynamodb-create-delay" env:"DYNAMODB_CREATE_DELAY" default:"0s" help:"Simulated CREATING→ACTIVE delay. 0 disables lifecycle."` //nolint:lll // Kong struct tag makes this line long
	// EnforceThroughput enables token-bucket throughput throttling per table.
	// When true, operations that exceed the provisioned RCU/WCU return ProvisionedThroughputExceededException.
	EnforceThroughput bool `name:"dynamodb-enforce-throughput" env:"DYNAMODB_ENFORCE_THROUGHPUT" default:"false" help:"Enforce provisioned throughput limits (token bucket per table)."` //nolint:lll // Kong struct tag makes this line long
}
