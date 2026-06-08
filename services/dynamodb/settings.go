package dynamodb

import "time"

// Settings holds service-level configuration for the DynamoDB backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	DefaultRegion     string        `json:"default_region"       env:"DYNAMODB_REGION"               default:"us-east-1" help:"Default region for DynamoDB."`                          //nolint:lll // Kong struct tag makes this line long
	JanitorInterval   time.Duration `json:"janitor_interval"     env:"DYNAMODB_JANITOR_INTERVAL"     default:"500ms"     help:"Janitor interval."`                                     //nolint:lll // Kong struct tag makes this line long
	TTLSweepBatchSize int           `json:"ttl_sweep_batch_size" env:"DYNAMODB_TTL_SWEEP_BATCH_SIZE" default:"1000"      help:"Maximum items checked per TTL sweep lock acquisition."` //nolint:lll // Kong struct tag makes this line long
	// CreateDelay is the simulated CREATING → ACTIVE transition time.
	// Set to 0 (default) for immediate table activation (no lifecycle transition).
	CreateDelay time.Duration `json:"create_delay"         env:"DYNAMODB_CREATE_DELAY"         default:"0s"        help:"Simulated CREATING→ACTIVE delay. 0 disables lifecycle."          name:"dynamodb-create-delay"` //nolint:lll,golines // Kong struct tag makes this line long
	// EnforceThroughput enables token-bucket throughput throttling per table.
	// When true, operations that exceed the provisioned RCU/WCU return ProvisionedThroughputExceededException.
	EnforceThroughput bool `json:"enforce_throughput"   env:"DYNAMODB_ENFORCE_THROUGHPUT"   default:"false"     help:"Enforce provisioned throughput limits (token bucket per table)." name:"dynamodb-enforce-throughput"` //nolint:lll,golines // Kong struct tag makes this line long
}
