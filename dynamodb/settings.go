package dynamodb

import "time"

// Settings holds service-level configuration for the DynamoDB backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	DefaultRegion   string        `env:"DYNAMODB_REGION"           default:"us-east-1" help:"Default region for DynamoDB."`
	JanitorInterval time.Duration `env:"DYNAMODB_JANITOR_INTERVAL" default:"500ms"     help:"Janitor interval."`
}
