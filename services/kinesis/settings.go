package kinesis

import "time"

// Settings holds service-level configuration for the Kinesis backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	// Latency delays every non-streaming API response (0 = none).
	Latency time.Duration `json:"latency" name:"latency" env:"KINESIS_RESPONSE_LATENCY" default:"0s" help:"Artificial delay before each Kinesis API response (LocalStack KINESIS_LATENCY)."` //nolint:lll // Kong struct tag makes this line long
	// ShardLimit caps open shards per region (0 = AWS default 500, negative = unlimited).
	ShardLimit int `json:"shard_limit" name:"shard-limit" env:"KINESIS_ACCOUNT_SHARD_LIMIT" default:"0" help:"Open shard limit (0 = AWS default 500, negative = unlimited)."` //nolint:lll // Kong struct tag makes this line long
	// OnDemandStreamCountLimit caps ON_DEMAND streams per region (0 = AWS default 10, negative = unlimited).
	OnDemandStreamCountLimit int `json:"on_demand_stream_count_limit" name:"on-demand-stream-count-limit" env:"KINESIS_ACCOUNT_ON_DEMAND_STREAM_LIMIT" default:"0" help:"On-demand stream count limit (0 = AWS default 10, negative = unlimited)."` //nolint:lll // Kong struct tag makes this line long
	JanitorInterval time.Duration `json:"janitor_interval" env:"KINESIS_JANITOR_INTERVAL" default:"1m" help:"Janitor tick interval."` //nolint:lll // Kong struct tag makes this line long
}
