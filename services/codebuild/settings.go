package codebuild

import "time"

// Settings holds service-level configuration for the CodeBuild backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `json:"janitor_interval" env:"CODEBUILD_JANITOR_INTERVAL" default:"1m"  help:"Janitor tick interval."`                            //nolint:lll // Kong struct tag makes this line long
	BuildTTL        time.Duration `json:"build_ttl"        env:"CODEBUILD_BUILD_TTL"        default:"24h" help:"TTL for completed builds before they are evicted."` //nolint:lll // Kong struct tag makes this line long
}
