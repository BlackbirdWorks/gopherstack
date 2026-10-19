package codebuild

import "time"

// Settings holds service-level configuration for the CodeBuild backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	Engine          string        `json:"engine"           env:"CODEBUILD_ENGINE"           default:"stub"         help:"CodeBuild engine mode: stub (simulated builds) or docker (run buildspec phases in a container)."` //nolint:lll // Kong struct tag makes this line long
	EngineImage     string        `json:"engine_image"     env:"CODEBUILD_ENGINE_IMAGE"     default:"ubuntu:22.04" help:"Image standing in for aws/codebuild/* curated images in docker engine mode."`                     //nolint:lll // Kong struct tag makes this line long
	JanitorInterval time.Duration `json:"janitor_interval" env:"CODEBUILD_JANITOR_INTERVAL" default:"1m"           help:"Janitor tick interval."`                                                                          //nolint:lll // Kong struct tag makes this line long
	BuildTTL        time.Duration `json:"build_ttl"        env:"CODEBUILD_BUILD_TTL"        default:"24h"          help:"TTL for completed builds before they are evicted."`                                               //nolint:lll // Kong struct tag makes this line long
}
