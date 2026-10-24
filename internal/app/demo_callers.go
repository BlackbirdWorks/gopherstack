package app

import (
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/blackbirdworks/gopherstack/demo"
)

// newDemoCallers returns in-process callers for the services the demo loader seeds through sdkwire.
func newDemoCallers(handler http.Handler, cfg aws.Config) map[string]demo.Caller {
	keys := []string{"ecr", "appsync", "amplify", "ecs", "eks", "iot", "codedeploy", "codepipeline"}
	out := make(map[string]demo.Caller, len(keys))

	for _, key := range keys {
		if c, err := demo.NewWireCaller(key, handler, cfg.Credentials, cfg.Region); err == nil {
			out[key] = c
		}
	}

	return out
}
