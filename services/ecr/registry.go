package ecr

import (
	"context"
	"net/http"

	"github.com/distribution/distribution/v3/configuration"
	"github.com/distribution/distribution/v3/registry/handlers"

	// Register in-memory storage driver.
	_ "github.com/distribution/distribution/v3/registry/storage/driver/inmemory"
)

// newDistributionRegistry creates an embedded Docker Registry v2 [http.Handler]
// using in-memory storage and no authentication (all requests are accepted).
func newDistributionRegistry(_ context.Context) http.Handler {
	cfg := &configuration.Configuration{
		Version: "0.1",
		Storage: configuration.Storage{
			"inmemory": configuration.Parameters{},
			"delete":   configuration.Parameters{"enabled": true},
		},
		HTTP: configuration.HTTP{
			Headers: map[string][]string{
				"X-Content-Type-Options": {"nosniff"},
			},
		},
	}
	// Disable access logging from the distribution library to keep output clean.
	cfg.Log.Level = configuration.Loglevel("error")

	// The distribution library's logger looks up "instance.id" in the context.
	// We provide it via a string key since that's what the library expects internally.
	//nolint:revive,staticcheck // distribution/distribution requires this string key
	ctx := context.WithValue(
		context.Background(),
		"instance.id",
		"gopherstack-ecr",
	)

	return handlers.NewApp(ctx, cfg)
}
