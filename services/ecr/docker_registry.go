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
// The returned *handlers.App also satisfies an internal Shutdown() error
// interface the ecr Handler uses to release it (see handler.go).
func newDistributionRegistry(parent context.Context) http.Handler {
	cfg := &configuration.Configuration{
		Version: "0.1",
		Storage: configuration.Storage{
			"inmemory": configuration.Parameters{},
			"delete":   configuration.Parameters{"enabled": true},
			// Upload-purging periodically scans for stale multipart uploads
			// to delete; storage here never outlives the process, so the
			// scan is meaningless and only exists to spawn
			// startUploadPurger's unstoppable goroutine (app.go, distribution
			// v3.1.1) -- disable it instead of leaking it.
			"maintenance": configuration.Parameters{
				"uploadpurging": map[any]any{"enabled": false},
			},
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
	ctx := context.WithValue(
		parent,
		instanceIDKey(),
		"gopherstack-ecr",
	)

	return handlers.NewApp(ctx, cfg)
}

func instanceIDKey() any {
	return "instance.id"
}
