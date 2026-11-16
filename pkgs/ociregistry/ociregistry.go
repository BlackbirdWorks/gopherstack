// Package ociregistry embeds a Docker Registry v2 / OCI distribution server, shared by the ECR, Azure
// Container Registry and Artifact Registry emulators.
package ociregistry

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/distribution/distribution/v3/configuration"
	"github.com/distribution/distribution/v3/registry/handlers"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"

	// Register the storage drivers New selects between.
	_ "github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
	_ "github.com/distribution/distribution/v3/registry/storage/driver/inmemory"
)

// maxCatalogEntries is the library default; an unset (zero) value rejects every ?n= on /v2/_catalog.
const maxCatalogEntries = 1000

// Options configures a registry.
type Options struct {
	// Driver, when set, is the storage the registry uses instead of one built from RootDir, so the owner can
	// inspect and prune it.
	Driver storagedriver.StorageDriver
	// InstanceID labels the registry in the distribution library's logs.
	InstanceID string
	// RootDir keeps blobs and manifests on disk under this directory; empty keeps them in memory.
	RootDir string
}

const injectedDriver = "gopherstack-injected"

var errNoInjectedDriver = errors.New("ociregistry: no storage driver supplied")

type injectedFactory struct{}

func (injectedFactory) Create(_ context.Context, params map[string]any) (storagedriver.StorageDriver, error) {
	if d, ok := params["driver"].(storagedriver.StorageDriver); ok && d != nil {
		return d, nil
	}

	return nil, errNoInjectedDriver
}

var registerInjected sync.Once //nolint:gochecknoglobals // the driver factory registry is process-wide

// New builds a registry [http.Handler] with deletes enabled and no authentication. The result is a
// *handlers.App, whose Shutdown() error releases the library's resources.
func New(parent context.Context, opts Options) http.Handler {
	cfg := &configuration.Configuration{
		Version: "0.1",
		Storage: storageConfig(opts),
		HTTP: configuration.HTTP{
			Headers: map[string][]string{"X-Content-Type-Options": {"nosniff"}},
		},
	}
	cfg.Log.Level = configuration.Loglevel("error")
	cfg.Catalog.MaxEntries = maxCatalogEntries

	id := opts.InstanceID
	if id == "" {
		id = "gopherstack-registry"
	}

	// The library reads its instance id from a plain string context key.
	return handlers.NewApp(context.WithValue(parent, instanceIDKey(), id), cfg)
}

func instanceIDKey() any { return "instance.id" }

func storageConfig(opts Options) configuration.Storage {
	driver := configuration.Parameters{}
	name := "inmemory"

	switch {
	case opts.Driver != nil:
		registerInjected.Do(func() { factory.Register(injectedDriver, injectedFactory{}) })

		name = injectedDriver
		driver = configuration.Parameters{"driver": opts.Driver}
	case opts.RootDir != "":
		name = "filesystem"
		driver = configuration.Parameters{"rootdirectory": opts.RootDir}
	}

	return configuration.Storage{
		name:     driver,
		"delete": configuration.Parameters{"enabled": true},
		// The upload purger spawns an unstoppable goroutine (distribution v3.1.1 app.go); stale uploads
		// are not worth sweeping here.
		"maintenance": configuration.Parameters{
			"uploadpurging": map[any]any{"enabled": false},
		},
	}
}

// Shutdown releases a registry built by New; other handlers are ignored.
func Shutdown(h http.Handler) {
	if s, ok := h.(interface{ Shutdown() error }); ok {
		_ = s.Shutdown()
	}
}
