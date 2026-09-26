package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"

	directoryservicebackend "github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// siblingServices is the subset of *CLI's method set this backend needs to
// reach the Directory Service backend, so RegisterWorkspaceDirectory can
// populate DirectoryName/Alias/DirectoryType/DnsIpAddresses/CustomerUserName
// from the registered AD directory, matching real AWS. Matched structurally
// against *CLI (no import of the top-level package, which would cycle); see
// services/grafana/cross_service.go's SetAppConfig doc comment for why the
// lookup is resolved lazily instead of at construction time.
type siblingServices interface {
	GetDirectoryServiceHandler() service.Registerable
}

// SetAppConfig records the service.AppContext.Config value Provider.Init
// received, so this backend can resolve the Directory Service backend on
// demand -- see services/grafana/cross_service.go's SetAppConfig doc
// comment for why this must happen lazily rather than at Init time.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.appConfig = cfg
}

func (b *InMemoryBackend) directoryServiceBackend() (directoryservicebackend.StorageBackend, bool) {
	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetDirectoryServiceHandler().(*directoryservicebackend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.Backend, true
}

// resolveDirectoryInfo looks up directoryID in the Directory Service
// backend. ok is false when Directory Service isn't wired (e.g. a unit test
// constructing InMemoryBackend directly) or the directory doesn't exist
// there -- callers fall back to whatever the request supplied rather than
// fabricating a value.
func (b *InMemoryBackend) resolveDirectoryInfo(directoryID string) (*directoryservicebackend.Directory, bool) {
	dsBk, ok := b.directoryServiceBackend()
	if !ok {
		return nil, false
	}

	dirs, _, err := dsBk.DescribeDirectories(context.Background(), []string{directoryID}, 1, "")
	if err != nil || len(dirs) != 1 {
		return nil, false
	}

	return dirs[0], true
}
