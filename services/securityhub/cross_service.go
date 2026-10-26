package securityhub

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// siblingServices is matched structurally against *CLI; handlers are wired after every provider initialises.
type siblingServices interface {
	GetS3Handler() service.Registerable
}

// SetAppConfig records the service.AppContext.Config for lazy sibling lookup.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.mu.Lock("SetAppConfig")
	defer b.mu.Unlock()

	b.appConfig = cfg
}

// s3Backend returns the emulator's S3 backend; the caller holds b.mu.
func (b *InMemoryBackend) s3Backend() (s3backend.StorageBackend, bool) {
	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetS3Handler().(*s3backend.S3Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	return h.Backend, true
}
