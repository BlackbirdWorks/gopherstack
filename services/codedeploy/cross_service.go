package codedeploy

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// siblingServices is the subset of *CLI's method set this backend needs: the
// EC2 backend, so deploymentTargets can resolve Ec2TagFilters/Ec2TagSet
// against real services/ec2 instances instead of always matching zero.
// Matched structurally against *CLI (no import of the top-level package,
// which would cycle) -- same pattern as services/mgn's cross_service.go.
type siblingServices interface {
	GetEC2Handler() service.Registerable
}

// SetAppConfig records the service.AppContext.Config value Provider.Init
// received, so this backend can resolve the EC2 handler on demand -- see
// services/grafana/cross_service.go's SetAppConfig doc comment for why this
// must be lazy rather than resolved at construction time.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.appConfig = cfg
}

func (b *InMemoryBackend) siblings() (siblingServices, bool) {
	s, ok := b.appConfig.(siblingServices)

	return s, ok
}

// ec2Backend returns the emulator's EC2 backend, if wired.
func (b *InMemoryBackend) ec2Backend() (ec2backend.Backend, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	h, ok := s.GetEC2Handler().(*ec2backend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.Backend, true
}
