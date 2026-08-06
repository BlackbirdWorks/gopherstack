package ec2

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"

	outpostsbackend "github.com/blackbirdworks/gopherstack/services/outposts"
)

// siblingServices is the subset of *CLI's method set this backend needs to
// reach the Outposts backend, so RunInstances/TerminateInstances can
// consume/release real Outposts capacity when launching onto (or
// terminating off of) an Outpost-hosted subnet -- matching real AWS
// depleting an Outpost's configured capacity as instances launch onto it.
// Matched structurally against *CLI (no import of the top-level package,
// which would cycle) -- same pattern as services/grafana/cross_service.go
// and services/mgn/cross_service.go. Only ec2 imports services/outposts
// (not the reverse): outposts records its own view of "what's running here"
// via ConsumeCapacity/ReleaseCapacity rather than importing services/ec2 to
// read its Instance table, since that would create an import cycle.
type siblingServices interface {
	GetOutpostsHandler() service.Registerable
}

// SetAppConfig records the service.AppContext.Config value Provider.Init
// received, so this backend can resolve the Outposts handler on demand --
// see services/grafana/cross_service.go's SetAppConfig doc comment for why
// this must be lazy rather than resolved at construction time.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.appConfig = cfg
}

func (b *InMemoryBackend) siblings() (siblingServices, bool) {
	s, ok := b.appConfig.(siblingServices)

	return s, ok
}

// outpostsBackend returns the emulator's Outposts backend, if wired.
func (b *InMemoryBackend) outpostsBackend() (*outpostsbackend.InMemoryBackend, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	h, ok := s.GetOutpostsHandler().(*outpostsbackend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.Backend, true
}

// validateOutpostArn rejects an OutpostArn that doesn't resolve to a real
// Outpost, mirroring real AWS CreateSubnet cross-validating against the
// Outposts control plane. A no-op when outpostArn is empty (the field is
// optional) or when the Outposts backend isn't wired -- e.g. unit tests
// constructing InMemoryBackend directly, with no sibling registry.
func (b *InMemoryBackend) validateOutpostArn(outpostArn string) error {
	if outpostArn == "" {
		return nil
	}

	outpostsBk, ok := b.outpostsBackend()
	if !ok {
		return nil
	}

	if _, err := outpostsBk.GetOutpost(outpostArn); err != nil {
		return fmt.Errorf("%w: %s", ErrOutpostArnNotFound, outpostArn)
	}

	return nil
}

// releaseOutpostCapacityIfFirstTermination returns inst's Outpost capacity
// once its instance genuinely terminates for the first time, mirroring real
// AWS returning consumed capacity when the instance that held it
// terminates (see services/outposts/capacity_ledger.go's ReleaseCapacity
// doc comment). Guarded on prev so a repeated TerminateInstances call
// against an already-shutting-down/terminated instance never
// double-credits capacity.
func (b *InMemoryBackend) releaseOutpostCapacityIfFirstTermination(inst *Instance, id string, prev InstanceState) {
	if inst.OutpostArn == "" || prev.Name == StateShuttingDown.Name || prev.Name == StateTerminated.Name {
		return
	}

	outpostsBk, ok := b.outpostsBackend()
	if !ok {
		return
	}

	outpostsBk.ReleaseCapacity(id)
}

// translateOutpostsCapacityErr maps ConsumeCapacity's exported sentinel
// errors onto this package's own EC2-wire-shaped sentinels, so callers
// (RunInstances) never leak services/outposts' internal error type across
// the package boundary.
func translateOutpostsCapacityErr(err error) error {
	switch {
	case errors.Is(err, outpostsbackend.ErrOutpostNotFound):
		return fmt.Errorf("%w: %w", ErrOutpostArnNotFound, err)
	case errors.Is(err, outpostsbackend.ErrInsufficientOutpostCapacity):
		return fmt.Errorf("%w: %w", ErrInsufficientInstanceCapacity, err)
	default:
		return err
	}
}
