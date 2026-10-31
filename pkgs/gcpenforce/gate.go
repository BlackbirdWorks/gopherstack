// Package gcpenforce applies the opt-in project-existence and API-enablement checks to every GCP emulator: one
// gRPC interceptor and one HTTP middleware, fed by an Enforcer that Resource Manager implements.
package gcpenforce

import "context"

// Settings are the opt-in switches, embedded in the root CLI command as CLI.GCPEnforce.
type Settings struct {
	Projects          bool `json:"projects"          env:"GCP_ENFORCE_PROJECTS"           default:"false" name:"projects"           help:"Reject GCP requests whose project does not exist or is DELETE_REQUESTED (needs the gcpresourcemanager service). Default off: any project id is accepted."`                     //nolint:lll // config struct tags are intentionally verbose
	ServiceEnablement bool `json:"serviceEnablement" env:"GCP_ENFORCE_SERVICE_ENABLEMENT" default:"false" name:"service-enablement" help:"Reject GCP requests to a service whose API is not enabled for the project (SERVICE_DISABLED); implies the project check (needs the gcpresourcemanager service). Default off."` //nolint:lll // config struct tags are intentionally verbose
}

// Enabled reports whether any check is on.
func (s Settings) Enabled() bool { return s.Projects || s.ServiceEnablement }

// Enforcer answers the two checks. project is a project id or number, with or without the "projects/" prefix.
type Enforcer interface {
	CheckProject(project, service string) error
	CheckServiceEnabled(project, service string) error
}

// Gate pairs an Enforcer with the switches that are on.
type Gate struct {
	Enforcer Enforcer
	Settings Settings
}

// Check applies the enabled checks for a call to service (an API host such as pubsub.googleapis.com) on project.
func (g *Gate) Check(project, service string) error {
	if g == nil || g.Enforcer == nil || project == "" {
		return nil
	}

	if g.Settings.ServiceEnablement {
		return g.Enforcer.CheckServiceEnabled(project, service)
	}

	if g.Settings.Projects {
		return g.Enforcer.CheckProject(project, service)
	}

	return nil
}

// GateProvider is implemented by the service that owns the Enforcer.
type GateProvider interface {
	EnforcementGate() *Gate
}

type gateKey struct{}

// WithGate returns ctx carrying g; services read it in StartWorker. A nil g returns ctx unchanged.
func WithGate(ctx context.Context, g *Gate) context.Context {
	if g == nil {
		return ctx
	}

	return context.WithValue(ctx, gateKey{}, g)
}

// GateFrom returns the Gate carried by ctx, or nil.
func GateFrom(ctx context.Context) *Gate {
	g, _ := ctx.Value(gateKey{}).(*Gate)

	return g
}

// WithGateFrom installs the Gate of the first candidate that is a GateProvider with enforcement on.
func WithGateFrom[T any](ctx context.Context, candidates []T) context.Context {
	for _, c := range candidates {
		if gp, ok := any(c).(GateProvider); ok {
			if g := gp.EnforcementGate(); g != nil {
				return WithGate(ctx, g)
			}
		}
	}

	return ctx
}
