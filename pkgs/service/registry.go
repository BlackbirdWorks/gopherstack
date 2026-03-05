package service

import (
	"errors"
	"fmt"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// ErrServiceAlreadyRegistered is returned when a service with the same name is already registered.
var ErrServiceAlreadyRegistered = errors.New("service already registered")

// Entry represents a registered service with its pre-wrapped handler and priority.
type Entry struct {
	Registerable   Registerable
	Matcher        Matcher
	WrappedHandler echo.HandlerFunc
	Priority       int
}

// Registry manages the ordered registration of services and applies
// observability wrapping and other middleware at registration time.
type Registry struct {
	lookup      map[string]*Entry
	services    []*Entry
	middlewares []Middleware
	latencyMs   int
}

// NewRegistry creates a new service registry.
func NewRegistry() *Registry {
	return &Registry{
		services:    make([]*Entry, 0),
		lookup:      make(map[string]*Entry),
		middlewares: make([]Middleware, 0),
	}
}

// SetLatencyMs configures per-request latency injection for all services registered
// after this call. A random sleep of [0, ms) milliseconds is inserted inside the
// telemetry wrapper so that operation duration metrics include the simulated latency.
// A value <= 0 disables latency injection.
func (r *Registry) SetLatencyMs(ms int) {
	r.latencyMs = ms
}

// Use adds a global middleware to the registry. Global middlewares
// are applied to all services registered AFTER the middleware is added.
func (r *Registry) Use(mw Middleware) {
	r.middlewares = append(r.middlewares, mw)
}

// Register adds a service to the registry with optional per-service middleware.
// Returns error if service name already registered.
// Services are internally sorted by priority after registration.
func (r *Registry) Register(svc Registerable, mws ...Middleware) error {
	name := svc.Name()

	// Check for duplicates
	if _, exists := r.lookup[name]; exists {
		return fmt.Errorf("%w: %q", ErrServiceAlreadyRegistered, name)
	}

	matcher := svc.RouteMatcher()
	handler := svc.Handler()
	priority := svc.MatchPriority()

	// Apply latency as the innermost wrapper so that the telemetry timer includes
	// the simulated sleep in its recorded operation duration.
	if r.latencyMs > 0 {
		handler = telemetry.LatencyMiddleware(r.latencyMs)(handler)
	}

	// Pre-wrap handler with telemetry first (special case as it needs observer)
	h := telemetry.WrapEchoHandler(
		name,
		handler,
		svc,
	)

	// Apply global middlewares
	for _, mw := range r.middlewares {
		h = mw(h)
	}

	// Apply per-service middlewares
	for _, mw := range mws {
		h = mw(h)
	}

	entry := &Entry{
		Registerable:   svc,
		Matcher:        matcher,
		WrappedHandler: h,
		Priority:       priority,
	}

	r.services = append(r.services, entry)
	r.lookup[name] = entry

	return nil
}

// GetAll returns all registered services in registration order.
func (r *Registry) GetAll() []*Entry {
	return r.services
}

// GetByName returns a service by its name, or nil if not found.
func (r *Registry) GetByName(name string) *Entry {
	return r.lookup[name]
}

// Count returns the number of registered services.
func (r *Registry) Count() int {
	return len(r.services)
}
