package service

import (
	"fmt"
	"log/slog"

	"github.com/labstack/echo/v5"

	"errors"

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
	logger      *slog.Logger
	services    []*Entry
	middlewares []Middleware
}

// NewRegistry creates a new service registry.
func NewRegistry(logger *slog.Logger) *Registry {
	return &Registry{
		services:    make([]*Entry, 0),
		lookup:      make(map[string]*Entry),
		logger:      logger,
		middlewares: make([]Middleware, 0),
	}
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

	// Pre-wrap handler with telemetry first (special case as it needs observer)
	h := telemetry.WrapEchoHandler(
		name,
		handler,
		svc,
		r.logger,
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
