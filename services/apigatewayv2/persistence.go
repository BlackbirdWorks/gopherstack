package apigatewayv2

import (
	"context"
	"encoding/json"
)

// ensureMap returns m if non-nil, otherwise a new empty map of the same type.
func ensureMap[K comparable, V any](m map[K]V) map[K]V {
	if m != nil {
		return m
	}

	return make(map[K]V)
}

type apiDataSnapshot struct {
	Stages               map[string]*Stage                          `json:"stages"`
	Routes               map[string]*Route                          `json:"routes"`
	Integrations         map[string]*Integration                    `json:"integrations"`
	Deployments          map[string]*Deployment                     `json:"deployments"`
	Authorizers          map[string]*Authorizer                     `json:"authorizers"`
	IntegrationResponses map[string]map[string]*IntegrationResponse `json:"integrationResponses"`
	Models               map[string]*Model                          `json:"models"`
	RouteResponses       map[string]map[string]*RouteResponse       `json:"routeResponses"`
	API                  API                                        `json:"api"`
}

type backendSnapshot struct {
	APIs           map[string]*apiDataSnapshot           `json:"apis"`
	DomainNames    map[string]*DomainName                `json:"domainNames"`
	APIMappings    map[string]map[string]*APIMapping     `json:"apiMappings"`
	Portals        map[string]*Portal                    `json:"portals"`
	PortalProducts map[string]*PortalProduct             `json:"portalProducts"`
	ProductPages   map[string][]*ProductPage             `json:"productPages"`
	ProductREPages map[string][]*ProductRestEndpointPage `json:"productREPages"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		APIs:           make(map[string]*apiDataSnapshot, len(b.apis)),
		DomainNames:    b.domainNames,
		APIMappings:    b.apiMappings,
		Portals:        b.portals,
		PortalProducts: b.portalProducts,
		ProductPages:   b.productPages,
		ProductREPages: b.productREPages,
	}

	for id, d := range b.apis {
		snap.APIs[id] = &apiDataSnapshot{
			API:                  d.api,
			Stages:               d.stages,
			Routes:               d.routes,
			Integrations:         d.integrations,
			Deployments:          d.deployments,
			Authorizers:          d.authorizers,
			IntegrationResponses: d.integrationResponses,
			Models:               d.models,
			RouteResponses:       d.routeResponses,
		}
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// restoreAPIData converts a snapshot API entry into a live apiData, initialising any nil maps.
func restoreAPIData(d *apiDataSnapshot) *apiData {
	if d.Stages == nil {
		d.Stages = make(map[string]*Stage)
	}

	if d.Routes == nil {
		d.Routes = make(map[string]*Route)
	}

	if d.Integrations == nil {
		d.Integrations = make(map[string]*Integration)
	}

	if d.Deployments == nil {
		d.Deployments = make(map[string]*Deployment)
	}

	if d.Authorizers == nil {
		d.Authorizers = make(map[string]*Authorizer)
	}

	if d.IntegrationResponses == nil {
		d.IntegrationResponses = make(map[string]map[string]*IntegrationResponse)
	}

	if d.Models == nil {
		d.Models = make(map[string]*Model)
	}

	if d.RouteResponses == nil {
		d.RouteResponses = make(map[string]map[string]*RouteResponse)
	}

	return &apiData{
		api:                  d.API,
		stages:               d.Stages,
		routes:               d.Routes,
		integrations:         d.Integrations,
		deployments:          d.Deployments,
		authorizers:          d.Authorizers,
		integrationResponses: d.IntegrationResponses,
		models:               d.Models,
		routeResponses:       d.RouteResponses,
	}
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.apis = make(map[string]*apiData, len(snap.APIs))

	for id, d := range snap.APIs {
		b.apis[id] = restoreAPIData(d)
	}

	b.domainNames = ensureMap(snap.DomainNames)
	b.apiMappings = ensureMap(snap.APIMappings)
	b.portals = ensureMap(snap.Portals)
	b.portalProducts = ensureMap(snap.PortalProducts)
	b.productPages = ensureMap(snap.ProductPages)
	b.productREPages = ensureMap(snap.ProductREPages)

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
