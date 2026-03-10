package apigatewayv2

import (
	"encoding/json"
)

type apiDataSnapshot struct {
	Stages       map[string]*Stage       `json:"stages"`
	Routes       map[string]*Route       `json:"routes"`
	Integrations map[string]*Integration `json:"integrations"`
	Deployments  map[string]*Deployment  `json:"deployments"`
	Authorizers  map[string]*Authorizer  `json:"authorizers"`
	API          API                     `json:"api"`
}

type backendSnapshot struct {
	APIs map[string]*apiDataSnapshot `json:"apis"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		APIs: make(map[string]*apiDataSnapshot, len(b.apis)),
	}

	for id, d := range b.apis {
		snap.APIs[id] = &apiDataSnapshot{
			API:          d.api,
			Stages:       d.stages,
			Routes:       d.routes,
			Integrations: d.integrations,
			Deployments:  d.deployments,
			Authorizers:  d.authorizers,
		}
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.apis = make(map[string]*apiData, len(snap.APIs))

	for id, d := range snap.APIs {
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

		b.apis[id] = &apiData{
			api:          d.API,
			stages:       d.Stages,
			routes:       d.Routes,
			integrations: d.Integrations,
			deployments:  d.Deployments,
			authorizers:  d.Authorizers,
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
