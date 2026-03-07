package apigateway

import (
	"encoding/json"
)

type apiDataSnapshot struct {
	Resources         map[string]*Resource         `json:"resources"`
	Deployments       map[string]*Deployment       `json:"deployments"`
	Stages            map[string]*Stage            `json:"stages"`
	Authorizers       map[string]*Authorizer       `json:"authorizers"`
	RequestValidators map[string]*RequestValidator `json:"requestValidators"`
	API               RestAPI                      `json:"api"`
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
			API:               d.api,
			Resources:         d.resources,
			Deployments:       d.deployments,
			Stages:            d.stages,
			Authorizers:       d.authorizers,
			RequestValidators: d.requestValidators,
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
		if d.Resources == nil {
			d.Resources = make(map[string]*Resource)
		}

		if d.Deployments == nil {
			d.Deployments = make(map[string]*Deployment)
		}

		if d.Stages == nil {
			d.Stages = make(map[string]*Stage)
		}

		if d.Authorizers == nil {
			d.Authorizers = make(map[string]*Authorizer)
		}

		if d.RequestValidators == nil {
			d.RequestValidators = make(map[string]*RequestValidator)
		}

		b.apis[id] = &apiData{
			api:               d.API,
			resources:         d.Resources,
			deployments:       d.Deployments,
			stages:            d.Stages,
			authorizers:       d.Authorizers,
			requestValidators: d.RequestValidators,
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
