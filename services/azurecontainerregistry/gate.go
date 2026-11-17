package azurecontainerregistry

import (
	"bytes"
	"io"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const maxManifestBody = 4 << 20

func newGate() *lockmetrics.RWMutex { return lockmetrics.New("azurecontainerregistry.gate") }

func (b *InMemoryBackend) gateOf(reg string) *lockmetrics.RWMutex {
	b.mu.RLock("gateOf")
	defer b.mu.RUnlock()

	if r, ok := b.regs[strs.Fold(reg)]; ok {
		return r.gate
	}

	return nil
}

// pushGate holds the registry's gate shared, so manifest and tag writes cannot interleave with a delete or sweep.
// Never call it with the backend lock held.
func (b *InMemoryBackend) pushGate(reg string) func() {
	g := b.gateOf(reg)
	if g == nil {
		return func() {}
	}

	g.RLock("pushGate")

	return g.RUnlock
}

// deleteGate holds the registry's gate exclusively for a delete, garbage collection or retention sweep.
func (b *InMemoryBackend) deleteGate(reg string) func() {
	g := b.gateOf(reg)
	if g == nil {
		return func() {}
	}

	g.Lock("deleteGate")

	return g.Unlock
}

// gate takes the gate a request needs: exclusive for DELETE, shared for a manifest PUT whose body is read in
// full first so a slow client cannot hold the gate.
func (h *Handler) gate(w http.ResponseWriter, r *http.Request, reg string) (func(), bool) {
	switch r.Method {
	case http.MethodDelete:
		return h.Backend.deleteGate(reg), true
	case http.MethodPut:
		if _, _, isManifest := manifestRef(r.URL.Path); isManifest {
			body, err := io.ReadAll(io.LimitReader(r.Body, maxManifestBody+1))
			if err != nil || len(body) > maxManifestBody {
				writeRegistryError(
					w, http.StatusBadRequest, "MANIFEST_INVALID", "manifest payload is invalid or too large",
				)

				return nil, false
			}

			r.Body = io.NopCloser(bytes.NewReader(body))

			return h.Backend.pushGate(reg), true
		}
	}

	return func() {}, true
}
