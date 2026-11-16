package lambdaweb

import "net/http"

// PreloadScript returns the Node.js runtime shim mounted into every environment.
func PreloadScript() []byte { return preloadScript }

// ValidMultiRegionSignature runs the MultiRegion signature gate on r.
func (b *InMemoryBackend) ValidMultiRegionSignature(r *http.Request) bool {
	return b.validSignature(r, &Endpoint{Type: endpointMultiRegion}, r.Header.Get("Authorization"))
}
