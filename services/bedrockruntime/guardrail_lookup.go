package bedrockruntime

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

var errGuardrailRegistryUnavailable = errors.New("bedrock guardrail registry unavailable")

// GuardrailLookup resolves a stored guardrail version in the control-plane registry.
type GuardrailLookup interface {
	GetGuardrailVersion(idOrARN, version string) (*bedrock.Guardrail, error)
}

type bedrockHandlerProvider interface {
	GetBedrockHandler() service.Registerable
}

// lazyGuardrailLookup resolves the control-plane backend per call: the CLI stores its
// handlers only after every provider's Init has run.
type lazyGuardrailLookup struct {
	provider bedrockHandlerProvider
	region   string
}

func (l lazyGuardrailLookup) GetGuardrailVersion(idOrARN, version string) (*bedrock.Guardrail, error) {
	h, ok := l.provider.GetBedrockHandler().(*bedrock.Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, errGuardrailRegistryUnavailable
	}

	return h.BackendFor(l.region).GetGuardrailVersion(idOrARN, version)
}

// SetGuardrailLookup wires the registry used to verify guardrail references.
func (b *InMemoryBackend) SetGuardrailLookup(l GuardrailLookup) {
	b.mu.Lock("SetGuardrailLookup")
	defer b.mu.Unlock()

	b.guardrailLookup = l
}

// guardrailExists is true when no registry is wired or the guardrail version exists.
func (b *InMemoryBackend) guardrailExists(id, version string) bool {
	b.mu.RLock("guardrailExists")
	lookup := b.guardrailLookup
	b.mu.RUnlock()

	if lookup == nil || id == "" {
		return true
	}

	_, err := lookup.GetGuardrailVersion(id, version)
	if err != nil {
		return errors.Is(err, errGuardrailRegistryUnavailable)
	}

	return true
}

func writeGuardrailNotFound(c *echo.Context, id, version string) error {
	return c.JSON(http.StatusNotFound, errorResponse(
		"ResourceNotFoundException", "Guardrail "+id+" version "+version+" was not found.",
	))
}

func wireGuardrailLookup(ctx *service.AppContext, backend *InMemoryBackend) {
	if ctx == nil {
		return
	}

	if hp, ok := ctx.Config.(bedrockHandlerProvider); ok {
		backend.SetGuardrailLookup(lazyGuardrailLookup{provider: hp, region: backend.region})
	}
}

// inheritGuardrailLookup points a regional sibling at the same registry, scoped to its own region.
func (b *InMemoryBackend) inheritGuardrailLookup(home *InMemoryBackend) {
	home.mu.RLock("inheritGuardrailLookup")
	lookup := home.guardrailLookup
	home.mu.RUnlock()

	if l, ok := lookup.(lazyGuardrailLookup); ok {
		l.region = b.region
		b.guardrailLookup = l
	}
}

// converseGuardrailMissing writes ResourceNotFoundException when the request's guardrailConfig
// references a guardrail that is not registered; the bool reports whether a response was written.
func (h *Handler) converseGuardrailMissing(c *echo.Context, raw json.RawMessage) (bool, error) {
	if len(raw) == 0 {
		return false, nil
	}

	gc, parsed := parseGuardrailConfig(raw)
	if !parsed {
		return false, nil
	}

	if h.Backend.guardrailExists(gc.GuardrailIdentifier, gc.GuardrailVersion) {
		return false, nil
	}

	return true, writeGuardrailNotFound(c, gc.GuardrailIdentifier, gc.GuardrailVersion)
}

// invokeGuardrailMissing is converseGuardrailMissing for the InvokeModel guardrail headers.
func (h *Handler) invokeGuardrailMissing(c *echo.Context) (bool, error) {
	id := c.Request().Header.Get(hdrGuardrailIdentifier)
	version := c.Request().Header.Get(hdrGuardrailVersion)

	if h.Backend.guardrailExists(id, version) {
		return false, nil
	}

	return true, writeGuardrailNotFound(c, id, version)
}

func parseGuardrailConfig(raw json.RawMessage) (guardrailConfigBody, bool) {
	var gc guardrailConfigBody

	return gc, json.Unmarshal(raw, &gc) == nil
}
