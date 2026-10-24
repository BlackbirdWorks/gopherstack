package azurearm

import (
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// ResourceChangeKind is the kind of a successful ARM operation.
type ResourceChangeKind int

// Kinds of ResourceChange.
const (
	ResourceWritten ResourceChangeKind = iota
	ResourceDeleted
	ResourceActionRun
)

// ResourceChange describes a successful ARM write, delete or action.
type ResourceChange struct {
	Time            time.Time
	Claims          map[string]any
	SubscriptionID  string
	ResourceGroup   string
	ResourceURI     string
	Provider        string
	OperationName   string
	CorrelationID   string
	TenantID        string
	HTTPMethod      string
	HTTPURL         string
	ClientRequestID string
	ClientIP        string
	Kind            ResourceChangeKind
}

// ResourceObserver is told about every successful ARM write, delete and action.
type ResourceObserver interface {
	ResourceChanged(ResourceChange)
}

// SetResourceObserver registers the observer; call it before the handler serves requests.
func (h *Handler) SetResourceObserver(o ResourceObserver) { h.observer = o }

const (
	opSuffixWrite     = "/write"
	opSuffixDelete    = "/delete"
	opSuffixAction    = "/action"
	providerResources = "Microsoft.Resources"
	rgOperation       = "Microsoft.Resources/subscriptions/resourceGroups"
	minActionSegs     = 3
	minResourceSegs   = 2
	rgPathSegs        = 4
)

// notifyResourceChange reports a finished, successful PUT, DELETE or POST to the observer.
func (h *Handler) notifyResourceChange(c *echo.Context, segs []string) {
	if h.observer == nil {
		return
	}

	r := c.Request()

	resp, err := echo.UnwrapResponse(c.Response())
	if err != nil || resp.Status < http.StatusOK || resp.Status >= http.StatusMultipleChoices {
		return
	}

	ch, ok := classifyResourceChange(r.Method, segs)
	if !ok {
		return
	}

	ch.Time = time.Now().UTC()
	ch.TenantID = h.Settings.TenantID
	ch.CorrelationID = r.Header.Get("X-Ms-Correlation-Request-Id")

	if ch.CorrelationID == "" {
		ch.CorrelationID = uuid.NewString()
	}

	ch.ClientRequestID = r.Header.Get("X-Ms-Client-Request-Id")
	ch.ClientIP = c.RealIP()
	ch.HTTPMethod = r.Method
	ch.HTTPURL = h.baseURLFor(r) + r.URL.RequestURI()
	ch.Claims = h.bearerClaims(r)

	h.observer.ResourceChanged(ch)
}

func (h *Handler) bearerClaims(r *http.Request) map[string]any {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" || h.Issuer == nil {
		return nil
	}

	claims, err := h.Issuer.Parse(token)
	if err != nil {
		return nil
	}

	return map[string]any(claims)
}

// classifyResourceChange maps a request to the ARM resource operation it performed.
// Event subscriptions are skipped so a subscription never receives its own creation.
func classifyResourceChange(method string, segs []string) (ResourceChange, bool) {
	if len(segs) < rgPathSegs || !strings.EqualFold(segs[0], subscriptionsSegment) {
		return ResourceChange{}, false
	}

	ch := ResourceChange{SubscriptionID: segs[1]}

	if len(segs) == rgPathSegs && strings.EqualFold(segs[2], resourceGroupsSegment) {
		return classifyResourceGroupChange(method, ch, segs)
	}

	last := -1

	for i, s := range segs {
		if strings.EqualFold(s, providersSegment) && i+1 < len(segs) {
			last = i
		}
	}

	if last < 0 || last+2 >= len(segs) {
		return ResourceChange{}, false
	}

	if strings.EqualFold(segs[2], resourceGroupsSegment) && len(segs) > 3 {
		ch.ResourceGroup = segs[3]
	}

	ch.Provider = segs[last+1]
	rest := segs[last+2:]

	return classifyProviderChange(method, ch, segs, rest)
}

func classifyResourceGroupChange(method string, ch ResourceChange, segs []string) (ResourceChange, bool) {
	ch.Provider = providerResources
	ch.ResourceURI = "/" + joinSegs(segs)

	switch method {
	case http.MethodPut:
		ch.Kind, ch.OperationName = ResourceWritten, rgOperation+opSuffixWrite
	case http.MethodDelete:
		ch.Kind, ch.OperationName = ResourceDeleted, rgOperation+opSuffixDelete
	default:
		return ResourceChange{}, false
	}

	return ch, true
}

func classifyProviderChange(method string, ch ResourceChange, segs, rest []string) (ResourceChange, bool) {
	if len(rest) >= minResourceSegs && strings.EqualFold(rest[0], egSubscriptionsType) &&
		strings.EqualFold(ch.Provider, namespaceMicrosoftEventGrid) {
		return ResourceChange{}, false
	}

	switch {
	case len(rest) >= minResourceSegs && len(rest)%typeNamePairWidth == 0:
		ch.ResourceURI = "/" + joinSegs(segs)

		switch method {
		case http.MethodPut:
			ch.Kind, ch.OperationName = ResourceWritten, operationName(ch.Provider, rest, opSuffixWrite)
		case http.MethodDelete:
			ch.Kind, ch.OperationName = ResourceDeleted, operationName(ch.Provider, rest, opSuffixDelete)
		default:
			return ResourceChange{}, false
		}
	case len(rest) >= minActionSegs && len(rest)%typeNamePairWidth == 1 && method == http.MethodPost:
		ch.Kind = ResourceActionRun
		ch.ResourceURI = "/" + joinSegs(segs[:len(segs)-1])
		ch.OperationName = operationName(ch.Provider, rest[:len(rest)-1], "/"+rest[len(rest)-1]+opSuffixAction)
	default:
		return ResourceChange{}, false
	}

	return ch, true
}

// operationName renders "{provider}/{type}[/{childType}...]{suffix}" from type/name pairs.
func operationName(provider string, pairs []string, suffix string) string {
	parts := []string{provider}

	for i := 0; i < len(pairs); i += typeNamePairWidth {
		parts = append(parts, pairs[i])
	}

	return strings.Join(parts, "/") + suffix
}
