package azurearm

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	egListSegsMin   = 3
	egActionTailLen = 5
	egItemTailLen   = 4
	egLocations     = "locations"
	egTopicTypes    = "topicTypes"
	egSkipToken     = "$skiptoken"
)

// egSubPath is a path ending in providers/Microsoft.EventGrid/eventSubscriptions[/{name}[/{action}]].
type egSubPath struct {
	name   string
	action string
	scope  []string
}

func isEventSubscriptionsAt(segs []string, i int) bool {
	return i >= 2 && i+2 < len(segs) &&
		strings.EqualFold(segs[i], providersSegment) &&
		strings.EqualFold(segs[i+1], namespaceMicrosoftEventGrid) &&
		strings.EqualFold(segs[i+2], egSubscriptionsType)
}

func parseEventSubscriptionPath(segs []string) (egSubPath, bool) {
	if len(segs) < egListSegsMin || !isSubscriptionScoped(segs) {
		return egSubPath{}, false
	}

	for _, tail := range []int{egItemTailLen, egActionTailLen} {
		i := len(segs) - tail
		if !isEventSubscriptionsAt(segs, i) {
			continue
		}

		sp := egSubPath{scope: segs[:i], name: segs[i+3]}

		if tail == egActionTailLen {
			sp.action = segs[i+4]
		}

		return sp, true
	}

	return egSubPath{}, false
}

// egOwnedScope reports whether scope is an Event Grid topic, domain or domain topic,
// whose subscriptions the Event Grid provider serves itself.
func egOwnedScope(scope []string) bool {
	for i := 0; i+2 < len(scope); i++ {
		if strings.EqualFold(scope[i], providersSegment) && strings.EqualFold(scope[i+1], namespaceMicrosoftEventGrid) {
			return true
		}
	}

	return false
}

func isScopeSubscriptionRoute(segs []string, r *http.Request) bool {
	sp, ok := parseEventSubscriptionPath(segs)
	if !ok || egOwnedScope(sp.scope) {
		return false
	}

	switch sp.action {
	case "":
		return r.Method == http.MethodPut || r.Method == http.MethodGet || r.Method == http.MethodDelete
	case egActionFullURL, egActionDelivery:
		return r.Method == http.MethodPost
	}

	return false
}

func isEventSubscriptionListRoute(segs []string, r *http.Request) bool {
	_, ok := parseEventSubscriptionList(segs)

	return ok && r.Method == http.MethodGet
}

//nolint:gochecknoglobals // static route table, read-only after init
var eventGridScopeRoutes = []armRoute{
	{match: isScopeSubscriptionRoute, handle: func(h *Handler, c *echo.Context, segs []string) error {
		return h.handleScopeSubscription(c, segs)
	}},
	{match: isEventSubscriptionListRoute, handle: func(h *Handler, c *echo.Context, segs []string) error {
		return h.handleListEventSubscriptions(c, segs)
	}},
}

func (h *Handler) scopeFor(c *echo.Context, scope []string, mustExist bool) (EventSubscriptionScope, error) {
	out := EventSubscriptionScope{ID: "/" + joinSegs(scope), SubscriptionID: scope[1]}

	const rgIdx = 3
	if len(scope) > rgIdx && strings.EqualFold(scope[2], resourceGroupsSegment) {
		out.ResourceGroup = scope[rgIdx]
	}

	if !mustExist {
		return out, nil
	}

	if out.ResourceGroup != "" && !h.Backend.ResourceGroupExists(out.ResourceGroup) {
		return out, ErrResourceGroupNotFound
	}

	if len(scope) > rgPathSegs {
		id, err := ParseGenericResourcePath(out.ID)
		if err != nil {
			return out, err
		}

		if _, err = h.Registry.Get(c.Request().Context(), id); err != nil {
			return out, err
		}
	}

	return out, nil
}

func (h *Handler) handleScopeSubscription(c *echo.Context, segs []string) error {
	p, ok := h.eventGridProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	sp, _ := parseEventSubscriptionPath(segs)
	r := c.Request()

	scope, err := h.scopeFor(c, sp.scope, r.Method == http.MethodPut)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	switch {
	case sp.action == egActionFullURL:
		return h.writeScopeResult(c, http.StatusOK)(p.ScopeSubscriptionFullURL(scope, sp.name))
	case sp.action == egActionDelivery:
		return h.writeScopeResult(c, http.StatusOK)(p.ScopeDeliveryAttributes(scope, sp.name))
	case r.Method == http.MethodGet:
		return h.writeScopeResult(c, http.StatusOK)(p.ScopeSubscription(scope, sp.name))
	case r.Method == http.MethodDelete:
		if err = p.DeleteScopeSubscription(scope, sp.name); err != nil {
			return h.writeAPIError(c, err)
		}

		return h.writeAccepted(c, ResourceID{SubscriptionID: scope.SubscriptionID},
			namespaceMicrosoftEventGrid, egOperationsStatus)
	}

	body, err := decodeJSONBody(r)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeScopeResult(c, http.StatusCreated)(p.PutScopeSubscription(r.Context(), scope, sp.name, body))
}

func (h *Handler) writeScopeResult(c *echo.Context, status int) func(map[string]any, error) error {
	return func(out map[string]any, err error) error {
		if err != nil {
			return h.writeAPIError(c, err)
		}

		return h.writeJSON(c, status, out)
	}
}

// parseEventSubscriptionList recognises the event subscription list paths of armeventgrid's
// EventSubscriptionsClient: global and regional, by subscription or resource group,
// optionally by topic type, and by resource (topics, domains, domain topics and others).
func parseEventSubscriptionList(segs []string) (EventSubscriptionQuery, bool) {
	const byResourceMin = 7

	if len(segs) < egListSegsMin || !isSubscriptionScoped(segs) ||
		!strings.EqualFold(segs[len(segs)-1], egSubscriptionsType) {
		return EventSubscriptionQuery{}, false
	}

	q := EventSubscriptionQuery{SubscriptionID: segs[1]}
	rest := segs[2:]

	if len(rest) >= 2 && strings.EqualFold(rest[0], resourceGroupsSegment) {
		q.ResourceGroup, rest = rest[1], rest[2:]
	}

	if q.ResourceGroup != "" && len(rest) >= byResourceMin && isEventSubscriptionsAt(rest, len(rest)-egListSegsMin) {
		q.OwnerID = "/" + joinSegs(segs[:len(segs)-egListSegsMin])

		return q, true
	}

	if len(rest) < egListSegsMin || !strings.EqualFold(rest[0], providersSegment) ||
		!strings.EqualFold(rest[1], namespaceMicrosoftEventGrid) {
		return EventSubscriptionQuery{}, false
	}

	return q, applyListQualifiers(&q, rest[2:len(rest)-1])
}

// applyListQualifiers reads the optional locations/{location} and topicTypes/{type} pairs.
func applyListQualifiers(q *EventSubscriptionQuery, mid []string) bool {
	for len(mid) >= 2 {
		switch {
		case strings.EqualFold(mid[0], egLocations) && q.Location == "" && q.TopicType == "":
			q.Location, q.Regional = mid[1], true
		case strings.EqualFold(mid[0], egTopicTypes) && q.TopicType == "":
			q.TopicType = mid[1]
		default:
			return false
		}

		mid = mid[2:]
	}

	return len(mid) == 0
}

func (h *Handler) handleListEventSubscriptions(c *echo.Context, segs []string) error {
	p, ok := h.eventGridProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	q, _ := parseEventSubscriptionList(segs)
	r := c.Request()
	query := r.URL.Query()
	q.Filter, q.SkipToken = query.Get("$filter"), query.Get(egSkipToken)

	if raw := query.Get("$top"); raw != "" {
		top, err := strconv.Atoi(raw)
		if err != nil {
			return h.writeAPIError(c, badEventGridRequest(err))
		}

		q.Top = top
	}

	values, next, err := p.ListEventSubscriptions(q)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out := map[string]any{fieldValue: values}
	if next != "" {
		query.Set(egSkipToken, next)
		out["nextLink"] = h.baseURLFor(r) + (&url.URL{Path: r.URL.Path, RawQuery: query.Encode()}).String()
	}

	return h.writeJSON(c, http.StatusOK, out)
}
