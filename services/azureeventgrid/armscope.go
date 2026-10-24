package azureeventgrid

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Azure Resource Manager event types and schema versions.
const (
	EventResourceWriteSuccess  = "Microsoft.Resources.ResourceWriteSuccess"
	EventResourceDeleteSuccess = "Microsoft.Resources.ResourceDeleteSuccess"
	EventResourceActionSuccess = "Microsoft.Resources.ResourceActionSuccess"

	armEventDataVersion = "2"
)

// armScope owns the event subscriptions of a subscription, resource group or resource.
type armScope struct {
	Subs map[string]*subscription `json:"subs"`
	ID   string                   `json:"id"`
}

func armScopeKey(id string) string { return strings.ToLower(strings.TrimRight(id, "/")) }

func (b *InMemoryBackend) ensureARMScopeLocked(sc Scope) {
	if sc.Resource == "" {
		return
	}

	key := armScopeKey(sc.Resource)
	if _, ok := b.armScopes[key]; !ok {
		b.armScopes[key] = &armScope{ID: sc.Resource, Subs: map[string]*subscription{}}
	}
}

func (b *InMemoryBackend) dropEmptyARMScopeLocked(sc Scope) {
	if sc.Resource == "" {
		return
	}

	key := armScopeKey(sc.Resource)
	if a, ok := b.armScopes[key]; ok && len(a.Subs) == 0 {
		delete(b.armScopes, key)
	}
}

func normalizeARMScopes(snap *backendSnapshot) error {
	if snap.ARMScopes == nil {
		snap.ARMScopes = map[string]*armScope{}
	}

	for key, a := range snap.ARMScopes {
		if a == nil {
			return fmt.Errorf("%w: scope %q", errNullEntry, key)
		}

		if a.Subs == nil {
			a.Subs = map[string]*subscription{}
		}

		for sname, s := range a.Subs {
			if s == nil {
				return fmt.Errorf("%w: subscription %q", errNullEntry, sname)
			}
		}
	}

	return nil
}

// ResourceEvent describes a successful Azure Resource Manager write, delete or action.
type ResourceEvent struct {
	Time            time.Time
	Claims          map[string]any
	EventType       string
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
}

func (e ResourceEvent) fields(id string) map[string]any {
	claims := e.Claims
	if claims == nil {
		claims = map[string]any{}
	}

	return map[string]any{
		"id":           id,
		fieldSubject:   e.ResourceURI,
		fieldEventType: e.EventType,
		fieldEventTime: e.Time.UTC().Format(time.RFC3339Nano),
		"data": map[string]any{
			"authorization": map[string]any{"scope": e.ResourceURI, "action": e.OperationName},
			"claims":        claims,
			"correlationId": e.CorrelationID,
			"httpRequest": map[string]any{
				"clientRequestId": e.ClientRequestID, "clientIpAddress": e.ClientIP,
				"method": e.HTTPMethod, "url": e.HTTPURL,
			},
			"resourceProvider": e.Provider,
			"resourceUri":      e.ResourceURI,
			"operationName":    e.OperationName,
			"status":           "Succeeded",
			"subscriptionId":   e.SubscriptionID,
			"tenantId":         e.TenantID,
		},
		fieldDataVersion: armEventDataVersion,
	}
}

// PublishResourceEvent delivers an Azure Resource Manager event to the event
// subscriptions of its Azure subscription and, when it has one, its resource group.
func (b *InMemoryBackend) PublishResourceEvent(ev ResourceEvent) {
	topics := []string{"/subscriptions/" + ev.SubscriptionID}
	if ev.ResourceGroup != "" {
		topics = append(topics, topics[0]+"/resourceGroups/"+ev.ResourceGroup)
	}

	b.mu.Lock("PublishResourceEvent")
	defer b.mu.Unlock()

	now := b.now().UTC()
	queued := false

	for _, topicID := range topics {
		a, ok := b.armScopes[armScopeKey(topicID)]
		if !ok {
			continue
		}

		id := uuid.NewString()
		e := Event{Fields: ev.fields(id), ID: id}

		if err := e.stamp(a.ID, SchemaEventGrid); err != nil {
			continue
		}

		for _, s := range a.Subs {
			s.enqueue(e, now)
		}

		queued = true
	}

	if queued {
		select {
		case b.kick <- struct{}{}:
		default:
		}
	}
}
