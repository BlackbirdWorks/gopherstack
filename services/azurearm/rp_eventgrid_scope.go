package azurearm

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	egSubscriptionTypeName = namespaceMicrosoftEventGrid + "/eventSubscriptions"
	egDefaultListTop       = 20
	egMaxListTop           = 100
	subscriptionScopeSegs  = 2
)

// EventSubscriptionScope identifies the owner of an event subscription that is not
// an Event Grid resource: a subscription, a resource group or a resource.
type EventSubscriptionScope struct {
	ID             string
	SubscriptionID string
	ResourceGroup  string
}

func scopeSubID(scopeID, name string) string {
	return scopeID + "/providers/" + egSubscriptionTypeName + "/" + name
}

func (p *EventGridProvider) scopeSubRecord(scope EventSubscriptionScope, name string) (*storedEG, bool) {
	r, ok := p.res[egKey(scope.ResourceGroup, egKindScopeSub, scope.ID, name)]

	return r, ok
}

// PutScopeSubscription creates or updates an event subscription on an ARM scope.
func (p *EventGridProvider) PutScopeSubscription(
	ctx context.Context, scope EventSubscriptionScope, name string, body map[string]any,
) (map[string]any, error) {
	props, _ := body[fieldProperties].(map[string]any)

	p.mu.Lock("PutScopeSubscription")
	defer p.mu.Unlock()

	if _, err := p.dataPlane.PutResourceSubscription(ctx, scope.ID, name, props); err != nil {
		if isEventGridNotFound(err) {
			return nil, err
		}

		return nil, badEventGridRequest(err)
	}

	rec := &storedEG{
		name: name, resourceGroup: scope.ResourceGroup, subscriptionID: scope.SubscriptionID,
		location: DefaultLocation, createdAt: time.Now().UTC(), names: []string{scope.ID, name}, kind: egKindScopeSub,
	}
	if existing, ok := p.scopeSubRecord(scope, name); ok {
		rec.createdAt = existing.createdAt
	}

	p.res[egKey(scope.ResourceGroup, egKindScopeSub, scope.ID, name)] = rec

	return p.scopeSubBody(rec)
}

// ScopeSubscription returns an event subscription of an ARM scope.
func (p *EventGridProvider) ScopeSubscription(scope EventSubscriptionScope, name string) (map[string]any, error) {
	p.mu.RLock("ScopeSubscription")
	defer p.mu.RUnlock()

	rec, ok := p.scopeSubRecord(scope, name)
	if !ok {
		return nil, ErrEventGridSubscriptionNotFound
	}

	return p.scopeSubBody(rec)
}

// DeleteScopeSubscription removes an event subscription of an ARM scope.
func (p *EventGridProvider) DeleteScopeSubscription(scope EventSubscriptionScope, name string) error {
	p.mu.Lock("DeleteScopeSubscription")
	defer p.mu.Unlock()

	return p.deleteRecord(scope.ResourceGroup, egKindScopeSub, []string{scope.ID, name})
}

// ScopeSubscriptionFullURL returns a WebHook subscription's complete endpoint URL.
func (p *EventGridProvider) ScopeSubscriptionFullURL(
	scope EventSubscriptionScope, name string,
) (map[string]any, error) {
	p.mu.RLock("ScopeSubscriptionFullURL")
	defer p.mu.RUnlock()

	if _, ok := p.scopeSubRecord(scope, name); !ok {
		return nil, ErrEventGridSubscriptionNotFound
	}

	url, err := p.dataPlane.ResourceSubscriptionFullURL(scope.ID, name)
	if err != nil {
		return nil, badEventGridRequest(err)
	}

	return map[string]any{"endpointUrl": url}, nil
}

// ScopeDeliveryAttributes returns a subscription's delivery attribute mappings with secret values.
func (p *EventGridProvider) ScopeDeliveryAttributes(
	scope EventSubscriptionScope, name string,
) (map[string]any, error) {
	p.mu.RLock("ScopeDeliveryAttributes")
	defer p.mu.RUnlock()

	if _, ok := p.scopeSubRecord(scope, name); !ok {
		return nil, ErrEventGridSubscriptionNotFound
	}

	list, err := p.dataPlane.ResourceDeliveryAttributes(scope.ID, name)
	if err != nil {
		return nil, badEventGridRequest(err)
	}

	return map[string]any{fieldValue: list}, nil
}

func (p *EventGridProvider) scopeSubBody(rec *storedEG) (map[string]any, error) {
	props, ok := p.dataPlane.GetResourceSubscription(rec.names[0], rec.name)
	if !ok {
		return nil, ErrEventGridSubscriptionNotFound
	}

	return map[string]any{
		"id": scopeSubID(rec.names[0], rec.name), fieldName: rec.name, fieldType: egSubscriptionTypeName,
		fieldProperties: props,
	}, nil
}

// EventSubscriptionQuery selects the event subscriptions a list operation returns.
type EventSubscriptionQuery struct {
	// OwnerID, when set, restricts the list to subscriptions owned by that resource.
	OwnerID        string
	SubscriptionID string
	ResourceGroup  string
	Location       string
	TopicType      string
	Filter         string
	SkipToken      string
	// Regional selects subscriptions of Event Grid topics and domains (by location);
	// otherwise subscriptions of subscriptions, resource groups and other resources.
	Regional bool
	Top      int
}

type egOwner struct {
	id        string
	topicType string
	location  string
	global    bool
}

// ListEventSubscriptions lists event subscriptions per q and returns the page and the next token.
func (p *EventGridProvider) ListEventSubscriptions(q EventSubscriptionQuery) ([]map[string]any, string, error) {
	match, err := parseNameFilter(q.Filter)
	if err != nil {
		return nil, "", badEventGridRequest(err)
	}

	if q.Top == 0 {
		q.Top = egDefaultListTop
	}

	if q.Top < 1 || q.Top > egMaxListTop {
		return nil, "", badEventGridRequest(
			fmt.Errorf("%w: $top must be between 1 and %d", ErrInvalidRequestBody, egMaxListTop))
	}

	p.mu.RLock("ListEventSubscriptions")
	defer p.mu.RUnlock()

	type item struct {
		body map[string]any
		key  string
	}

	var all []item

	for _, rec := range p.res {
		owner, ok := p.ownerOf(rec)
		if !ok || !q.matches(rec, owner) || !match(rec.name) {
			continue
		}

		props, found := p.subscriptionProps(rec)
		if !found {
			continue
		}

		id := scopeSubID(owner.id, rec.name)
		all = append(all, item{key: strs.Fold(id), body: map[string]any{
			"id": id, fieldName: rec.name, fieldType: egSubscriptionTypeName, fieldProperties: props,
		}})
	}

	sort.Slice(all, func(i, j int) bool { return all[i].key < all[j].key })

	pg := page.New(all, q.SkipToken, q.Top, egDefaultListTop)

	values := make([]map[string]any, 0, len(pg.Data))
	for _, it := range pg.Data {
		values = append(values, it.body)
	}

	return values, pg.Next, nil
}

func (q EventSubscriptionQuery) matches(rec *storedEG, o egOwner) bool {
	if q.OwnerID != "" {
		return strings.EqualFold(q.OwnerID, o.id)
	}

	if q.Regional == o.global {
		return false
	}

	if q.SubscriptionID != "" && rec.subscriptionID != "" && !strings.EqualFold(rec.subscriptionID, q.SubscriptionID) {
		return false
	}

	if q.ResourceGroup != "" && !resourceGroupsEqual(rec.resourceGroup, q.ResourceGroup) {
		return false
	}

	if q.Location != "" && !strings.EqualFold(normalizeLocation(o.location), normalizeLocation(q.Location)) {
		return false
	}

	return q.TopicType == "" || strings.EqualFold(q.TopicType, o.topicType)
}

func normalizeLocation(l string) string { return strings.ReplaceAll(strings.ToLower(l), " ", "") }

// ownerOf resolves the resource that owns the subscription record rec.
func (p *EventGridProvider) ownerOf(rec *storedEG) (egOwner, bool) {
	rg, sub := rec.resourceGroup, rec.subscriptionID
	armID := func(types string, names ...string) string {
		var sb strings.Builder

		sb.WriteString("/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/" + namespaceMicrosoftEventGrid)

		for i, n := range names {
			sb.WriteString("/" + strings.Split(types, "/")[i] + "/" + n)
		}

		return sb.String()
	}

	switch rec.kind {
	case egKindTopicSub:
		return egOwner{
			id: armID("topics", rec.names[0]), topicType: "Microsoft.EventGrid.Topics",
			location: p.locationOf(rg, egKindTopic, rec.names[0]),
		}, true
	case egKindDomainSub:
		return egOwner{
			id: armID("domains", rec.names[0]), topicType: "Microsoft.EventGrid.Domains",
			location: p.locationOf(rg, egKindDomain, rec.names[0]),
		}, true
	case egKindDomainTopicSub:
		return egOwner{
			id: armID("domains/topics", rec.names[0], rec.names[1]), topicType: "Microsoft.EventGrid.Domains",
			location: p.locationOf(rg, egKindDomain, rec.names[0]),
		}, true
	case egKindScopeSub:
		return egOwner{id: rec.names[0], topicType: scopeTopicType(rec.names[0]), global: true}, true
	case egKindTopic, egKindNamespace, egKindNsTopic, egKindNsSub, egKindDomain, egKindDomainTopic, egKindUnsupported:
	}

	return egOwner{}, false
}

func (p *EventGridProvider) locationOf(rg string, kind egKind, name string) string {
	if r, ok := p.res[egKey(rg, kind, name)]; ok {
		return r.location
	}

	return DefaultLocation
}

func (p *EventGridProvider) subscriptionProps(rec *storedEG) (map[string]any, bool) {
	switch rec.kind {
	case egKindTopicSub:
		return p.dataPlane.GetSubscription(rec.names[0], rec.name)
	case egKindDomainSub, egKindDomainTopicSub:
		domain, topic := egScopeOf(rec.names, rec.kind)

		return p.dataPlane.GetScopedSubscription(domain, topic, rec.name)
	case egKindScopeSub:
		return p.dataPlane.GetResourceSubscription(rec.names[0], rec.name)
	case egKindTopic, egKindNamespace, egKindNsTopic, egKindNsSub, egKindDomain, egKindDomainTopic, egKindUnsupported:
	}

	return nil, false
}

// scopeTopicType names the Event Grid topic type of an ARM scope.
func scopeTopicType(scopeID string) string {
	segs := splitARMPath(scopeID)

	switch {
	case len(segs) <= subscriptionScopeSegs:
		return "Microsoft.Resources.Subscriptions"
	case len(segs) == rgPathSegs:
		return "Microsoft.Resources.ResourceGroups"
	}

	for i := range slices.Backward(segs) {
		if strings.EqualFold(segs[i], providersSegment) && i+2 < len(segs) {
			t := segs[i+2]

			return segs[i+1] + "." + strings.ToUpper(t[:1]) + t[1:]
		}
	}

	return ""
}
