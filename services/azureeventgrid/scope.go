package azureeventgrid

import "strings"

// Scope addresses the owner of event subscriptions: a custom topic (Topic only),
// a domain (Domain only), a domain topic (both) or an Azure Resource Manager
// scope such as a subscription, resource group or resource (Resource only).
type Scope struct {
	Domain   string
	Topic    string
	Resource string
}

func (s Scope) leaf() string {
	if s.Resource != "" {
		return s.Resource[strings.LastIndex(s.Resource, "/")+1:]
	}

	if s.Topic != "" {
		return s.Topic
	}

	return s.Domain
}

type scopeInfo struct {
	subs   map[string]*subscription
	schema string
	rid    string
}

// scopeLocked resolves the subscription owner. With forPut a missing domain
// topic resolves (with no subscriptions) when its domain auto-creates topics.
func (b *InMemoryBackend) scopeLocked(sc Scope, forPut bool) (scopeInfo, error) {
	if sc.Resource != "" {
		a, ok := b.armScopes[armScopeKey(sc.Resource)]
		if !ok {
			if forPut {
				return scopeInfo{schema: SchemaEventGrid, rid: sc.Resource}, nil
			}

			return scopeInfo{}, ErrSubscriptionNotFound
		}

		return scopeInfo{subs: a.Subs, schema: SchemaEventGrid, rid: a.ID}, nil
	}

	if sc.Domain == "" {
		t, err := b.topicLocked(sc.Topic)
		if err != nil {
			return scopeInfo{}, err
		}

		return scopeInfo{subs: t.Subs, schema: t.InputSchema, rid: t.ResourceID}, nil
	}

	d, err := b.domainLocked(sc.Domain)
	if err != nil {
		return scopeInfo{}, err
	}

	if sc.Topic == "" {
		return scopeInfo{subs: d.Subs, schema: d.InputSchema, rid: d.ResourceID}, nil
	}

	rid := d.topicRID(sc.Topic)

	t, ok := d.Topics[sc.Topic]
	if !ok {
		if forPut && d.AutoCreate {
			return scopeInfo{schema: d.InputSchema, rid: rid}, nil
		}

		return scopeInfo{}, ErrDomainTopicNotFound
	}

	return scopeInfo{subs: t.Subs, schema: d.InputSchema, rid: rid}, nil
}

// eachSubsLocked visits every subscription owner: custom topics, domains and domain topics.
func (b *InMemoryBackend) eachSubsLocked(fn func(sc Scope, rid string, subs map[string]*subscription)) {
	for _, t := range b.topics {
		fn(Scope{Topic: t.Name}, t.ResourceID, t.Subs)
	}

	for _, d := range b.domains {
		fn(Scope{Domain: d.Name}, d.ResourceID, d.Subs)

		for _, t := range d.Topics {
			fn(Scope{Domain: d.Name, Topic: t.Name}, d.topicRID(t.Name), t.Subs)
		}
	}

	for _, a := range b.armScopes {
		fn(Scope{Resource: a.ID}, a.ID, a.Subs)
	}
}
