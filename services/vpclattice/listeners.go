package vpclattice

import (
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// resolveListenerID resolves a listener identifier to (serviceID, listenerID).
func (b *InMemoryBackend) resolveListenerID(serviceID, identifier string) (string, bool) {
	if l, ok := b.listeners.Get(identifier); ok && l.ServiceID == serviceID {
		return identifier, true
	}
	for _, l := range b.listenersByService.Get(serviceID) {
		if l.ARN == identifier {
			return l.ID, true
		}
	}

	return "", false
}

// ------- Listener operations -------

// CreateListener creates a listener on a service.
func (b *InMemoryBackend) CreateListener(
	serviceID, name, protocol string,
	port int32,
	defaultAction *RuleAction,
	tags map[string]string,
) (*Listener, error) {
	if name == "" || protocol == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateListener")
	defer b.mu.Unlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	// check duplicate name within service
	for _, l := range b.listenersByService.Get(svcID) {
		if l.Name == name {
			return nil, ErrAlreadyExists
		}
	}

	if port == 0 {
		if protocol == protocolHTTPS {
			port = 443
		} else {
			port = 80
		}
	}

	now := time.Now().UTC()
	id := newID(idPrefixListener)
	svc, _ := b.services.Get(svcID)
	listenerARN := b.buildListenerARN(svcID, id)

	l := &storedListener{
		ARN:           listenerARN,
		ID:            id,
		ServiceARN:    svc.ARN,
		ServiceID:     svcID,
		Name:          name,
		Protocol:      protocol,
		Port:          port,
		DefaultAction: defaultAction,
		Tags:          copyTags(tags),
		CreatedAt:     now,
		LastUpdatedAt: now,
	}

	b.listeners.Put(l)
	b.tags[listenerARN] = copyTags(tags)

	// create the default rule
	b.createDefaultRule(svcID, id, listenerARN, defaultAction, now)

	return l.toListener(), nil
}

func (b *InMemoryBackend) createDefaultRule(
	serviceID, listenerID, _ string,
	action *RuleAction,
	now time.Time,
) {
	id := newID(idPrefixRule)
	ruleARN := b.buildRuleARN(serviceID, listenerID, id)

	r := &storedRule{
		ARN:           ruleARN,
		ID:            id,
		ServiceID:     serviceID,
		ListenerID:    listenerID,
		Name:          "default",
		Priority:      defaultRulePriority,
		Action:        action,
		IsDefault:     true,
		Tags:          make(map[string]string),
		CreatedAt:     now,
		LastUpdatedAt: now,
	}

	b.rules.Put(r)
}

// GetListener returns a listener.
func (b *InMemoryBackend) GetListener(serviceID, listenerID string) (*Listener, error) {
	b.mu.RLock("GetListener")
	defer b.mu.RUnlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	lID, ok := b.resolveListenerID(svcID, listenerID)
	if !ok {
		return nil, ErrNotFound
	}

	l, _ := b.listeners.Get(lID)

	return l.toListener(), nil
}

// UpdateListener updates the default action of a listener.
func (b *InMemoryBackend) UpdateListener(
	serviceID, listenerID string,
	defaultAction *RuleAction,
) (*Listener, error) {
	b.mu.Lock("UpdateListener")
	defer b.mu.Unlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	lID, ok := b.resolveListenerID(svcID, listenerID)
	if !ok {
		return nil, ErrNotFound
	}

	l, _ := b.listeners.Get(lID)

	if defaultAction != nil {
		l.DefaultAction = defaultAction
	}

	l.LastUpdatedAt = time.Now().UTC()

	return l.toListener(), nil
}

// DeleteListener deletes a listener and its rules.
func (b *InMemoryBackend) DeleteListener(serviceID, listenerID string) error {
	b.mu.Lock("DeleteListener")
	defer b.mu.Unlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return ErrNotFound
	}

	lID, ok := b.resolveListenerID(svcID, listenerID)
	if !ok {
		return ErrNotFound
	}

	l, _ := b.listeners.Get(lID)
	b.deleteListenerCascade(l)

	return nil
}

// deleteListenerCascade removes a listener and all of its rules. It backs
// both DeleteListener and DeleteService, which cascades through every
// listener on the service being deleted (real AWS deletes a service's
// listeners and listener rules automatically -- see DeleteService's doc
// comment).
func (b *InMemoryBackend) deleteListenerCascade(l *storedListener) {
	b.listeners.Delete(l.ID)
	delete(b.tags, l.ARN)

	for _, r := range slices.Clone(b.rulesByListener.Get(l.ID)) {
		b.rules.Delete(r.ID)
		delete(b.tags, r.ARN)
	}
}

// ListListeners lists listeners for a service.
func (b *InMemoryBackend) ListListeners(
	serviceID string,
	maxResults int32,
	nextToken string,
) ([]*ListenerSummary, string, error) {
	b.mu.RLock("ListListeners")
	defer b.mu.RUnlock()

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, "", ErrNotFound
	}

	all := make([]*ListenerSummary, 0)

	for _, l := range b.listenersByService.Get(svcID) {
		all = append(all, l.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}
