package ociapigateway

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	entityUsagePlan = "usagePlan"
	entitySubscribe = "subscriber"

	wrCreateUsagePlan = "CREATE_USAGE_PLAN"
	wrUpdateUsagePlan = "UPDATE_USAGE_PLAN"
	wrDeleteUsagePlan = "DELETE_USAGE_PLAN"
	wrCreateSubscribe = "CREATE_SUBSCRIBER"
	wrUpdateSubscribe = "UPDATE_SUBSCRIBER"
	wrDeleteSubscribe = "DELETE_SUBSCRIBER"

	unitSecond     = "SECOND"
	resetCalendar  = "CALENDAR"
	breachReject   = "REJECT"
	breachAllow    = "ALLOW"
	unitMinute     = "MINUTE"
	unitHour       = "HOUR"
	unitDay        = "DAY"
	unitWeek       = "WEEK"
	unitMonth      = "MONTH"
	locationHeader = "headers"
	locationQuery  = "query"
)

// RateLimit caps requests per unit of time; the only documented unit is SECOND.
type RateLimit struct {
	Unit  string `json:"unit"`
	Value int    `json:"value"`
}

// Quota caps requests per calendar period.
type Quota struct {
	Unit              string `json:"unit"`
	ResetPolicy       string `json:"resetPolicy"`
	OperationOnBreach string `json:"operationOnBreach"`
	Value             int    `json:"value"`
}

// EntitlementTarget names a deployment an entitlement applies to.
type EntitlementTarget struct {
	DeploymentID string `json:"deploymentId"`
}

// Entitlement is one rate limit and quota that applies to a set of deployments.
type Entitlement struct {
	RateLimit   *RateLimit          `json:"rateLimit,omitempty"`
	Quota       *Quota              `json:"quota,omitempty"`
	Name        string              `json:"name"`
	Description string              `json:"description,omitempty"`
	Targets     []EntitlementTarget `json:"targets,omitempty"`
}

// UsagePlan groups entitlements that subscribers receive.
type UsagePlan struct {
	TimeCreated  time.Time     `json:"timeCreated"`
	TimeUpdated  time.Time     `json:"timeUpdated"`
	Tags         Tags          `json:"tags"`
	TransitionAt *time.Time    `json:"transitionAt,omitempty"`
	ID           string        `json:"id"`
	Compartment  string        `json:"compartmentId"`
	DisplayName  string        `json:"displayName"`
	State        string        `json:"state"`
	Etag         string        `json:"etag"`
	Entitlements []Entitlement `json:"entitlements"`
	Locks        []Lock        `json:"locks,omitempty"`
}

// Client is a named API consumer with its client token.
type Client struct {
	Name  string `json:"name"`
	Token string `json:"token"`
}

// Subscriber maps client tokens to usage plans.
type Subscriber struct {
	TimeCreated  time.Time  `json:"timeCreated"`
	TimeUpdated  time.Time  `json:"timeUpdated"`
	Tags         Tags       `json:"tags"`
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	ID           string     `json:"id"`
	Compartment  string     `json:"compartmentId"`
	DisplayName  string     `json:"displayName"`
	State        string     `json:"state"`
	Etag         string     `json:"etag"`
	Clients      []Client   `json:"clients"`
	UsagePlans   []string   `json:"usagePlans"`
	Locks        []Lock     `json:"locks,omitempty"`
}

func (i *createUsagePlanIn) setRetryToken(t string) { i.RetryToken = t }

type createUsagePlanIn struct {
	Tags
	RetryToken    string        `json:"-"`
	CompartmentID string        `json:"compartmentId"`
	DisplayName   string        `json:"displayName"`
	Entitlements  []Entitlement `json:"entitlements"`
	Locks         []lockIn      `json:"locks"`
}

type updateUsagePlanIn struct {
	Tags
	DisplayName  *string       `json:"displayName"`
	Entitlements []Entitlement `json:"entitlements"`
}

type createSubscriberIn struct {
	Tags
	CompartmentID string   `json:"compartmentId"`
	DisplayName   string   `json:"displayName"`
	RetryToken    string   `json:"-"`
	Clients       []Client `json:"clients"`
	UsagePlans    []string `json:"usagePlans"`
	Locks         []lockIn `json:"locks"`
}

func (i *createSubscriberIn) setRetryToken(t string) { i.RetryToken = t }

type updateSubscriberIn struct {
	Tags
	DisplayName *string  `json:"displayName"`
	Clients     []Client `json:"clients"`
	UsagePlans  []string `json:"usagePlans"`
}

// UsagePlanWire is the wire form of a usage plan; targets are omitted in list summaries.
type UsagePlanWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	ID             string                    `json:"id"`
	CompartmentID  string                    `json:"compartmentId"`
	DisplayName    string                    `json:"displayName"`
	LifecycleState string                    `json:"lifecycleState"`
	Entitlements   []Entitlement             `json:"entitlements"`
	Locks          []Lock                    `json:"locks,omitempty"`
}

type clientSummary struct {
	Name string `json:"name"`
}

// SubscriberWire is the wire form of a subscriber; list summaries carry client names only.
type SubscriberWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	ID             string                    `json:"id"`
	CompartmentID  string                    `json:"compartmentId"`
	DisplayName    string                    `json:"displayName"`
	LifecycleState string                    `json:"lifecycleState"`
	Clients        any                       `json:"clients"`
	UsagePlans     []string                  `json:"usagePlans"`
	Locks          []Lock                    `json:"locks,omitempty"`
}

func usagePlanView(p *UsagePlan) UsagePlanWire {
	return UsagePlanWire{
		TimeCreated: p.TimeCreated, TimeUpdated: p.TimeUpdated, DefinedTags: p.Tags.DefinedTags,
		FreeformTags: p.Tags.FreeformTags, ID: p.ID, CompartmentID: p.Compartment, DisplayName: p.DisplayName,
		LifecycleState: p.State, Entitlements: p.Entitlements, Locks: p.Locks,
	}
}

func usagePlanSummary(p *UsagePlan) UsagePlanWire {
	w := usagePlanView(p)
	w.Entitlements = make([]Entitlement, len(p.Entitlements))

	for i, e := range p.Entitlements {
		e.Targets = nil
		w.Entitlements[i] = e
	}

	return w
}

func subscriberView(s *Subscriber, full bool) SubscriberWire {
	w := SubscriberWire{
		TimeCreated: s.TimeCreated, TimeUpdated: s.TimeUpdated, DefinedTags: s.Tags.DefinedTags,
		FreeformTags: s.Tags.FreeformTags, ID: s.ID, CompartmentID: s.Compartment, DisplayName: s.DisplayName,
		LifecycleState: s.State, UsagePlans: s.UsagePlans, Locks: s.Locks,
	}

	if full {
		w.Clients = s.Clients

		return w
	}

	names := make([]clientSummary, len(s.Clients))
	for i, c := range s.Clients {
		names[i] = clientSummary{Name: c.Name}
	}

	w.Clients = names

	return w
}

func (b *InMemoryBackend) validateEntitlements(es []Entitlement) *APIError {
	if len(es) == 0 {
		return errMissing("at least one entitlement is required")
	}

	seen := map[string]bool{}

	for i := range es {
		e := &es[i]
		if e.Name == "" || seen[e.Name] {
			return errParam("entitlement names are required and must be unique")
		}

		seen[e.Name] = true

		if err := validateLimits(e); err != nil {
			return err
		}

		for _, t := range e.Targets {
			if d, ok := b.deployments[t.DeploymentID]; !ok || d.State == stateDeleted {
				return errNotFound(t.DeploymentID)
			}
		}
	}

	return nil
}

func validateLimits(e *Entitlement) *APIError {
	if r := e.RateLimit; r != nil && (r.Value < 1 || r.Unit != unitSecond) {
		return errParam("rateLimit needs a positive value and unit SECOND")
	}

	if q := e.Quota; q != nil {
		if q.Value < 1 || !slices.Contains([]string{unitMinute, unitHour, unitDay, unitWeek, unitMonth}, q.Unit) {
			return errParam("quota needs a positive value and unit MINUTE, HOUR, DAY, WEEK or MONTH")
		}

		if q.ResetPolicy != resetCalendar ||
			(q.OperationOnBreach != breachReject && q.OperationOnBreach != breachAllow) {
			return errParam("quota needs resetPolicy CALENDAR and operationOnBreach REJECT or ALLOW")
		}
	}

	return nil
}

func (b *InMemoryBackend) validateSubscriber(clients []Client, plans []string, except string) *APIError {
	if len(clients) == 0 || len(plans) == 0 {
		return errMissing("clients and usagePlans are required")
	}

	names := map[string]bool{}

	for _, c := range clients {
		if c.Name == "" || c.Token == "" || names[c.Name] {
			return errParam("clients need unique names and non-empty tokens")
		}

		names[c.Name] = true

		if b.tokenTaken(c.Token, except) {
			return errConflict("a client token is already in use by another subscriber")
		}
	}

	for _, id := range plans {
		if p, ok := b.usagePlans[id]; !ok || p.State == stateDeleted {
			return errNotFound(id)
		}
	}

	return nil
}

func (b *InMemoryBackend) tokenTaken(token, except string) bool {
	for _, s := range b.subscribers {
		if s.ID == except || s.State == stateDeleted {
			continue
		}

		if slices.ContainsFunc(s.Clients, func(c Client) bool { return c.Token == token }) {
			return true
		}
	}

	return false
}

// CreateUsagePlan creates a usage plan.
func (b *InMemoryBackend) CreateUsagePlan(in createUsagePlanIn) (UsagePlanWire, string, string, error) {
	b.mu.Lock("CreateUsagePlan")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("CreateUsagePlan", in.RetryToken, b.nowUTC()); ok {
		id, wr := ociretry.Split(v)
		p, live := b.usagePlans[id]
		if !live || p.State == stateDeleting || p.State == stateDeleted {
			return UsagePlanWire{}, "", "", errInvalidatedToken()
		}

		return usagePlanView(p), wr, p.Etag, nil
	}

	if err := b.checkTags(in.Tags); err != nil {
		return UsagePlanWire{}, "", "", err
	}

	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return UsagePlanWire{}, "", "", err
	}

	if err := b.validateEntitlements(in.Entitlements); err != nil {
		return UsagePlanWire{}, "", "", err
	}

	now := b.nowUTC()

	locks, lerr := newLocks(in.Locks, now)
	if lerr != nil {
		return UsagePlanWire{}, "", "", lerr
	}

	name := in.DisplayName
	if name == "" {
		name = "usageplan" + now.Format("20060102150405")
	}

	p := &UsagePlan{
		ID: newOCID("apigatewayusageplan"), Compartment: in.CompartmentID, DisplayName: name,
		Entitlements: in.Entitlements, Locks: locks, State: stateCreating, TransitionAt: b.transitionAt(),
		TimeCreated: now, TimeUpdated: now, Etag: newEtag(), Tags: in.Tags,
	}
	b.usagePlans[p.ID] = p
	wr := b.newWR(wrCreateUsagePlan, p.Compartment, entityUsagePlan, p.ID, actionCreated, *p.TransitionAt)
	b.retry.Remember("CreateUsagePlan", in.RetryToken, ociretry.Join(p.ID, wr), now)

	return usagePlanView(p), wr, p.Etag, nil
}

func (b *InMemoryBackend) liveUsagePlan(id string) (*UsagePlan, *APIError) {
	p, ok := b.usagePlans[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return p, nil
}

// GetUsagePlan returns one usage plan and its etag.
func (b *InMemoryBackend) GetUsagePlan(id string) (UsagePlanWire, string, error) {
	b.mu.RLock("GetUsagePlan")
	defer b.mu.RUnlock()

	p, err := b.liveUsagePlan(id)
	if err != nil {
		return UsagePlanWire{}, "", err
	}

	return usagePlanView(p), p.Etag, nil
}

// ListUsagePlans lists the usage plans of a compartment.
func (b *InMemoryBackend) ListUsagePlans(
	compartment, name, state string, o ListOpts,
) (page.Page[UsagePlanWire], error) {
	if compartment == "" {
		return page.Page[UsagePlanWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListUsagePlans")
	defer b.mu.RUnlock()

	return listOf(b.usagePlans, o, func(p *UsagePlan) bool {
		return p.Compartment == compartment && stateMatches(p.State, state) &&
			(name == "" || strs.Equal(p.DisplayName, name))
	}, func(p *UsagePlan) string { return p.DisplayName }, func(p *UsagePlan) time.Time { return p.TimeCreated },
		usagePlanSummary), nil
}

func (b *InMemoryBackend) activeUsagePlan(id, ifMatch string, deleting bool) (*UsagePlan, *APIError) {
	p, err := b.liveUsagePlan(id)
	if err != nil {
		return nil, err
	}

	if p.State != stateActive {
		return nil, errState("usage plan " + id + " is in state " + p.State)
	}

	if err = checkEtag(p.Etag, ifMatch); err != nil {
		return nil, err
	}

	if lerr := checkLocked(p.Locks, deleting); lerr != nil {
		return nil, lerr
	}

	return p, nil
}

// UpdateUsagePlan changes the name, tags or entitlements of a usage plan.
func (b *InMemoryBackend) UpdateUsagePlan(id, ifMatch string, in updateUsagePlanIn) (string, error) {
	b.mu.Lock("UpdateUsagePlan")
	defer b.mu.Unlock()

	if err := b.checkTags(in.Tags); err != nil {
		return "", err
	}

	p, err := b.activeUsagePlan(id, ifMatch, false)
	if err != nil {
		return "", err
	}

	if in.Entitlements != nil {
		if verr := b.validateEntitlements(in.Entitlements); verr != nil {
			return "", verr
		}

		p.Entitlements = in.Entitlements
		b.usage.forgetPlan(id)
	}

	if in.DisplayName != nil && *in.DisplayName != "" {
		p.DisplayName = *in.DisplayName
	}

	p.Tags.merge(in.Tags)
	p.State, p.TransitionAt = stateUpdating, b.transitionAt()
	p.TimeUpdated, p.Etag = b.nowUTC(), newEtag()

	return b.newWR(wrUpdateUsagePlan, p.Compartment, entityUsagePlan, id, actionUpdated, *p.TransitionAt), nil
}

// DeleteUsagePlan starts deleting a usage plan no subscriber references.
func (b *InMemoryBackend) DeleteUsagePlan(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteUsagePlan")
	defer b.mu.Unlock()

	p, err := b.activeUsagePlan(id, ifMatch, true)
	if err != nil {
		return "", err
	}

	for _, s := range b.subscribers {
		if s.State != stateDeleted && slices.Contains(s.UsagePlans, id) {
			return "", errConflict("usage plan " + id + " is used by subscriber " + s.ID)
		}
	}

	p.State, p.TransitionAt = stateDeleting, b.transitionAt()
	p.TimeUpdated = b.nowUTC()

	return b.newWR(wrDeleteUsagePlan, p.Compartment, entityUsagePlan, id, actionDeleted, *p.TransitionAt), nil
}

// ChangeUsagePlanCompartment moves a usage plan.
func (b *InMemoryBackend) ChangeUsagePlanCompartment(id, ifMatch string, in changeCompartmentIn) (string, error) {
	b.mu.Lock("ChangeUsagePlanCompartment")
	defer b.mu.Unlock()

	p, err := b.liveUsagePlan(id)
	if err != nil {
		return "", err
	}

	return b.move(moveSpec{
		id: id, op: wrUpdateUsagePlan, entity: entityUsagePlan, locks: p.Locks, compartment: &p.Compartment,
		etag: &p.Etag, updated: &p.TimeUpdated,
	}, ifMatch, in)
}

// CreateSubscriber creates a subscriber.
func (b *InMemoryBackend) CreateSubscriber(in createSubscriberIn) (SubscriberWire, string, string, error) {
	b.mu.Lock("CreateSubscriber")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("CreateSubscriber", in.RetryToken, b.nowUTC()); ok {
		id, wr := ociretry.Split(v)

		s, live := b.subscribers[id]
		if !live || s.State == stateDeleting || s.State == stateDeleted {
			return SubscriberWire{}, "", "", errInvalidatedToken()
		}

		return subscriberView(s, true), wr, s.Etag, nil
	}

	if err := b.checkTags(in.Tags); err != nil {
		return SubscriberWire{}, "", "", err
	}

	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return SubscriberWire{}, "", "", err
	}

	if err := b.validateSubscriber(in.Clients, in.UsagePlans, ""); err != nil {
		return SubscriberWire{}, "", "", err
	}

	now := b.nowUTC()

	locks, lerr := newLocks(in.Locks, now)
	if lerr != nil {
		return SubscriberWire{}, "", "", lerr
	}

	name := in.DisplayName
	if name == "" {
		name = "subscriber" + now.Format("20060102150405")
	}

	s := &Subscriber{
		ID: newOCID("apigatewaysubscriber"), Compartment: in.CompartmentID, DisplayName: name,
		Clients: in.Clients, UsagePlans: in.UsagePlans, Locks: locks, State: stateCreating,
		TransitionAt: b.transitionAt(), TimeCreated: now, TimeUpdated: now, Etag: newEtag(), Tags: in.Tags,
	}
	b.subscribers[s.ID] = s
	wr := b.newWR(wrCreateSubscribe, s.Compartment, entitySubscribe, s.ID, actionCreated, *s.TransitionAt)
	b.retry.Remember("CreateSubscriber", in.RetryToken, ociretry.Join(s.ID, wr), now)

	return subscriberView(s, true), wr, s.Etag, nil
}

func (b *InMemoryBackend) liveSubscriber(id string) (*Subscriber, *APIError) {
	s, ok := b.subscribers[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return s, nil
}

// GetSubscriber returns one subscriber and its etag.
func (b *InMemoryBackend) GetSubscriber(id string) (SubscriberWire, string, error) {
	b.mu.RLock("GetSubscriber")
	defer b.mu.RUnlock()

	s, err := b.liveSubscriber(id)
	if err != nil {
		return SubscriberWire{}, "", err
	}

	return subscriberView(s, true), s.Etag, nil
}

// ListSubscribers lists the subscribers of a compartment.
func (b *InMemoryBackend) ListSubscribers(
	compartment, name, state string, o ListOpts,
) (page.Page[SubscriberWire], error) {
	if compartment == "" {
		return page.Page[SubscriberWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListSubscribers")
	defer b.mu.RUnlock()

	return listOf(b.subscribers, o, func(s *Subscriber) bool {
		return s.Compartment == compartment && stateMatches(s.State, state) &&
			(name == "" || strs.Equal(s.DisplayName, name))
	}, func(s *Subscriber) string { return s.DisplayName }, func(s *Subscriber) time.Time { return s.TimeCreated },
		func(s *Subscriber) SubscriberWire { return subscriberView(s, false) }), nil
}

func (b *InMemoryBackend) activeSubscriber(id, ifMatch string, deleting bool) (*Subscriber, *APIError) {
	s, err := b.liveSubscriber(id)
	if err != nil {
		return nil, err
	}

	if s.State != stateActive {
		return nil, errState("subscriber " + id + " is in state " + s.State)
	}

	if err = checkEtag(s.Etag, ifMatch); err != nil {
		return nil, err
	}

	if lerr := checkLocked(s.Locks, deleting); lerr != nil {
		return nil, lerr
	}

	return s, nil
}

// UpdateSubscriber changes the name, tags, clients or usage plans of a subscriber.
func (b *InMemoryBackend) UpdateSubscriber(id, ifMatch string, in updateSubscriberIn) (string, error) {
	b.mu.Lock("UpdateSubscriber")
	defer b.mu.Unlock()

	if err := b.checkTags(in.Tags); err != nil {
		return "", err
	}

	s, err := b.activeSubscriber(id, ifMatch, false)
	if err != nil {
		return "", err
	}

	clients, plans := s.Clients, s.UsagePlans
	if in.Clients != nil {
		clients = in.Clients
	}

	if in.UsagePlans != nil {
		plans = in.UsagePlans
	}

	if verr := b.validateSubscriber(clients, plans, id); verr != nil {
		return "", verr
	}

	s.Clients, s.UsagePlans = clients, plans

	if in.DisplayName != nil && *in.DisplayName != "" {
		s.DisplayName = *in.DisplayName
	}

	s.Tags.merge(in.Tags)
	s.State, s.TransitionAt = stateUpdating, b.transitionAt()
	s.TimeUpdated, s.Etag = b.nowUTC(), newEtag()

	return b.newWR(wrUpdateSubscribe, s.Compartment, entitySubscribe, id, actionUpdated, *s.TransitionAt), nil
}

// DeleteSubscriber starts deleting a subscriber.
func (b *InMemoryBackend) DeleteSubscriber(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteSubscriber")
	defer b.mu.Unlock()

	s, err := b.activeSubscriber(id, ifMatch, true)
	if err != nil {
		return "", err
	}

	s.State, s.TransitionAt = stateDeleting, b.transitionAt()
	s.TimeUpdated = b.nowUTC()

	return b.newWR(wrDeleteSubscribe, s.Compartment, entitySubscribe, id, actionDeleted, *s.TransitionAt), nil
}

// ChangeSubscriberCompartment moves a subscriber.
func (b *InMemoryBackend) ChangeSubscriberCompartment(id, ifMatch string, in changeCompartmentIn) (string, error) {
	b.mu.Lock("ChangeSubscriberCompartment")
	defer b.mu.Unlock()

	s, err := b.liveSubscriber(id)
	if err != nil {
		return "", err
	}

	return b.move(moveSpec{
		id: id, op: wrUpdateSubscribe, entity: entitySubscribe, locks: s.Locks, compartment: &s.Compartment,
		etag: &s.Etag, updated: &s.TimeUpdated,
	}, ifMatch, in)
}

func (h *Handler) createUsagePlan(q *req) error {
	return created(q, "/usagePlans/", func(w UsagePlanWire) string { return w.ID }, h.Backend.CreateUsagePlan)
}

func (h *Handler) getUsagePlan(q *req) error {
	w, etag, err := h.Backend.GetUsagePlan(q.param("usagePlanId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listUsagePlans(q *req) error {
	pg, err := h.Backend.ListUsagePlans(q.query("compartmentId"), q.query("displayName"),
		q.query("lifecycleState"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) updateUsagePlan(q *req) error {
	return withBody(q, func(in updateUsagePlanIn) (string, error) {
		return h.Backend.UpdateUsagePlan(q.param("usagePlanId"), q.ifMatch(), in)
	})
}

func (h *Handler) deleteUsagePlan(q *req) error {
	wr, err := h.Backend.DeleteUsagePlan(q.param("usagePlanId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeUsagePlanCompartment(q *req) error {
	return withBody(q, func(in changeCompartmentIn) (string, error) {
		return h.Backend.ChangeUsagePlanCompartment(q.param("usagePlanId"), q.ifMatch(), in)
	})
}

func (h *Handler) createSubscriber(q *req) error {
	return created(q, "/subscribers/", func(w SubscriberWire) string { return w.ID }, h.Backend.CreateSubscriber)
}

func (h *Handler) getSubscriber(q *req) error {
	w, etag, err := h.Backend.GetSubscriber(q.param("subscriberId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listSubscribers(q *req) error {
	pg, err := h.Backend.ListSubscribers(q.query("compartmentId"), q.query("displayName"),
		q.query("lifecycleState"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) updateSubscriber(q *req) error {
	return withBody(q, func(in updateSubscriberIn) (string, error) {
		return h.Backend.UpdateSubscriber(q.param("subscriberId"), q.ifMatch(), in)
	})
}

func (h *Handler) deleteSubscriber(q *req) error {
	wr, err := h.Backend.DeleteSubscriber(q.param("subscriberId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeSubscriberCompartment(q *req) error {
	return withBody(q, func(in changeCompartmentIn) (string, error) {
		return h.Backend.ChangeSubscriberCompartment(q.param("subscriberId"), q.ifMatch(), in)
	})
}
