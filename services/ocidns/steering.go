package ocidns

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

var steeringTemplates = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	"FAILOVER": true, "LOAD_BALANCE": true, "ROUTE_BY_GEO": true, "ROUTE_BY_ASN": true, "ROUTE_BY_IP": true,
	"CUSTOM": true,
}

var steeringRuleTypes = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	"FILTER": true, "HEALTH": true, "WEIGHTED": true, "PRIORITY": true, "LIMIT": true,
}

const defaultSteeringTTL = 30

// SteeringPolicyWire is the wire form of a steering policy.
type SteeringPolicyWire struct {
	TimeCreated          time.Time                 `json:"timeCreated"`
	FreeformTags         map[string]string         `json:"freeformTags"`
	DefinedTags          map[string]map[string]any `json:"definedTags"`
	HealthCheckMonitorID *string                   `json:"healthCheckMonitorId"`
	CompartmentID        string                    `json:"compartmentId"`
	DisplayName          string                    `json:"displayName"`
	Template             string                    `json:"template"`
	Self                 string                    `json:"self"`
	ID                   string                    `json:"id"`
	LifecycleState       string                    `json:"lifecycleState"`
	Answers              []map[string]any          `json:"answers"`
	Rules                []map[string]any          `json:"rules"`
	TTL                  int                       `json:"ttl"`
}

// SteeringPolicySummaryWire is the list form of a steering policy.
type SteeringPolicySummaryWire struct {
	TimeCreated          time.Time                 `json:"timeCreated"`
	FreeformTags         map[string]string         `json:"freeformTags"`
	DefinedTags          map[string]map[string]any `json:"definedTags"`
	HealthCheckMonitorID *string                   `json:"healthCheckMonitorId"`
	CompartmentID        string                    `json:"compartmentId"`
	DisplayName          string                    `json:"displayName"`
	Template             string                    `json:"template"`
	Self                 string                    `json:"self"`
	ID                   string                    `json:"id"`
	LifecycleState       string                    `json:"lifecycleState"`
	TTL                  int                       `json:"ttl"`
}

// AttachmentWire is the wire form of a steering policy attachment.
type AttachmentWire struct {
	TimeCreated      time.Time `json:"timeCreated"`
	SteeringPolicyID string    `json:"steeringPolicyId"`
	ZoneID           string    `json:"zoneId"`
	DomainName       string    `json:"domainName"`
	DisplayName      string    `json:"displayName"`
	CompartmentID    string    `json:"compartmentId"`
	Self             string    `json:"self"`
	ID               string    `json:"id"`
	LifecycleState   string    `json:"lifecycleState"`
	Rtypes           []string  `json:"rtypes"`
}

func (b *InMemoryBackend) policyWire(p *SteeringPolicy) SteeringPolicyWire {
	return SteeringPolicyWire{
		TimeCreated: p.Created, FreeformTags: p.free(), DefinedTags: p.defined(),
		HealthCheckMonitorID: optStr(p.HealthCheckMonitorID), CompartmentID: p.CompartmentID,
		DisplayName: p.DisplayName, Template: p.Template, Self: b.self("steeringPolicies", p.ID), ID: p.ID,
		LifecycleState: stateActive, Answers: nonNil(p.Answers), Rules: nonNil(p.Rules), TTL: p.TTL,
	}
}

func (b *InMemoryBackend) policySummary(p *SteeringPolicy) SteeringPolicySummaryWire {
	return SteeringPolicySummaryWire{
		TimeCreated: p.Created, FreeformTags: p.free(), DefinedTags: p.defined(),
		HealthCheckMonitorID: optStr(p.HealthCheckMonitorID), CompartmentID: p.CompartmentID,
		DisplayName: p.DisplayName, Template: p.Template, Self: b.self("steeringPolicies", p.ID), ID: p.ID,
		LifecycleState: stateActive, TTL: p.TTL,
	}
}

func (b *InMemoryBackend) attachmentWire(a *Attachment) AttachmentWire {
	return AttachmentWire{
		TimeCreated: a.Created, SteeringPolicyID: a.SteeringPolicyID, ZoneID: a.ZoneID, DomainName: a.Domain,
		DisplayName: a.DisplayName, CompartmentID: a.CompartmentID, Self: b.self("steeringPolicyAttachments", a.ID),
		ID: a.ID, LifecycleState: stateActive, Rtypes: nonNil(a.Rtypes),
	}
}

func (b *InMemoryBackend) policyByID(id string) (*SteeringPolicy, *APIError) {
	p, ok := b.policies[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return p, nil
}

// CreatePolicyIn is the body of CreateSteeringPolicy.
type CreatePolicyIn struct {
	Tags
	RetryToken           string           `json:"-"`
	TTL                  *int             `json:"ttl"`
	CompartmentID        string           `json:"compartmentId"`
	DisplayName          string           `json:"displayName"`
	Template             string           `json:"template"`
	HealthCheckMonitorID string           `json:"healthCheckMonitorId"`
	Answers              []map[string]any `json:"answers"`
	Rules                []map[string]any `json:"rules"`
}

func strField(m map[string]any, k string) string {
	s, _ := m[k].(string)

	return s
}

func checkAnswers(answers []map[string]any) *APIError {
	seen := map[string]bool{}

	for _, a := range answers {
		name, rtype, rdata := strField(a, "name"), strings.ToUpper(strField(a, "rtype")), strField(a, "rdata")
		if name == "" || rtype == "" || rdata == "" {
			return errMissing("each answer needs name, rtype and rdata")
		}

		if seen[name] {
			return errParam("answer names must be unique within a policy")
		}

		seen[name] = true

		if rtype == rtypeSOA || !supportedRtypes[rtype] {
			return errParam("answer rtype " + rtype + " is not supported")
		}

		if _, err := normRdata("steering.invalid", rtype, rdata, defaultSteeringTTL); err != nil {
			return err
		}
	}

	return nil
}

func checkRules(rules []map[string]any) *APIError {
	for _, r := range rules {
		if !steeringRuleTypes[strings.ToUpper(strField(r, "ruleType"))] {
			return errParam("each rule needs a ruleType of FILTER, HEALTH, WEIGHTED, PRIORITY or LIMIT")
		}
	}

	return nil
}

func checkPolicyTTL(ttl *int) (int, *APIError) {
	if ttl == nil {
		return defaultSteeringTTL, nil
	}

	if *ttl < 0 || *ttl > maxTTL {
		return 0, errParam("ttl must be between 0 and 2147483647")
	}

	return *ttl, nil
}

// CreateSteeringPolicy creates a steering policy.
func (b *InMemoryBackend) CreateSteeringPolicy(in CreatePolicyIn) (SteeringPolicyWire, Tag, error) {
	b.mu.Lock("CreateSteeringPolicy")
	defer b.mu.Unlock()

	if id, ok := b.retry.Lookup("CreateSteeringPolicy", in.RetryToken, b.nowUTC()); ok {
		p, live := b.policies[id]
		if !live {
			return SteeringPolicyWire{}, Tag{}, errInvalidatedToken()
		}

		return b.policyWire(p), tagOf(p.ID, p.meta), nil
	}

	if err := firstAPIErr(b.checkCompartment(in.CompartmentID), b.checkTags(in.Tags)); err != nil {
		return SteeringPolicyWire{}, Tag{}, err
	}

	in.Template = strings.ToUpper(in.Template)

	if in.DisplayName == "" || in.Template == "" {
		return SteeringPolicyWire{}, Tag{}, errMissing("displayName and template are required")
	}

	if !steeringTemplates[in.Template] {
		return SteeringPolicyWire{}, Tag{}, errParam("template is not a supported value")
	}

	ttl, err := checkPolicyTTL(in.TTL)
	if err != nil {
		return SteeringPolicyWire{}, Tag{}, err
	}

	if err = cmp.Or(checkAnswers(in.Answers), checkRules(in.Rules)); err != nil {
		return SteeringPolicyWire{}, Tag{}, err
	}

	now := b.nowUTC()
	p := &SteeringPolicy{
		ID: newOCID("steeringpolicy"), CompartmentID: in.CompartmentID, DisplayName: in.DisplayName,
		Template: in.Template, HealthCheckMonitorID: in.HealthCheckMonitorID, Answers: in.Answers, Rules: in.Rules,
		TTL: ttl, Created: now, Seq: b.nextSeq(), Tags: in.Tags,
	}
	p.touch(now)
	b.policies[p.ID] = p
	b.retry.Remember("CreateSteeringPolicy", in.RetryToken, p.ID, now)

	return b.policyWire(p), tagOf(p.ID, p.meta), nil
}

// GetSteeringPolicy returns a steering policy.
func (b *InMemoryBackend) GetSteeringPolicy(id string) (SteeringPolicyWire, Tag, error) {
	b.mu.RLock("GetSteeringPolicy")
	defer b.mu.RUnlock()

	p, err := b.policyByID(id)
	if err != nil {
		return SteeringPolicyWire{}, Tag{}, err
	}

	return b.policyWire(p), tagOf(p.ID, p.meta), nil
}

// PolicyFilter selects steering policies in a list.
type PolicyFilter struct {
	Created  func(time.Time) bool
	Compart  string
	ID       string
	Name     string
	Contains string
	Monitor  string
	Template string
	State    string
}

func (f PolicyFilter) match(p *SteeringPolicy) bool {
	return p.CompartmentID == f.Compart && (f.ID == "" || p.ID == f.ID) &&
		(f.Name == "" || strs.Equal(p.DisplayName, f.Name)) &&
		(f.Contains == "" || containsFold(p.DisplayName, f.Contains)) &&
		(f.Monitor == "" || p.HealthCheckMonitorID == f.Monitor) &&
		(f.Template == "" || strings.EqualFold(p.Template, f.Template)) &&
		(f.State == "" || strings.EqualFold(f.State, stateActive)) && (f.Created == nil || f.Created(p.Created))
}

var policySorters = sorter[*SteeringPolicy]{ //nolint:gochecknoglobals // immutable comparator table
	keyDisplayName: byString(
		func(p *SteeringPolicy) string { return p.DisplayName },
		func(p *SteeringPolicy) int64 { return p.Seq },
	),
	"template": byString(
		func(p *SteeringPolicy) string { return p.Template },
		func(p *SteeringPolicy) int64 { return p.Seq },
	),
	keyTimeCreated: byCreated(
		func(p *SteeringPolicy) time.Time { return p.Created },
		func(p *SteeringPolicy) int64 { return p.Seq },
	),
}

// ListSteeringPolicies lists the steering policies of a compartment.
func (b *InMemoryBackend) ListSteeringPolicies(
	f PolicyFilter,
	o ListOpts,
) (ListResult[SteeringPolicySummaryWire], error) {
	b.mu.RLock("ListSteeringPolicies")
	defer b.mu.RUnlock()

	if err := b.checkCompartment(f.Compart); err != nil {
		return ListResult[SteeringPolicySummaryWire]{}, err
	}

	var all []*SteeringPolicy

	for _, p := range b.policies {
		if f.match(p) {
			all = append(all, p)
		}
	}

	pg, err := sortPage(all, o, keyTimeCreated, true, policySorters)
	if err != nil {
		return ListResult[SteeringPolicySummaryWire]{}, err
	}

	return finish(pg, len(all), b.policySummary), nil
}

// UpdatePolicyIn is the body of UpdateSteeringPolicy.
type UpdatePolicyIn struct {
	Tags
	DisplayName          *string          `json:"displayName"`
	TTL                  *int             `json:"ttl"`
	HealthCheckMonitorID *string          `json:"healthCheckMonitorId"`
	Template             string           `json:"template"`
	Answers              []map[string]any `json:"answers"`
	Rules                []map[string]any `json:"rules"`
}

// UpdateSteeringPolicy changes a steering policy; attachments pick up the new answers' record types.
func (b *InMemoryBackend) UpdateSteeringPolicy(id string, c cond, in UpdatePolicyIn) (SteeringPolicyWire, Tag, error) {
	b.mu.Lock("UpdateSteeringPolicy")
	defer b.mu.Unlock()

	p, err := b.policyByID(id)
	if err != nil {
		return SteeringPolicyWire{}, Tag{}, err
	}

	if err = firstAPIErr(c.check(p.ID, p.meta), b.checkTags(in.Tags)); err != nil {
		return SteeringPolicyWire{}, Tag{}, err
	}

	tpl := strings.ToUpper(in.Template)
	if tpl != "" && !steeringTemplates[tpl] {
		return SteeringPolicyWire{}, Tag{}, errParam("template is not a supported value")
	}

	ttl := p.TTL

	if in.TTL != nil {
		if ttl, err = checkPolicyTTL(in.TTL); err != nil {
			return SteeringPolicyWire{}, Tag{}, err
		}
	}

	if err = cmp.Or(checkAnswers(in.Answers), checkRules(in.Rules)); err != nil {
		return SteeringPolicyWire{}, Tag{}, err
	}

	p.TTL = ttl

	if tpl != "" {
		p.Template = tpl
	}

	if in.DisplayName != nil {
		p.DisplayName = *in.DisplayName
	}

	if in.HealthCheckMonitorID != nil {
		p.HealthCheckMonitorID = *in.HealthCheckMonitorID
	}

	if in.Answers != nil {
		p.Answers = in.Answers
	}

	if in.Rules != nil {
		p.Rules = in.Rules
	}

	p.Tags.merge(in.Tags)
	now := b.nowUTC()
	p.touch(now)

	for _, a := range b.attachments {
		if a.SteeringPolicyID == p.ID {
			a.Rtypes = policyRtypes(p)
		}
	}

	return b.policyWire(p), tagOf(p.ID, p.meta), nil
}

// DeleteSteeringPolicy deletes a policy that no zone attaches.
func (b *InMemoryBackend) DeleteSteeringPolicy(id string, c cond) error {
	b.mu.Lock("DeleteSteeringPolicy")
	defer b.mu.Unlock()

	p, err := b.policyByID(id)
	if err != nil {
		return err
	}

	if err = c.check(p.ID, p.meta); err != nil {
		return err
	}

	for _, a := range b.attachments {
		if a.SteeringPolicyID == id {
			return errConflict("The steering policy is attached to a zone")
		}
	}

	delete(b.policies, id)

	return nil
}

// ChangeSteeringPolicyCompartment moves a steering policy.
func (b *InMemoryBackend) ChangeSteeringPolicyCompartment(id string, c cond, in ChangeCompartmentIn) error {
	b.mu.Lock("ChangeSteeringPolicyCompartment")
	defer b.mu.Unlock()

	p, err := b.policyByID(id)
	if err != nil {
		return err
	}

	if err = c.check(p.ID, p.meta); err != nil {
		return err
	}

	if err = b.checkCompartment(in.CompartmentID); err != nil {
		return err
	}

	p.CompartmentID = in.CompartmentID
	p.touch(b.nowUTC())

	return nil
}

func policyRtypes(p *SteeringPolicy) []string {
	set := map[string]bool{}

	for _, a := range p.Answers {
		set[strings.ToUpper(strField(a, "rtype"))] = true
	}

	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}

	slices.Sort(out)

	return out
}

// CreateAttachmentIn is the body of CreateSteeringPolicyAttachment.
type CreateAttachmentIn struct {
	RetryToken       string `json:"-"`
	SteeringPolicyID string `json:"steeringPolicyId"`
	ZoneID           string `json:"zoneId"`
	DomainName       string `json:"domainName"`
	DisplayName      string `json:"displayName"`
}

func (b *InMemoryBackend) validateAttachment(in CreateAttachmentIn) (*SteeringPolicy, string, *APIError) {
	if in.SteeringPolicyID == "" || in.ZoneID == "" || in.DomainName == "" {
		return nil, "", errMissing("steeringPolicyId, zoneId and domainName are required")
	}

	p, err := b.policyByID(in.SteeringPolicyID)
	if err != nil {
		return nil, "", err
	}

	z, ok := b.zones[in.ZoneID]
	if !ok {
		return nil, "", errNotFound(in.ZoneID)
	}

	if z.Scope != scopeGlobal {
		return nil, "", errParam("the attached zone must be a public (GLOBAL) zone")
	}

	domain, dok := normName(in.DomainName, false)
	if !dok || !inZone(z.Name, domain) {
		return nil, "", errParam("domainName must be a domain within the zone")
	}

	for _, a := range b.attachments {
		if a.ZoneID == z.ID && a.Domain == domain {
			return nil, "", errExists("A steering policy is already attached to " + domain)
		}
	}

	return p, domain, nil
}

// CreateAttachment attaches a steering policy to a domain of a GLOBAL zone.
func (b *InMemoryBackend) CreateAttachment(in CreateAttachmentIn) (AttachmentWire, Tag, error) {
	b.mu.Lock("CreateAttachment")
	defer b.mu.Unlock()

	if id, ok := b.retry.Lookup("CreateSteeringPolicyAttachment", in.RetryToken, b.nowUTC()); ok {
		a, live := b.attachments[id]
		if !live {
			return AttachmentWire{}, Tag{}, errInvalidatedToken()
		}

		return b.attachmentWire(a), tagOf(a.ID, a.meta), nil
	}

	p, domain, err := b.validateAttachment(in)
	if err != nil {
		return AttachmentWire{}, Tag{}, err
	}

	z := b.zones[in.ZoneID]
	now := b.nowUTC()
	a := &Attachment{
		ID: newOCID("steeringpolicyattachment"), CompartmentID: z.CompartmentID, DisplayName: in.DisplayName,
		SteeringPolicyID: p.ID, ZoneID: z.ID, Domain: domain, Rtypes: policyRtypes(p), Created: now,
		Seq: b.nextSeq(),
	}
	a.touch(now)
	b.attachments[a.ID] = a
	b.retry.Remember("CreateSteeringPolicyAttachment", in.RetryToken, a.ID, now)

	return b.attachmentWire(a), tagOf(a.ID, a.meta), nil
}

func (b *InMemoryBackend) attachmentByID(id string) (*Attachment, *APIError) {
	a, ok := b.attachments[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return a, nil
}

// GetAttachment returns a steering policy attachment.
func (b *InMemoryBackend) GetAttachment(id string) (AttachmentWire, Tag, error) {
	b.mu.RLock("GetAttachment")
	defer b.mu.RUnlock()

	a, err := b.attachmentByID(id)
	if err != nil {
		return AttachmentWire{}, Tag{}, err
	}

	return b.attachmentWire(a), tagOf(a.ID, a.meta), nil
}

// AttachmentFilter selects steering policy attachments in a list.
type AttachmentFilter struct {
	Created  func(time.Time) bool
	Compart  string
	ID       string
	Name     string
	PolicyID string
	ZoneID   string
	Domain   string
	Contains string
	State    string
}

func (f AttachmentFilter) match(a *Attachment) bool {
	return a.CompartmentID == f.Compart && f.matchIdentity(a) &&
		(f.Domain == "" || a.Domain == mustNorm(f.Domain)) &&
		(f.Contains == "" || containsFold(a.Domain, f.Contains)) &&
		(f.State == "" || strings.EqualFold(f.State, stateActive)) && (f.Created == nil || f.Created(a.Created))
}

func (f AttachmentFilter) matchIdentity(a *Attachment) bool {
	return (f.ID == "" || a.ID == f.ID) && (f.Name == "" || strs.Equal(a.DisplayName, f.Name)) &&
		(f.PolicyID == "" || a.SteeringPolicyID == f.PolicyID) && (f.ZoneID == "" || a.ZoneID == f.ZoneID)
}

var attachmentSorters = sorter[*Attachment]{ //nolint:gochecknoglobals // immutable comparator table
	keyDisplayName: byString(
		func(a *Attachment) string { return a.DisplayName },
		func(a *Attachment) int64 { return a.Seq },
	),
	"domainName": byString(func(a *Attachment) string { return a.Domain }, func(a *Attachment) int64 { return a.Seq }),
	keyTimeCreated: byCreated(
		func(a *Attachment) time.Time { return a.Created },
		func(a *Attachment) int64 { return a.Seq },
	),
}

// ListAttachments lists the steering policy attachments of a compartment.
func (b *InMemoryBackend) ListAttachments(f AttachmentFilter, o ListOpts) (ListResult[AttachmentWire], error) {
	b.mu.RLock("ListAttachments")
	defer b.mu.RUnlock()

	if err := b.checkCompartment(f.Compart); err != nil {
		return ListResult[AttachmentWire]{}, err
	}

	var all []*Attachment

	for _, a := range b.attachments {
		if f.match(a) {
			all = append(all, a)
		}
	}

	pg, err := sortPage(all, o, keyTimeCreated, true, attachmentSorters)
	if err != nil {
		return ListResult[AttachmentWire]{}, err
	}

	return finish(pg, len(all), b.attachmentWire), nil
}

// UpdateAttachmentIn is the body of UpdateSteeringPolicyAttachment.
type UpdateAttachmentIn struct {
	DisplayName *string `json:"displayName"`
}

// UpdateAttachment changes the display name of an attachment.
func (b *InMemoryBackend) UpdateAttachment(id string, c cond, in UpdateAttachmentIn) (AttachmentWire, Tag, error) {
	b.mu.Lock("UpdateAttachment")
	defer b.mu.Unlock()

	a, err := b.attachmentByID(id)
	if err != nil {
		return AttachmentWire{}, Tag{}, err
	}

	if err = c.check(a.ID, a.meta); err != nil {
		return AttachmentWire{}, Tag{}, err
	}

	if in.DisplayName != nil {
		a.DisplayName = *in.DisplayName
	}

	a.touch(b.nowUTC())

	return b.attachmentWire(a), tagOf(a.ID, a.meta), nil
}

// DeleteAttachment deletes a steering policy attachment.
func (b *InMemoryBackend) DeleteAttachment(id string, c cond) error {
	b.mu.Lock("DeleteAttachment")
	defer b.mu.Unlock()

	a, err := b.attachmentByID(id)
	if err != nil {
		return err
	}

	if err = c.check(a.ID, a.meta); err != nil {
		return err
	}

	delete(b.attachments, id)

	return nil
}
