package ocicore

import (
	"net/http"
	"net/netip"
	"slices"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	kindDrgNatPolicy  = "drgnatpolicy"
	kindDrgNatRule    = "drgnatrule"
	maxDrgNatPolicies = 8
	maxDrgNatRules    = 16
	opDrgNatRules     = "UPDATE_DRG_NAT_RULES"
	opDeleteDrgNat    = "DELETE_DRG_NAT_POLICY"
)

// DrgNatRule maps an original source or destination to a translated one.
type DrgNatRule struct {
	ID                    string `json:"id"`
	DrgNatPolicyID        string `json:"drgNatPolicyId"`
	OriginalSource        string `json:"originalSource,omitempty"`
	TranslatedSource      string `json:"translatedSource,omitempty"`
	OriginalDestination   string `json:"originalDestination,omitempty"`
	TranslatedDestination string `json:"translatedDestination,omitempty"`
	Priority              int64  `json:"drgNatRulePriority"`
}

// DrgNatPolicy is an ordered set of DRG NAT rules that DRG attachments reference; traffic is not translated.
type DrgNatPolicy struct {
	Base
	Rules []DrgNatRule `json:"rules,omitempty"`
}

func (v *DrgNatPolicy) vcnRef() string { return "" }

func (v *DrgNatPolicy) view() Resource {
	c := *v
	c.Internal, c.Rules = nil, nil

	return &c
}

// CreateDrgNatPolicyIn is the CreateDrgNatPolicy request body.
type CreateDrgNatPolicyIn struct {
	TagsIn
	CompartmentID string `json:"compartmentId"`
	DisplayName   string `json:"displayName"`
}

// CreateDrgNatPolicy creates an empty policy.
func (b *InMemoryBackend) CreateDrgNatPolicy(in CreateDrgNatPolicyIn) (Resource, string, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	if len(liveOf(b, func(*DrgNatPolicy) bool { return true })) >= maxDrgNatPolicies {
		return nil, "", errLimit("A region can have at most 8 DRG NAT policies")
	}

	p := &DrgNatPolicy{Base: b.newBase(kindDrgNatPolicy, in.CompartmentID, in.DisplayName, in.TagsIn)}
	b.moveTo(p, statCreating, byoipActive)
	b.add(p)

	return p.view(), p.Internal.Etag, nil
}

// UpdateDrgNatPolicy changes the name and tags of a policy.
func (b *InMemoryBackend) UpdateDrgNatPolicy(id, ifMatch string, in UpdateIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in, func(*DrgNatPolicy) *APIError { return nil })
}

// DeleteDrgNatPolicy deletes a policy no attachment references.
func (b *InMemoryBackend) DeleteDrgNatPolicy(id, ifMatch string) (string, *APIError) {
	b.mu.Lock("DeleteDrgNatPolicy")
	defer b.mu.Unlock()

	p, aerr := get[*DrgNatPolicy](b, id)
	if aerr != nil {
		return "", aerr
	}

	wr := b.drgNatWorkRequest(opDeleteDrgNat, p)
	if !live(p) || p.LifecycleState == statDeleted {
		return wr, nil
	}

	if aerr = checkEtag(p, ifMatch); aerr != nil {
		return "", aerr
	}

	if a := firstID(liveOf(b, func(x *DrgAttachment) bool { return x.DrgNatPolicyID == id })); a != "" {
		return "", errConflict("The DRG NAT policy " + id + " is used by DRG attachment " + a)
	}

	b.moveTo(p, statDeleting, statDeleted)

	return wr, nil
}

func (b *InMemoryBackend) drgNatWorkRequest(op string, p *DrgNatPolicy) string {
	return b.newWorkRequest(op, p.CompartmentID, []WorkResource{{
		ActionType: wrActionUpdated, EntityType: "drgnatpolicy", Identifier: p.ID,
		EntityURI: apiBase + "/drgNatPolicies/" + p.ID,
	}})
}

// DrgNatRuleIn is one rule of AddDrgNatRules and UpdateDrgNatRules.
type DrgNatRuleIn struct {
	Priority              *int64  `json:"drgNatRulePriority"`
	OriginalSource        *string `json:"originalSource"`
	TranslatedSource      *string `json:"translatedSource"`
	OriginalDestination   *string `json:"originalDestination"`
	TranslatedDestination *string `json:"translatedDestination"`
	ID                    string  `json:"id"`
}

func v4Cidr(s string) bool {
	p, err := netip.ParsePrefix(s)

	return err == nil && p.Addr().Is4() && p.Masked() == p
}

func natPair(orig, trans string) *APIError {
	if (orig == "") != (trans == "") {
		return errParam("an original address block and its translated block must be given together")
	}

	if orig != "" && (!v4Cidr(orig) || !v4Cidr(trans)) {
		return errParam("NAT blocks must be IPv4 CIDR blocks in canonical form")
	}

	return nil
}

func checkNatRule(r DrgNatRule) *APIError {
	if r.OriginalSource == "" && r.OriginalDestination == "" {
		return errParam("a rule needs a source or a destination translation")
	}

	if aerr := natPair(r.OriginalSource, r.TranslatedSource); aerr != nil {
		return aerr
	}

	return natPair(r.OriginalDestination, r.TranslatedDestination)
}

func (b *InMemoryBackend) natPolicyForUpdate(id, ifMatch string) (*DrgNatPolicy, *APIError) {
	p, aerr := getLive[*DrgNatPolicy](b, id)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = checkEtag(p, ifMatch); aerr != nil {
		return nil, aerr
	}

	if p.LifecycleState != byoipActive {
		return nil, errState("The DRG NAT policy " + id + " is " + p.LifecycleState)
	}

	return p, nil
}

func checkNatPriorities(rules []DrgNatRule) *APIError {
	if len(rules) > maxDrgNatRules {
		return errLimit("A DRG NAT policy can have at most 16 rules")
	}

	seen := map[int64]bool{}

	for _, r := range rules {
		if r.Priority < 0 {
			return errParam("drgNatRulePriority must not be negative")
		}

		if seen[r.Priority] {
			return errConflict("Another rule already has priority " + strconv.FormatInt(r.Priority, 10))
		}

		seen[r.Priority] = true
	}

	return nil
}

func (b *InMemoryBackend) finishNatChange(p *DrgNatPolicy, rules []DrgNatRule) (string, *APIError) {
	if aerr := checkNatPriorities(rules); aerr != nil {
		return "", aerr
	}

	slices.SortFunc(rules, func(a, c DrgNatRule) int { return int(a.Priority - c.Priority) })
	p.Rules = rules
	b.moveTo(p, statUpdating, byoipActive)

	return b.drgNatWorkRequest(opDrgNatRules, p), nil
}

func (in DrgNatRuleIn) apply(r *DrgNatRule) {
	for _, f := range []struct {
		from *string
		to   *string
	}{
		{in.OriginalSource, &r.OriginalSource},
		{in.TranslatedSource, &r.TranslatedSource},
		{in.OriginalDestination, &r.OriginalDestination},
		{in.TranslatedDestination, &r.TranslatedDestination},
	} {
		if f.from != nil {
			*f.to = *f.from
		}
	}

	if in.Priority != nil {
		r.Priority = *in.Priority
	}
}

// AddDrgNatRules adds rules to a policy, all or none.
func (b *InMemoryBackend) AddDrgNatRules(id, ifMatch string, rules []DrgNatRuleIn) (string, *APIError) {
	b.mu.Lock("AddDrgNatRules")
	defer b.mu.Unlock()

	p, aerr := b.natPolicyForUpdate(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if len(rules) == 0 {
		return "", errMissing("rules is required")
	}

	next := slices.Clone(p.Rules)

	for _, in := range rules {
		if in.Priority == nil {
			return "", errMissing("drgNatRulePriority is required")
		}

		r := DrgNatRule{ID: newOCID(kindDrgNatRule), DrgNatPolicyID: id}
		in.apply(&r)

		if aerr = checkNatRule(r); aerr != nil {
			return "", aerr
		}

		next = append(next, r)
	}

	return b.finishNatChange(p, next)
}

// UpdateDrgNatRules changes rules by id, all or none.
func (b *InMemoryBackend) UpdateDrgNatRules(id, ifMatch string, rules []DrgNatRuleIn) (string, *APIError) {
	b.mu.Lock("UpdateDrgNatRules")
	defer b.mu.Unlock()

	p, aerr := b.natPolicyForUpdate(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	next := slices.Clone(p.Rules)

	for _, in := range rules {
		i := slices.IndexFunc(next, func(r DrgNatRule) bool { return r.ID == in.ID })
		if i < 0 {
			return "", errNotFound(in.ID)
		}

		in.apply(&next[i])

		if aerr = checkNatRule(next[i]); aerr != nil {
			return "", aerr
		}
	}

	return b.finishNatChange(p, next)
}

// RemoveDrgNatRules removes rules by id, all or none.
func (b *InMemoryBackend) RemoveDrgNatRules(id, ifMatch string, ids []string) (string, *APIError) {
	b.mu.Lock("RemoveDrgNatRules")
	defer b.mu.Unlock()

	p, aerr := b.natPolicyForUpdate(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	for _, rid := range ids {
		if !slices.ContainsFunc(p.Rules, func(r DrgNatRule) bool { return r.ID == rid }) {
			return "", errNotFound(rid)
		}
	}

	next := slices.DeleteFunc(slices.Clone(p.Rules), func(r DrgNatRule) bool { return slices.Contains(ids, r.ID) })

	return b.finishNatChange(p, next)
}

// listDrgNatRules lists the rules of a policy by priority.
func (b *InMemoryBackend) listDrgNatRules(id, token string, limit int) (page.Page[DrgNatRule], *APIError) {
	b.mu.RLock("ListDrgNatRules")
	defer b.mu.RUnlock()

	p, aerr := get[*DrgNatPolicy](b, id)
	if aerr != nil {
		return page.Page[DrgNatRule]{}, aerr
	}

	return page.New(slices.Clone(p.Rules), token, limit, defaultListLimit), nil
}

// bindNatPolicy validates a policy reference for an attachment.
func (b *InMemoryBackend) bindNatPolicy(id string) *APIError {
	if id == "" {
		return nil
	}

	p, aerr := getLive[*DrgNatPolicy](b, id)
	if aerr != nil {
		return aerr
	}

	if p.LifecycleState != byoipActive {
		return errState("The DRG NAT policy " + id + " is " + p.LifecycleState)
	}

	return nil
}

// RemoveDrgNatPolicy detaches the NAT policy from an attachment.
func (b *InMemoryBackend) RemoveDrgNatPolicy(id, ifMatch string) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, UpdateIn{}, func(a *DrgAttachment) *APIError {
		a.DrgNatPolicyID, a.PreservesNatRoutes = "", nil

		return nil
	})
}

func drgNatEntries() []entry {
	pol := idPath("/drgNatPolicies", "drgNatPolicyId")

	return []entry{
		{createOp((*InMemoryBackend).CreateDrgNatPolicy), http.MethodPost, "/drgNatPolicies", "CreateDrgNatPolicy"},
		{listOp[*DrgNatPolicy](nil), http.MethodGet, "/drgNatPolicies", "ListDrgNatPolicies"},
		{getOp[*DrgNatPolicy]("drgNatPolicyId"), http.MethodGet, pol, "GetDrgNatPolicy"},
		{updateOp("drgNatPolicyId", (*InMemoryBackend).UpdateDrgNatPolicy), http.MethodPut, pol, "UpdateDrgNatPolicy"},
		{
			workAction("drgNatPolicyId", (*InMemoryBackend).DeleteDrgNatPolicy),
			http.MethodDelete,
			pol,
			"DeleteDrgNatPolicy",
		},
		{
			changeOp[*DrgNatPolicy]("drgNatPolicyId"), http.MethodPost, pol + "/actions/changeCompartment",
			"ChangeDrgNatPolicyCompartment",
		},
		{
			natRulesOp((*InMemoryBackend).AddDrgNatRules),
			http.MethodPost,
			pol + "/actions/addDrgNatRules",
			"AddDrgNatRules",
		},
		{
			natRulesOp((*InMemoryBackend).UpdateDrgNatRules), http.MethodPost, pol + "/actions/updateDrgNatRules",
			"UpdateDrgNatRules",
		},
		{removeNatRulesOp, http.MethodPost, pol + "/actions/removeDrgNatRules", "RemoveDrgNatRules"},
		{listNatRulesOp, http.MethodGet, pol + "/drgNatRules", "ListDrgNatRules"},
		{
			removeNatPolicyOp, http.MethodPost, "/drgAttachments/{drgAttachmentId}/actions/removeDrgNatPolicy",
			"RemoveDrgNatPolicy",
		},
	}
}

func natRulesOp(run func(*InMemoryBackend, string, string, []DrgNatRuleIn) (string, *APIError)) opFunc {
	return func(h *Handler, q *req) error {
		var in struct {
			Rules []DrgNatRuleIn `json:"rules"`
		}

		if err := q.decode(&in); err != nil {
			return err
		}

		wr, aerr := run(h.Backend, q.param("drgNatPolicyId"), q.ifMatch(), in.Rules)
		if aerr != nil {
			return aerr
		}

		return q.acceptWorkRequest(wr)
	}
}

func removeNatRulesOp(h *Handler, q *req) error {
	var in struct {
		RuleIDs []string `json:"ruleIds"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, aerr := h.Backend.RemoveDrgNatRules(q.param("drgNatPolicyId"), q.ifMatch(), in.RuleIDs)
	if aerr != nil {
		return aerr
	}

	return q.acceptWorkRequest(wr)
}

func listNatRulesOp(h *Handler, q *req) error {
	o, err := q.listOpts()
	if err != nil {
		return err
	}

	pg, aerr := h.Backend.listDrgNatRules(q.param("drgNatPolicyId"), o.Page, o.Limit)
	if aerr != nil {
		return aerr
	}

	q.nextPage(pg.Next)

	if pg.Data == nil {
		pg.Data = []DrgNatRule{}
	}

	return q.json(http.StatusOK, pg.Data, "")
}

func removeNatPolicyOp(h *Handler, q *req) error {
	v, etag, aerr := h.Backend.RemoveDrgNatPolicy(q.param("drgAttachmentId"), q.ifMatch())

	return send(q, v, etag, aerr)
}
