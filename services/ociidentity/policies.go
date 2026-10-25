package ociidentity

import (
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxPolicies         = 100
	maxPolicyStatements = 50
	maxPolicyNameLen    = 100
	dateLayout          = "2006-01-02"
)

// CreatePolicyIn is the CreatePolicy request body.
type CreatePolicyIn struct {
	Tags
	VersionDate   *string  `json:"versionDate"`
	CompartmentID string   `json:"compartmentId"`
	Name          string   `json:"name"`
	Description   string   `json:"description"`
	Statements    []string `json:"statements"`
}

// UpdatePolicyIn is the UpdatePolicy request body.
type UpdatePolicyIn struct {
	Description *string  `json:"description"`
	VersionDate *string  `json:"versionDate"`
	Statements  []string `json:"statements"`
	Tags
}

// PolicyFilter holds the ListPolicies query.
type PolicyFilter struct {
	CompartmentID string
	Name          string
	State         string
}

func validateStatements(stmts []string) *APIError {
	if len(stmts) == 0 {
		return errMissing("statements must contain at least one statement")
	}

	if len(stmts) > maxPolicyStatements {
		return errLimit("A policy can hold at most 50 statements")
	}

	for _, s := range stmts {
		if strings.TrimSpace(s) == "" {
			return errParam("Policy statements must not be empty")
		}

		if err := ValidateStatement(s); err != nil {
			return errParam("Invalid policy statement '" + s + "': " + err.Error())
		}
	}

	return nil
}

func validateVersionDate(v *string) *APIError {
	if v == nil || *v == "" {
		return nil
	}

	if _, err := time.Parse(dateLayout, *v); err != nil {
		return errParam("versionDate must be a date in the form YYYY-MM-DD")
	}

	return nil
}

func (p *Policy) view() policyWire {
	w := policyWire{Meta: p.Meta, Statements: p.Statements}
	if p.VersionDate != "" {
		w.VersionDate = &p.VersionDate
	}

	return w
}

func (b *InMemoryBackend) policyByID(id string) (*Policy, *APIError) {
	p, ok := b.policies[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return p, nil
}

// CreatePolicy creates a policy after validating every statement.
func (b *InMemoryBackend) CreatePolicy(in CreatePolicyIn) (policyWire, string, error) {
	if in.CompartmentID == "" {
		return policyWire{}, "", errMissing("compartmentId is required")
	}

	if aerr := validateName(in.Name, maxPolicyNameLen); aerr != nil {
		return policyWire{}, "", aerr
	}

	if aerr := validateDescription(in.Description); aerr != nil {
		return policyWire{}, "", aerr
	}

	if aerr := validateStatements(in.Statements); aerr != nil {
		return policyWire{}, "", aerr
	}

	if aerr := validateVersionDate(in.VersionDate); aerr != nil {
		return policyWire{}, "", aerr
	}

	b.mu.Lock("CreatePolicy")
	defer b.mu.Unlock()

	if aerr := b.activeParent(in.CompartmentID); aerr != nil {
		return policyWire{}, "", aerr
	}

	if len(b.policies) >= maxPolicies {
		return policyWire{}, "", errLimit("The tenancy policy limit (100) has been reached")
	}

	for _, p := range b.policies {
		if strs.Equal(p.Name, in.Name) {
			return policyWire{}, "", errCode(http.StatusConflict, "Conflict", "Policy "+in.Name+" already exists in the tenancy")
		}
	}

	p := &Policy{
		Meta:       b.newMeta("policy", in.CompartmentID, in.Name, in.Description, in.Tags),
		Statements: append([]string(nil), in.Statements...), Internal: b.newInternal(),
	}
	if in.VersionDate != nil {
		p.VersionDate = *in.VersionDate
	}

	b.policies[p.ID] = p

	return p.view(), p.Internal.Etag, nil
}

// GetPolicy returns one policy.
func (b *InMemoryBackend) GetPolicy(id string) (policyWire, string, error) {
	b.mu.RLock("GetPolicy")
	defer b.mu.RUnlock()

	p, aerr := b.policyByID(id)
	if aerr != nil {
		return policyWire{}, "", aerr
	}

	return p.view(), p.Internal.Etag, nil
}

// ListPolicies lists the policies of one compartment.
func (b *InMemoryBackend) ListPolicies(f PolicyFilter, o ListOpts) (page.Page[policyWire], error) {
	if f.CompartmentID == "" {
		return page.Page[policyWire]{}, errMissing("compartmentId is required")
	}

	if aerr := validListOpts(o, "TIMECREATED", "NAME"); aerr != nil {
		return page.Page[policyWire]{}, aerr
	}

	if aerr := validState(f.State, b.listStates()...); aerr != nil {
		return page.Page[policyWire]{}, aerr
	}

	b.mu.RLock("ListPolicies")
	defer b.mu.RUnlock()

	if !b.compartmentExists(f.CompartmentID) {
		return page.Page[policyWire]{}, errNotFound(f.CompartmentID)
	}

	var all []*Policy

	for _, p := range b.policies {
		if p.CompartmentID == f.CompartmentID && (f.Name == "" || p.Name == f.Name) && matchState(p.LifecycleState, f.State) {
			all = append(all, p)
		}
	}

	pg := paginate(all, o, func(p *Policy) string { return p.Name },
		func(p *Policy) (time.Time, int64) { return p.TimeCreated, p.Internal.Seq })

	out := make([]policyWire, 0, len(pg.Data))
	for _, p := range pg.Data {
		out = append(out, p.view())
	}

	return page.Page[policyWire]{Data: out, Next: pg.Next}, nil
}

// UpdatePolicy changes description, statements, versionDate and tags.
func (b *InMemoryBackend) UpdatePolicy(id, ifMatch string, in UpdatePolicyIn) (policyWire, string, error) {
	if in.Statements != nil {
		if aerr := validateStatements(in.Statements); aerr != nil {
			return policyWire{}, "", aerr
		}
	}

	if aerr := validateVersionDate(in.VersionDate); aerr != nil {
		return policyWire{}, "", aerr
	}

	b.mu.Lock("UpdatePolicy")
	defer b.mu.Unlock()

	p, aerr := b.policyByID(id)
	if aerr != nil {
		return policyWire{}, "", aerr
	}

	if aerr = checkEtag(p.Internal.Etag, ifMatch); aerr != nil {
		return policyWire{}, "", aerr
	}

	if in.Description != nil {
		if aerr = validateDescription(*in.Description); aerr != nil {
			return policyWire{}, "", aerr
		}

		p.Description = *in.Description
	}

	if in.Statements != nil {
		p.Statements = append([]string(nil), in.Statements...)
	}

	if in.VersionDate != nil {
		p.VersionDate = *in.VersionDate
	}

	applyTags(&p.Tags, in.Tags)
	p.Internal.Etag = newEtag()

	return p.view(), p.Internal.Etag, nil
}

// DeletePolicy removes a policy.
func (b *InMemoryBackend) DeletePolicy(id, ifMatch string) error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	p, aerr := b.policyByID(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(p.Internal.Etag, ifMatch); aerr != nil {
		return aerr
	}

	delete(b.policies, id)

	return nil
}

func (h *Handler) createPolicy(q *req) error {
	var in CreatePolicyIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.CreatePolicy(in)

	return reply(q, v, etag, err)
}

func (h *Handler) getPolicy(q *req) error {
	v, etag, err := h.Backend.GetPolicy(q.param("policyId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listPolicies(q *req) error {
	pg, err := h.Backend.ListPolicies(
		PolicyFilter{CompartmentID: q.query("compartmentId"), Name: q.query("name"), State: q.query("lifecycleState")},
		q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updatePolicy(q *req) error {
	var in UpdatePolicyIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdatePolicy(q.param("policyId"), q.ifMatch(), in)

	return reply(q, v, etag, err)
}

func (h *Handler) deletePolicy(q *req) error {
	return done(q, h.Backend.DeletePolicy(q.param("policyId"), q.ifMatch()))
}
