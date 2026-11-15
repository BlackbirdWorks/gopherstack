package gcpbigquery

import (
	"context"
	"encoding/base64"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	litRowPolicies   = "rowAccessPolicies"
	litPolicyRefKey  = "rowAccessPolicyReference"
	litPolicyID      = "policyId"
	litTableID       = "tableId"
	litFilterPred    = "filterPredicate"
	litGrantees      = "grantees"
	maxPolicyIDLen   = 256
	policyAuditBase  = "google.cloud.bigquery.v2.RowAccessPolicyService."
	batchDeleteVerb  = "rowAccessPolicies:batchDelete"
	headerAuthedUser = "X-Goog-Authenticated-User-Email"
	jwtParts         = 3
)

// RowPolicy is a row access policy stored on a table.
type RowPolicy struct {
	ID       string   `json:"id"`
	Filter   string   `json:"filter"`
	Grantees []string `json:"grantees"`
	Created  int64    `json:"created"`
	Modified int64    `json:"modified"`
	Seq      int64    `json:"seq"`
}

//nolint:gochecknoglobals // compiled once, read-only
var policyIDPattern = sync.OnceValue(func() *regexp.Regexp { return regexp.MustCompile(`^[A-Za-z0-9_]+$`) })

func (t *Table) policyData() []bqsql.RowPolicyData {
	if len(t.RowPolicies) == 0 {
		return nil
	}

	out := make([]bqsql.RowPolicyData, len(t.RowPolicies))
	for i, p := range t.RowPolicies {
		out[i] = bqsql.RowPolicyData{Name: p.ID, Filter: p.Filter, Grantees: p.Grantees}
	}

	return out
}

func (t *Table) policyIndex(id string) int {
	for i, p := range t.RowPolicies {
		if strs.Equal(p.ID, id) {
			return i
		}
	}

	return -1
}

// checkPolicyColumns rejects a schema change that drops a column a policy filter reads.
func (t *Table) checkPolicyColumns(fields []bqsql.Field) error {
	keep := make(map[string]bool, len(fields))
	for _, f := range fields {
		keep[strs.Fold(f.Name)] = true
	}

	for _, f := range t.Schema {
		if keep[strs.Fold(f.Name)] {
			continue
		}

		for _, p := range t.RowPolicies {
			if bqsql.FilterReferences(p.Filter, f.Name) {
				return errInvalid("Cannot drop or rename column %s: row access policy %s references it", f.Name, p.ID)
			}
		}
	}

	return nil
}

func policyWire(project, ds, table string, p *RowPolicy) map[string]any {
	return map[string]any{
		litPolicyRefKey: map[string]any{
			litProjectid: project, litDatasetID: ds, litTableID: table, litPolicyID: p.ID,
		},
		litFilterPred:   p.Filter,
		litCreationtime: msString(p.Created),
		litLastModified: msString(p.Modified),
		litEtag:         etagOf("rowpolicy", project, ds, table, p.ID, msString(p.Seq)),
	}
}

func policyGrantees(body map[string]any) ([]string, error) {
	raw, ok := body[litGrantees]
	if !ok || raw == nil {
		return nil, nil
	}

	list, isList := raw.([]any)
	if !isList {
		return nil, errInvalid("grantees must be a list of IAM members")
	}

	out := make([]string, 0, len(list))

	for _, g := range list {
		s, isStr := g.(string)
		if !isStr || !bqsql.ValidGrantee(s) {
			return nil, errInvalid("Invalid grantee %v", g)
		}

		out = append(out, s)
	}

	return out, nil
}

func policyID(body map[string]any, project, ds, table, pathID string) (string, error) {
	ref := refIDs(body, litPolicyRefKey)
	id := ref[litPolicyID]

	if pathID != "" {
		if id != "" && id != pathID {
			return "", errInvalid("Policy ID in the reference (%s) does not match the request (%s)", id, pathID)
		}

		id = pathID
	}

	if id == "" {
		return "", errInvalid("Row access policy ID is required")
	}

	if len(id) > maxPolicyIDLen || !policyIDPattern().MatchString(id) {
		return "", errInvalid("Invalid row access policy ID %q.", id)
	}

	for key, want := range map[string]string{
		litProjectid: project, litDatasetID: ds, litTableID: table,
	} {
		if v := ref[key]; v != "" && v != want {
			return "", errInvalid(
				"%s in the row access policy reference (%s) does not match the request (%s)", key, v, want)
		}
	}

	return id, nil
}

func (b *InMemoryBackend) checkPolicy(project, ds, table string, grantees []string, filter string) error {
	in := &bqsql.Input{Catalog: catalog{b: b}, Project: project, Dataset: ds}
	ref := bqsql.TableRef{Project: project, Dataset: ds, Table: table}

	if err := bqsql.CheckRowPolicy(in, ref, grantees, filter); err != nil {
		return fromSQL(err)
	}

	return nil
}

// CreateRowPolicy implements rowAccessPolicies.insert.
func (b *InMemoryBackend) CreateRowPolicy(project, ds, table string, body map[string]any) (map[string]any, error) {
	id, err := policyID(body, project, ds, table, "")
	if err != nil {
		return nil, err
	}

	filter, _ := body[litFilterPred].(string)
	if strings.TrimSpace(filter) == "" {
		return nil, errInvalid("filterPredicate is required")
	}

	grantees, err := policyGrantees(body)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateRowPolicy")
	defer b.mu.Unlock()

	_, t, err := b.tableLocked(project, ds, table)
	if err != nil {
		return nil, err
	}

	if t.policyIndex(id) >= 0 {
		return nil, errDuplicate("Already Exists: Row access policy %s:%s.%s.%s", project, ds, table, id)
	}

	if err = b.checkPolicy(project, ds, table, grantees, filter); err != nil {
		return nil, err
	}

	now := nowMs()
	p := &RowPolicy{ID: id, Filter: filter, Grantees: grantees, Created: now, Modified: now, Seq: b.nextSeq()}
	t.RowPolicies = append(t.RowPolicies, p)

	b.auditCall(policyAuditBase+"InsertRowAccessPolicy", policyResource(project, ds, table, id), project)

	return policyWire(project, ds, table, p), nil
}

func policyResource(project, ds, table, id string) string {
	return "projects/" + project + "/datasets/" + ds + "/tables/" + table + "/rowAccessPolicies/" + id
}

// GetRowPolicy implements rowAccessPolicies.get.
func (b *InMemoryBackend) GetRowPolicy(project, ds, table, id string) (map[string]any, error) {
	b.mu.RLock("GetRowPolicy")
	defer b.mu.RUnlock()

	_, t, err := b.tableLocked(project, ds, table)
	if err != nil {
		return nil, err
	}

	i := t.policyIndex(id)
	if i < 0 {
		return nil, errNotFound("Not found: Row access policy %s:%s.%s.%s", project, ds, table, id)
	}

	return policyWire(project, ds, table, t.RowPolicies[i]), nil
}

// UpdateRowPolicy implements rowAccessPolicies.update: the filter is replaced and the grantee list only when sent.
func (b *InMemoryBackend) UpdateRowPolicy(
	project, ds, table, id, ifMatch string, body map[string]any,
) (map[string]any, error) {
	if _, err := policyID(body, project, ds, table, id); err != nil {
		return nil, err
	}

	filter, _ := body[litFilterPred].(string)
	if strings.TrimSpace(filter) == "" {
		return nil, errInvalid("filterPredicate is required")
	}

	grantees, err := policyGrantees(body)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateRowPolicy")
	defer b.mu.Unlock()

	_, t, err := b.tableLocked(project, ds, table)
	if err != nil {
		return nil, err
	}

	i := t.policyIndex(id)
	if i < 0 {
		return nil, errNotFound("Not found: Row access policy %s:%s.%s.%s", project, ds, table, id)
	}

	p := t.RowPolicies[i]

	if ifMatch != "" && ifMatch != policyWire(project, ds, table, p)[litEtag] {
		return nil, newErr(http.StatusPreconditionFailed, "conditionNotMet", "Precondition check failed.")
	}

	if grantees == nil {
		grantees = p.Grantees
	}

	if err = b.checkPolicy(project, ds, table, grantees, filter); err != nil {
		return nil, err
	}

	p.Filter, p.Grantees, p.Modified, p.Seq = filter, grantees, nowMs(), b.nextSeq()

	b.auditCall(policyAuditBase+"UpdateRowAccessPolicy", policyResource(project, ds, table, id), project)

	return policyWire(project, ds, table, p), nil
}

// DeleteRowPolicies implements rowAccessPolicies.delete and batchDelete. Removing the last policy widens access, so
// it needs force.
func (b *InMemoryBackend) DeleteRowPolicies(project, ds, table string, ids []string, force bool) error {
	b.mu.Lock("DeleteRowPolicies")
	defer b.mu.Unlock()

	_, t, err := b.tableLocked(project, ds, table)
	if err != nil {
		return err
	}

	drop := make(map[int]bool, len(ids))

	for _, id := range ids {
		i := t.policyIndex(id)
		if i < 0 {
			return errNotFound("Not found: Row access policy %s:%s.%s.%s", project, ds, table, id)
		}

		drop[i] = true
	}

	if len(drop) == len(t.RowPolicies) && !force {
		return errInvalid(
			"Cannot delete the last row access policy on %s:%s.%s: it would widen access; set force to true",
			project, ds, table)
	}

	kept := make([]*RowPolicy, 0, len(t.RowPolicies))

	for i, p := range t.RowPolicies {
		if !drop[i] {
			kept = append(kept, p)
		}
	}

	t.RowPolicies = kept
	if len(kept) == 0 {
		t.RowPolicies = nil
	}

	for _, id := range ids {
		b.auditCall(policyAuditBase+"DeleteRowAccessPolicy", policyResource(project, ds, table, id), project)
	}

	return nil
}

// ListRowPolicies implements rowAccessPolicies.list, ordered by policy ID.
func (b *InMemoryBackend) ListRowPolicies(
	project, ds, table string, o ListOptions,
) (page.Page[map[string]any], error) {
	b.mu.RLock("ListRowPolicies")
	defer b.mu.RUnlock()

	_, t, err := b.tableLocked(project, ds, table)
	if err != nil {
		return page.Page[map[string]any]{}, err
	}

	ps := append([]*RowPolicy(nil), t.RowPolicies...)
	sort.Slice(ps, func(i, j int) bool { return ps[i].ID < ps[j].ID })

	all := make([]map[string]any, len(ps))
	for i, p := range ps {
		all[i] = policyWire(project, ds, table, p)
	}

	return page.New(all, o.PageToken, o.Max, defaultListMax), nil
}

// effectRowPolicy applies CREATE and DROP ROW ACCESS POLICY.
func (b *InMemoryBackend) effectRowPolicy(e bqsql.Effect) error {
	t, err := b.effectTable(e)
	if err != nil {
		return err
	}

	now := nowMs()

	if e.Op == bqsql.OpDropRowPolicy {
		if e.DropAll {
			t.RowPolicies = nil

			return nil
		}

		if i := t.policyIndex(e.PolicyName); i >= 0 {
			t.RowPolicies = append(t.RowPolicies[:i], t.RowPolicies[i+1:]...)
		}

		return nil
	}

	p := &RowPolicy{
		ID: e.Policy.Name, Filter: e.Policy.Filter, Grantees: e.Policy.Grantees, Created: now, Modified: now,
		Seq: b.nextSeq(),
	}

	if i := t.policyIndex(p.ID); i >= 0 {
		p.Created = t.RowPolicies[i].Created
		t.RowPolicies[i] = p

		return nil
	}

	t.RowPolicies = append(t.RowPolicies, p)

	return nil
}

type principalKey struct{}

func withPrincipal(ctx context.Context, user string) context.Context {
	return context.WithValue(ctx, principalKey{}, user)
}

func principalFrom(ctx context.Context) string {
	if u, ok := ctx.Value(principalKey{}).(string); ok && u != "" {
		return u
	}

	return bqsql.DefaultUser
}

// requestPrincipal reads the caller from the IAP-style header or the email claim of an unverified bearer JWT.
func requestPrincipal(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get(headerAuthedUser)); v != "" {
		_, after, found := strings.Cut(v, ":")
		if found {
			return after
		}

		return v
	}

	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return ""
	}

	parts := strings.Split(tok, ".")
	if len(parts) != jwtParts {
		return ""
	}

	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return ""
	}

	claims, err := decodeObject(raw)
	if err != nil {
		return ""
	}

	email, _ := claims["email"].(string)

	return email
}
