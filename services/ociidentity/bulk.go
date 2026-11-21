package ociidentity

import (
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	opBulkDeleteTags    = "BulkDeleteTags"
	opBulkEditTags      = "BulkEditTags"
	opListBulkEditTypes = "ListBulkEditTagsResourceTypes"

	wrBulkDeleteTags = "BULK_DELETE_TAG_DEFINITION"
	wrBulkEditTags   = "BULK_EDIT_OF_TAGS"

	maxBulkResources = 1000
	maxBulkOps       = 50
	bulkStartDelay   = transitionWindow / 2

	statusAccepted  = "ACCEPTED"
	statusProgress  = "IN_PROGRESS"
	statusSucceeded = "SUCCEEDED"
	statusFailed    = "FAILED"
	statusPartial   = "PARTIALLY_SUCCEEDED"

	bulkAdd    = "ADD_WHERE_ABSENT"
	bulkSet    = "SET_WHERE_PRESENT"
	bulkAddSet = "ADD_OR_SET"
	bulkRemove = "REMOVE"

	rtCompartment  = "compartment"
	rtDynamicGroup = "dynamicgroup"
	rtGroup        = "group"
	rtUser         = "user"
	entityTag      = "tag"
)

// BulkOp is one bulk edit operation.
type BulkOp struct {
	DefinedTags map[string]map[string]any `json:"definedTags"`
	Type        string                    `json:"type"`
}

// BulkTarget is a resource a bulk work request acts on.
type BulkTarget struct {
	Metadata     map[string]string `json:"metadata,omitempty"`
	ID           string            `json:"id"`
	ResourceType string            `json:"resourceType"`
}

// BulkResult is the outcome for one target.
type BulkResult struct {
	ID         string `json:"id"`
	EntityType string `json:"entityType"`
	Action     string `json:"action"`
}

// BulkError is one failed target.
type BulkError struct {
	At      time.Time `json:"at"`
	Code    string    `json:"code"`
	Message string    `json:"message"`
}

// BulkJob is the persisted work of a bulk tag work request.
type BulkJob struct {
	Ops     []BulkOp     `json:"ops,omitempty"`
	Targets []BulkTarget `json:"targets"`
	Results []BulkResult `json:"results"`
	Errors  []BulkError  `json:"errors"`
	Applied bool         `json:"applied"`
}

// BulkEditResourceIn is a resource in a BulkEditTags body.
type BulkEditResourceIn struct {
	Metadata     map[string]string `json:"metadata"`
	ID           string            `json:"id"`
	ResourceType string            `json:"resourceType"`
}

// BulkEditOpIn is an operation in a BulkEditTags body.
type BulkEditOpIn struct {
	DefinedTags   map[string]map[string]any `json:"definedTags"`
	OperationType string                    `json:"operationType"`
}

// BulkEditTagsIn is the BulkEditTags request body.
type BulkEditTagsIn struct {
	RetryToken    string               `json:"-"`
	CompartmentID string               `json:"compartmentId"`
	Resources     []BulkEditResourceIn `json:"resources"`
	Operations    []BulkEditOpIn       `json:"bulkEditOperations"`
}

type bulkEditType struct {
	ResourceType string   `json:"resourceType"`
	MetadataKeys []string `json:"metadataKeys"`
}

type bulkEditTypeCollection struct {
	Items []bulkEditType `json:"items"`
}

func bulkEditTypeNames() []string {
	return []string{rtCompartment, rtDynamicGroup, rtGroup, "policy", "tagnamespace", rtUser}
}

func canonicalBulkType(t string) (string, bool) {
	for _, n := range bulkEditTypeNames() {
		if strs.Equal(n, t) {
			return n, true
		}
	}

	return "", false
}

// ListBulkEditTagsResourceTypes lists the resource types BulkEditTags can edit here.
func (b *InMemoryBackend) ListBulkEditTagsResourceTypes(o ListOpts) page.Page[bulkEditType] {
	items := make([]bulkEditType, 0, len(bulkEditTypeNames()))
	for _, n := range bulkEditTypeNames() {
		items = append(items, bulkEditType{ResourceType: n, MetadataKeys: []string{}})
	}

	return page.New(items, o.Page, o.Limit, defaultListLimit)
}

func (b *InMemoryBackend) newBulkWR(op, compartment string, job *BulkJob) *WorkRequest {
	now := b.nowUTC()
	wr := &WorkRequest{
		ID: newOCID("workrequest"), Operation: op, CompartmentID: compartment, Accepted: now,
		FinishAt: now.Add(transitionWindow), Seq: b.nextSeq(), Bulk: job,
	}
	b.workReqs[wr.ID] = wr

	return wr
}

func validBulkOp(op BulkEditOpIn) (BulkOp, *APIError) {
	t := strings.ToUpper(op.OperationType)
	if !slices.Contains([]string{bulkAdd, bulkSet, bulkAddSet, bulkRemove}, t) {
		return BulkOp{}, errParam(
			"operationType must be one of ADD_WHERE_ABSENT, SET_WHERE_PRESENT, ADD_OR_SET, REMOVE",
		)
	}

	if len(op.DefinedTags) == 0 {
		return BulkOp{}, errMissing("definedTags is required for every bulk edit operation")
	}

	for ns, keys := range op.DefinedTags {
		if ns == "" || len(keys) == 0 {
			return BulkOp{}, errParam("definedTags needs a namespace name and at least one tag key")
		}
	}

	return BulkOp{Type: t, DefinedTags: op.DefinedTags}, nil
}

func validBulkEdit(in BulkEditTagsIn) ([]BulkTarget, []BulkOp, *APIError) {
	if in.CompartmentID == "" {
		return nil, nil, errMissing("compartmentId is required")
	}

	if len(in.Resources) == 0 || len(in.Operations) == 0 {
		return nil, nil, errMissing("resources and bulkEditOperations are required")
	}

	if len(in.Resources) > maxBulkResources || len(in.Operations) > maxBulkOps {
		return nil, nil, errLimit("A bulk edit takes at most 1000 resources and 50 operations")
	}

	ops := make([]BulkOp, 0, len(in.Operations))

	for _, o := range in.Operations {
		op, aerr := validBulkOp(o)
		if aerr != nil {
			return nil, nil, aerr
		}

		ops = append(ops, op)
	}

	seen := map[string]bool{}
	targets := make([]BulkTarget, 0, len(in.Resources))

	for _, r := range in.Resources {
		if r.ID == "" || r.ResourceType == "" {
			return nil, nil, errMissing("every resource needs an id and a resourceType")
		}

		k := strs.Fold(r.ResourceType) + "\x00" + r.ID
		if seen[k] {
			continue
		}

		seen[k] = true
		targets = append(targets, BulkTarget(r))
	}

	return targets, ops, nil
}

// BulkEditTags accepts an asynchronous edit of the defined tags of several resources.
func (b *InMemoryBackend) BulkEditTags(in BulkEditTagsIn) (string, error) {
	targets, ops, aerr := validBulkEdit(in)
	if aerr != nil {
		return "", aerr
	}

	b.mu.Lock("BulkEditTags")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("BulkEditTags", in.RetryToken, b.nowUTC()); ok {
		return v, nil
	}

	if aerr = b.activeParent(in.CompartmentID); aerr != nil {
		return "", aerr
	}

	wr := b.newBulkWR(wrBulkEditTags, in.CompartmentID, &BulkJob{Targets: targets, Ops: ops})
	b.retry.Remember("BulkEditTags", in.RetryToken, wr.ID, b.nowUTC())

	return wr.ID, nil
}

// BulkDeleteTags accepts an asynchronous deletion of tag definitions.
func (b *InMemoryBackend) BulkDeleteTags(ids []string, token string) (string, error) {
	if len(ids) == 0 {
		return "", errMissing("tagDefinitionIds is required")
	}

	if len(ids) > maxBulkResources {
		return "", errLimit("A bulk delete takes at most 1000 tag definitions")
	}

	b.mu.Lock("BulkDeleteTags")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("BulkDeleteTags", token, b.nowUTC()); ok {
		return v, nil
	}

	uniq := slices.Compact(slices.Sorted(slices.Values(ids)))
	for _, id := range uniq {
		if d, ok := b.tagDefs[id]; !ok || d.LifecycleState == stateDeleted {
			return "", errNotFound(id)
		}
	}

	compartment := TenancyID
	if n, ok := b.tagNS[b.tagDefs[uniq[0]].NamespaceID]; ok {
		compartment = n.CompartmentID
	}

	job := &BulkJob{}
	finish := b.nowUTC().Add(transitionWindow)

	for _, id := range uniq {
		d := b.tagDefs[id]
		meta := map[string]string{"tagNamespace": d.NamespaceName, "tagName": d.Name}
		job.Targets = append(job.Targets, BulkTarget{ID: id, ResourceType: entityTag, Metadata: meta})

		if d.LifecycleState != stateActive {
			job.Results = append(job.Results, BulkResult{ID: id, EntityType: entityTag, Action: statusFailed})
			job.Errors = append(job.Errors, BulkError{
				At: b.nowUTC(), Code: "IncorrectState",
				Message: "Tag definition " + id + " is " + d.LifecycleState + "; the operation requires ACTIVE",
			})

			continue
		}

		for tid, td := range b.tagDefaults {
			if td.TagID == id {
				delete(b.tagDefaults, tid)
			}
		}

		d.LifecycleState, d.Internal.TransitionAt, d.IsCostTrack = stateDeleting, &finish, false
		job.Results = append(job.Results, BulkResult{ID: id, EntityType: entityTag, Action: "DELETED"})
	}

	wr := b.newBulkWR(wrBulkDeleteTags, compartment, job)
	b.retry.Remember("BulkDeleteTags", token, wr.ID, b.nowUTC())

	return wr.ID, nil
}

type taggable struct {
	meta     *Meta
	internal *Internal
}

func lookupTaggable[T any](m map[string]*T, id string, f func(*T) taggable) taggable {
	if v, ok := m[id]; ok {
		return f(v)
	}

	return taggable{}
}

func (b *InMemoryBackend) taggableByID(rtype, id string) (taggable, string) {
	var t taggable

	switch rtype {
	case rtCompartment:
		t = lookupTaggable(b.compartment, id, func(c *Compartment) taggable { return taggable{&c.Meta, &c.Internal} })
	case rtUser:
		t = lookupTaggable(b.users, id, func(u *User) taggable { return taggable{&u.Meta, &u.Internal} })
	case rtGroup:
		t = lookupTaggable(b.groups, id, func(g *Group) taggable { return taggable{&g.Meta, &g.Internal} })
	case rtDynamicGroup:
		t = lookupTaggable(b.dynGroups, id, func(d *DynamicGroup) taggable { return taggable{&d.Meta, &d.Internal} })
	case "policy":
		t = lookupTaggable(b.policies, id, func(p *Policy) taggable { return taggable{&p.Meta, &p.Internal} })
	case "tagnamespace":
		t = lookupTaggable(b.tagNS, id, func(n *TagNamespace) taggable { return taggable{&n.Meta, &n.Internal} })
	}

	if t.meta == nil || t.meta.LifecycleState == stateDeleted {
		return taggable{}, "NotAuthorizedOrNotFound"
	}

	if t.meta.LifecycleState != stateActive {
		return taggable{}, "IncorrectState"
	}

	return t, ""
}

func (b *InMemoryBackend) tagDefByNames(ns, key string) (*TagNamespace, *TagDef) {
	for _, n := range b.tagNS {
		if n.LifecycleState == stateDeleted || !strs.Equal(n.Name, ns) {
			continue
		}

		for _, d := range b.tagDefs {
			if d.NamespaceID == n.ID && d.LifecycleState != stateDeleted && strs.Equal(d.Name, key) {
				return n, d
			}
		}
	}

	return nil, nil
}

func foldKey[V any](m map[string]V, k string) (string, bool) {
	for have := range m {
		if strs.Equal(have, k) {
			return have, true
		}
	}

	return "", false
}

func cloneDefined(m map[string]map[string]any) map[string]map[string]any {
	out := make(map[string]map[string]any, len(m))
	for ns, keys := range m {
		out[ns] = maps.Clone(keys)
	}

	return out
}

func (b *InMemoryBackend) editDefined(cur map[string]map[string]any, ops []BulkOp) (map[string]map[string]any, string) {
	out := cloneDefined(cur)

	for _, op := range ops {
		for _, ns := range slices.Sorted(maps.Keys(op.DefinedTags)) {
			for _, key := range slices.Sorted(maps.Keys(op.DefinedTags[ns])) {
				val := op.DefinedTags[ns][key]

				n, d := b.tagDefByNames(ns, key)
				if d == nil {
					return nil, "Tag " + ns + "." + key + " does not exist"
				}

				if msg := applyBulkOp(out, op.Type, n, d, val); msg != "" {
					return nil, msg
				}
			}
		}
	}

	return out, ""
}

func removeDefined(out map[string]map[string]any, nsKey, tagKey string) {
	delete(out[nsKey], tagKey)

	if len(out[nsKey]) == 0 {
		delete(out, nsKey)
	}
}

func applyBulkOp(out map[string]map[string]any, opType string, n *TagNamespace, d *TagDef, val any) string {
	nsKey, nsOK := foldKey(out, n.Name)
	if !nsOK {
		nsKey = n.Name
	}

	tagKey, present := "", false
	if nsOK {
		tagKey, present = foldKey(out[nsKey], d.Name)
	}

	if opType == bulkRemove {
		if present {
			removeDefined(out, nsKey, tagKey)
		}

		return ""
	}

	if n.IsRetired || d.IsRetired {
		return "Tag " + n.Name + "." + d.Name + " is retired and cannot be applied"
	}

	if s, isStr := val.(string); isStr {
		if aerr := validateTagValue(d, s); aerr != nil {
			return aerr.Message
		}
	}

	if (opType == bulkAdd && present) || (opType == bulkSet && !present) {
		return ""
	}

	if !nsOK {
		out[nsKey] = map[string]any{}
	}

	if present {
		delete(out[nsKey], tagKey)
	}

	out[nsKey][d.Name] = val

	return ""
}

func (b *InMemoryBackend) applyBulkEdit(wr *WorkRequest) {
	job := wr.Bulk
	job.Results = job.Results[:0]

	for _, t := range job.Targets {
		fail := func(code, msg string) {
			job.Results = append(job.Results, BulkResult{ID: t.ID, EntityType: t.ResourceType, Action: statusFailed})
			job.Errors = append(job.Errors, BulkError{At: wr.FinishAt, Code: code, Message: msg})
		}

		rtype, ok := canonicalBulkType(t.ResourceType)
		if !ok {
			fail("InvalidParameter", "Resource type "+t.ResourceType+
				" cannot be edited here; only resources owned by the identity service are in reach")

			continue
		}

		res, code := b.taggableByID(rtype, t.ID)
		if code != "" {
			fail(code, "Resource "+t.ID+" of type "+rtype+" is not an editable resource")

			continue
		}

		edited, msg := b.editDefined(res.meta.DefinedTags, job.Ops)
		if msg != "" {
			fail("InvalidParameter", t.ID+": "+msg)

			continue
		}

		res.meta.DefinedTags, res.internal.Etag = edited, newEtag()
		job.Results = append(job.Results, BulkResult{ID: t.ID, EntityType: t.ResourceType, Action: "UPDATED"})
	}

	job.Applied = true
}

func (b *InMemoryBackend) finishBulkDelete(wr *WorkRequest) {
	for i, t := range wr.Bulk.Targets {
		if wr.Bulk.Results[i].Action != "DELETED" {
			continue
		}

		ns, key := t.Metadata["tagNamespace"], t.Metadata["tagName"]
		for _, m := range b.allMetas() {
			if nk, ok := foldKey(m.DefinedTags, ns); ok {
				if tk, found := foldKey(m.DefinedTags[nk], key); found {
					removeDefined(m.DefinedTags, nk, tk)
				}
			}
		}
	}

	wr.Bulk.Applied = true
}

func (b *InMemoryBackend) allMetas() []*Meta {
	out := make([]*Meta, 0, len(b.compartment)+len(b.users)+len(b.groups)+len(b.dynGroups)+len(b.policies)+len(b.tagNS))

	for _, c := range b.compartment {
		out = append(out, &c.Meta)
	}

	for _, u := range b.users {
		out = append(out, &u.Meta)
	}

	for _, g := range b.groups {
		out = append(out, &g.Meta)
	}

	for _, d := range b.dynGroups {
		out = append(out, &d.Meta)
	}

	for _, p := range b.policies {
		out = append(out, &p.Meta)
	}

	for _, n := range b.tagNS {
		out = append(out, &n.Meta)
	}

	return out
}

// sweepBulk applies bulk work requests whose finish time has passed; callers hold the write lock.
func (b *InMemoryBackend) sweepBulk(now time.Time) {
	var due []*WorkRequest

	for _, wr := range b.workReqs {
		if wr.Bulk != nil && !wr.Bulk.Applied && !wr.FinishAt.After(now) {
			due = append(due, wr)
		}
	}

	slices.SortFunc(due, func(a, c *WorkRequest) int { return int(a.Seq - c.Seq) })

	for _, wr := range due {
		if wr.Operation == wrBulkEditTags {
			b.applyBulkEdit(wr)
		} else {
			b.finishBulkDelete(wr)
		}
	}
}

func bulkFinal(wr *WorkRequest, now time.Time) bool {
	return wr.Bulk.Applied && !wr.FinishAt.After(now)
}

func bulkStatus(wr *WorkRequest, now time.Time) string {
	switch {
	case now.Before(wr.Accepted.Add(bulkStartDelay)):
		return statusAccepted
	case !bulkFinal(wr, now):
		return statusProgress
	case len(wr.Bulk.Errors) == 0:
		return statusSucceeded
	case len(wr.Bulk.Errors) >= len(wr.Bulk.Targets):
		return statusFailed
	default:
		return statusPartial
	}
}

func (b *InMemoryBackend) bulkView(wr *WorkRequest) WorkRequestWire {
	now := b.nowUTC()
	status := bulkStatus(wr, now)
	started := wr.Accepted.Add(bulkStartDelay)

	out := WorkRequestWire{
		OperationType: wr.Operation, ID: wr.ID, CompartmentID: wr.CompartmentID, TimeAccepted: wr.Accepted,
		Status: status, Errors: []wrErrorWire{}, Resources: make([]wrResourceWire, 0, len(wr.Bulk.Targets)),
	}

	switch status {
	case statusAccepted:
	case statusProgress:
		out.PercentComplete, out.TimeStarted = halfComplete, &started
	default:
		finished := wr.FinishAt
		out.PercentComplete, out.TimeStarted, out.TimeFinished = 100, &started, &finished
	}

	if bulkFinal(wr, now) {
		for _, r := range wr.Bulk.Results {
			out.Resources = append(
				out.Resources,
				wrResourceWire{EntityType: r.EntityType, ActionType: r.Action, Identifier: r.ID},
			)
		}

		return out
	}

	for _, t := range wr.Bulk.Targets {
		out.Resources = append(
			out.Resources,
			wrResourceWire{EntityType: t.ResourceType, ActionType: statusProgress, Identifier: t.ID},
		)
	}

	return out
}

// TaggingErrorWire is the tagging work request error wire shape.
type TaggingErrorWire struct {
	Timestamp time.Time `json:"timestamp"`
	Code      string    `json:"code"`
	Message   string    `json:"message"`
}

// TaggingWorkRequestErrors lists the errors of a finished tagging work request.
func (b *InMemoryBackend) TaggingWorkRequestErrors(id string, o ListOpts) (page.Page[TaggingErrorWire], error) {
	b.mu.RLock("TaggingWorkRequestErrors")
	defer b.mu.RUnlock()

	wr, ok := b.workReqs[id]
	if !ok || !isTaggingOp(wr.Operation) {
		return page.Page[TaggingErrorWire]{}, errNotFound(id)
	}

	out := []TaggingErrorWire{}

	if wr.Bulk != nil && bulkFinal(wr, b.nowUTC()) {
		for _, e := range wr.Bulk.Errors {
			out = append(out, TaggingErrorWire{Timestamp: e.At, Code: e.Code, Message: e.Message})
		}
	}

	return page.New(out, o.Page, o.Limit, defaultListLimit), nil
}

func (b *InMemoryBackend) bulkLogs(wr *WorkRequest) []TaggingWRLogWire {
	now := b.nowUTC()
	started := wr.Accepted.Add(bulkStartDelay)
	logs := []TaggingWRLogWire{{Timestamp: wr.Accepted, Message: wr.Operation + " work request accepted"}}

	if !now.Before(started) {
		logs = append(logs, TaggingWRLogWire{Timestamp: started, Message: wr.Operation + " work request started"})
	}

	if !bulkFinal(wr, now) {
		return logs
	}

	for _, r := range wr.Bulk.Results {
		logs = append(
			logs,
			TaggingWRLogWire{Timestamp: wr.FinishAt, Message: r.EntityType + " " + r.ID + ": " + r.Action},
		)
	}

	return append(logs, TaggingWRLogWire{
		Timestamp: wr.FinishAt, Message: wr.Operation + " work request " + strings.ToLower(bulkStatus(wr, now)),
	})
}

func (h *Handler) bulkDeleteTags(q *req) error {
	var in struct {
		IDs []string `json:"tagDefinitionIds"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.BulkDeleteTags(in.IDs, q.r.Header.Get(headerRetry))

	return accepted(q, wr, err)
}

func (h *Handler) bulkEditTags(q *req) error {
	var in BulkEditTagsIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken = q.r.Header.Get(headerRetry)
	wr, err := h.Backend.BulkEditTags(in)

	return accepted(q, wr, err)
}

func (h *Handler) listBulkEditTagsResourceTypes(q *req) error {
	pg := h.Backend.ListBulkEditTagsResourceTypes(q.listOpts())
	q.nextPage(pg.Next)

	return q.json(http.StatusOK, bulkEditTypeCollection{Items: pg.Data}, "")
}
