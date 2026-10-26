package bedrockagentcorecontrol

import (
	"cmp"
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	idSuffixLen  = 10
	idToken12Len = 12
	timeEpoch    = "epoch"
	timeDateTime = "datetime"
)

// reply is a rendered operation result with its HTTP status.
type reply struct {
	body   map[string]any
	status int
}

// visible is the status clients observe: the transitional one until its deadline passes.
func (b *InMemoryBackend) visible(r *Resource) string {
	if r.Transition != "" && b.now().Before(r.SettleAt) {
		return r.Transition
	}

	return r.Status
}

func (b *InMemoryBackend) live(r *Resource) bool {
	return !r.Deleting || b.now().Before(r.SettleAt)
}

func (b *InMemoryBackend) settle(r *Resource, transition string) {
	r.Updated = b.now()

	if b.dwell > 0 {
		r.Transition = transition
		r.SettleAt = r.Updated.Add(b.dwell)

		return
	}

	r.Transition = ""
	r.SettleAt = r.Updated
}

func (b *InMemoryBackend) purgeLocked() {
	for _, r := range b.resources.All() {
		if r.Deleting && !b.live(r) {
			b.resources.Delete(resourceKey(r))
		}
	}
}

func newID(k *kindSpec, name string) string {
	switch k.IDMode {
	case keyName:
		prefix := name
		if k.Name == kindGateway {
			prefix = strings.ToLower(name)
		}

		return awsid.NewN(prefix+"-", idSuffixLen)
	case idModeToken12:
		return awsid.NewN("", idToken12Len)
	default:
		return strings.ToUpper(awsid.NewN("", idSuffixLen))
	}
}

func (b *InMemoryBackend) arnOf(k *kindSpec, sc scope, parent, id string) string {
	if k.ARNType == "" {
		return ""
	}

	typ := strings.ReplaceAll(k.ARNType, "{parent}", parent)

	return arn.Build(arnService, sc.region, sc.account, typ+"/"+id)
}

func (b *InMemoryBackend) find(sc scope, k *kindSpec, parent, id string) (*Resource, bool) {
	r, ok := b.resources.Get(makeKey(sc.account, sc.region, k.Name, parent, id))
	if !ok || !b.live(r) {
		return b.systemResource(sc, k, id)
	}

	return r, true
}

func (b *InMemoryBackend) mustFind(sc scope, k *kindSpec, parent, id string) (*Resource, error) {
	if r, ok := b.find(sc, k, parent, id); ok {
		return r, nil
	}

	return nil, notFound("%s %q not found", humanKind(k), id)
}

func humanKind(k *kindSpec) string {
	var sb strings.Builder

	for i, c := range k.Name {
		if i > 0 && c >= 'A' && c <= 'Z' {
			sb.WriteByte(' ')
		}

		sb.WriteRune(c)
	}

	return strings.ToLower(sb.String())
}

// parentOf resolves and verifies the parent resource named by the call's parent label.
func (b *InMemoryBackend) parentOf(sc scope, k *kindSpec, in args) (*Resource, error) {
	if k.Parent == "" {
		return nil, nil //nolint:nilnil // top-level kinds have no parent
	}

	pk := b.kinds[k.Parent]

	return b.mustFind(sc, pk, "", idOf(in.str(k.ParentLabel)))
}

func parentID(p *Resource) string {
	if p == nil {
		return ""
	}

	return p.ID
}

func (b *InMemoryBackend) resolve(sc scope, k *kindSpec, in args) (*Resource, error) {
	p, err := b.parentOf(sc, k, in)
	if err != nil {
		return nil, err
	}

	id := idOf(in.str(k.IDLabel))
	if r, ok := b.find(sc, k, parentID(p), id); ok {
		return r, nil
	}

	if k.Name == kindConsentPortal {
		for _, r := range b.scoped(sc, k, "") {
			if r.Doc[k.NameKey] == id {
				return r, nil
			}
		}
	}

	return nil, notFound("%s %q not found", humanKind(k), id)
}

func (b *InMemoryBackend) scoped(sc scope, k *kindSpec, parent string) []*Resource {
	var out []*Resource

	for _, r := range b.resources.All() {
		if r.Kind == k.Name && r.Account == sc.account && r.Region == sc.region &&
			(parent == "" || r.Parent == parent) && b.live(r) {
			out = append(out, r)
		}
	}

	for _, id := range []string{"aws.browser.v1", "aws.codeinterpreter.v1"} {
		if sys, ok := b.systemResource(sc, k, id); ok && parent == "" {
			out = append(out, sys)
		}
	}

	slices.SortFunc(out, func(x, y *Resource) int {
		if c := x.Created.Compare(y.Created); c != 0 {
			return c
		}

		return cmp.Compare(x.ID, y.ID)
	})

	return out
}

func copyDoc(in args, skip ...string) map[string]any {
	doc := make(map[string]any, len(in))

	for k, v := range in {
		if !slices.Contains(skip, k) {
			doc[k] = v
		}
	}

	return doc
}

func tagsOf(in args) map[string]string {
	if !in.has(keyTags) {
		return nil
	}

	return in.strMap(keyTags)
}

func (b *InMemoryBackend) create(sc scope, k *kindSpec, op string, in args) (*reply, error) {
	b.mu.Lock("create")
	defer b.mu.Unlock()

	b.purgeLocked()

	fresh := false

	res, err := idempotency.Create(b.idem, op, in.str(keyClientToken), idempotency.Fingerprint(in),
		resourceKey,
		func(key string) (*Resource, error) {
			if r, ok := b.resources.Get(key); ok {
				return r, nil
			}

			return nil, notFound("resource not found")
		},
		func() (*Resource, error) {
			fresh = true

			return b.createLocked(sc, k, in)
		})
	if err != nil {
		if errIs(err, idempotency.ErrParamsMismatch) {
			return nil, restroute.NewError(
				http.StatusConflict,
				"ConflictException",
				"clientToken was used with different parameters",
			)
		}

		return nil, err
	}

	shown := b.visible(res)
	if fresh {
		shown = creatingStatus(k)
	}

	return b.render(op, k, res, shown, nil), nil
}

func creatingStatus(k *kindSpec) string {
	if k.Name == kindPolicyGeneration {
		return "GENERATING"
	}

	return stateCreating
}

func (b *InMemoryBackend) createLocked(sc scope, k *kindSpec, in args) (*Resource, error) {
	if err := b.validateCreate(sc, k, in); err != nil {
		return nil, err
	}

	parent, err := b.parentOf(sc, k, in)
	if err != nil {
		return nil, err
	}

	name := in.str(k.NameKey)

	if err = validateName(k, name); err != nil {
		return nil, err
	}

	id := b.chooseID(k, name, in)
	pid := parentID(parent)

	if _, exists := b.find(sc, k, pid, id); exists && k.IDMode != idModeToken {
		return nil, conflict("%s %q already exists", humanKind(k), id)
	}

	if name != "" && k.IDMode != idModeNameKeyed {
		for _, o := range b.scoped(sc, k, pid) {
			if o.Doc[k.NameKey] == name {
				return nil, conflict("%s with name %q already exists", humanKind(k), name)
			}
		}
	}

	now := b.now()
	r := &Resource{
		Kind: k.Name, Account: sc.account, Region: sc.region, Parent: pid, ID: id,
		Doc:     copyDoc(in, keyClientToken, keyTags, k.ParentLabel, "version"),
		Tags:    tagsOf(in),
		Created: now, Updated: now, SettleAt: now,
		Status: readyStatus(k),
	}
	r.ARN = b.arnOf(k, sc, pid, id)

	if parent != nil {
		r.ParentARN = parent.ARN
	}

	if err = b.decorateCreate(sc, k, r, in); err != nil {
		return nil, err
	}

	b.settleCreate(k, r)
	b.resources.Put(r)
	b.afterCreate(sc, k, r)

	return r, nil
}

func (b *InMemoryBackend) chooseID(k *kindSpec, name string, in args) string {
	switch {
	case k.IDMode == idModeNameKeyed:
		return name
	case k.ClientID != "" && in.str(k.ClientID) != "":
		return in.str(k.ClientID)
	default:
		return newID(k, name)
	}
}

func readyStatus(k *kindSpec) string {
	if k.Terminal {
		return ""
	}

	return k.Ready
}

func (b *InMemoryBackend) settleCreate(k *kindSpec, r *Resource) {
	if k.Terminal {
		return
	}

	b.settle(r, creatingStatus(k))
}

func (b *InMemoryBackend) update(sc scope, k *kindSpec, op string, in args) (*reply, error) {
	b.mu.Lock("update")
	defer b.mu.Unlock()

	b.purgeLocked()

	r, err := b.resolve(sc, k, in)
	if err != nil {
		return nil, err
	}

	if !k.Terminal && isTransitional(b.visible(r)) {
		return nil, conflict("%s %q is %s and cannot be updated", humanKind(k), r.ID, b.visible(r))
	}

	sp := specs()[op]
	patch := copyDoc(in, keyClientToken, k.ParentLabel, k.IDLabel)

	for q := range patch {
		if sp.isQuery(q) {
			delete(patch, q)
		}
	}

	unwrapUpdated(k.Name, patch)

	if name, ok := patch[k.NameKey].(string); ok && k.NameKey != "" {
		if err = b.checkRename(sc, k, r, name); err != nil {
			return nil, err
		}
	}

	if err = b.validateUpdate(sc, k, r, patch); err != nil {
		return nil, err
	}

	clearOmitted(k.Name, patch)

	overrides := b.applyUpdate(k, r, patch)

	if !k.Terminal {
		b.settle(r, stateUpdating)
	} else {
		r.Updated = b.now()
	}

	return b.render(op, k, r, stateUpdating, overrides), nil
}

func (b *InMemoryBackend) del(sc scope, k *kindSpec, op string, in args) (*reply, error) {
	b.mu.Lock("delete")
	defer b.mu.Unlock()

	b.purgeLocked()

	r, err := b.resolve(sc, k, in)
	if err != nil {
		return nil, err
	}

	if r.System {
		return nil, restroute.NewError(
			http.StatusForbidden,
			"AccessDeniedException",
			"%s %q is managed by the service",
			humanKind(k),
			r.ID,
		)
	}

	if err = b.canDelete(sc, k, r, in); err != nil {
		return nil, err
	}

	if done, derr := b.deleteVersion(sc, k, r, in); done || derr != nil {
		if derr != nil {
			return nil, derr
		}

		return b.render(op, k, r, stateDeleting, nil), nil
	}

	out := b.render(op, k, r, stateDeleting, nil)
	b.removeLocked(sc, k, r)

	return out, nil
}

// removeLocked deletes r (and its dependants) now, or marks it DELETING for the lifecycle window.
func (b *InMemoryBackend) removeLocked(sc scope, k *kindSpec, r *Resource) {
	b.cascade(sc, k, r)
	b.releaseSecrets(r)

	if b.dwell > 0 && !k.Terminal {
		r.Deleting = true
		b.settle(r, stateDeleting)

		return
	}

	b.resources.Delete(resourceKey(r))
	b.policies.Delete(r.ARN)
}

func (b *InMemoryBackend) get(sc scope, k *kindSpec, op string, in args) (*reply, error) {
	b.mu.RLock("get")
	defer b.mu.RUnlock()

	r, err := b.resolve(sc, k, in)
	if err != nil {
		return nil, err
	}

	view, err := b.versionView(k, r, in)
	if err != nil {
		return nil, err
	}

	return b.render(op, k, view, b.visible(r), nil), nil
}

func (b *InMemoryBackend) list(sc scope, k *kindSpec, op string, in args) (*reply, error) {
	b.mu.RLock("list")
	defer b.mu.RUnlock()

	var parent *Resource

	if k.Parent != "" {
		p, err := b.parentOf(sc, k, in)
		if err != nil {
			return nil, err
		}

		parent = p
	}

	items := b.filter(b.scoped(sc, k, parentID(parent)), op, in)

	return b.renderList(op, k, items, in)
}

func (b *InMemoryBackend) filter(items []*Resource, op string, in args) []*Resource {
	sp := specs()[op]
	out := items[:0:0]

	for _, r := range items {
		if b.matches(r, sp, in) {
			out = append(out, r)
		}
	}

	return out
}

func (b *InMemoryBackend) matches(r *Resource, sp *opSpec, in args) bool {
	keys := append(slices.Clone(sp.Query), sp.Body...)

	for _, q := range keys {
		want := in.str(q)
		if want == "" || q == keyNextToken {
			continue
		}

		switch q {
		case keyStatus:
			if b.visible(r) != want {
				return false
			}
		case "targetResourceScope":
			raw, _ := json.Marshal(r.Doc["definition"])
			if !strings.Contains(string(raw), want) {
				return false
			}
		case keyName:
			if v, ok := r.Doc[q].(string); ok && !strings.Contains(strings.ToLower(v), strings.ToLower(want)) {
				return false
			}
		default:
			if v, ok := r.Doc[q]; ok && v != want {
				return false
			}
		}
	}

	return true
}

func (b *InMemoryBackend) renderList(op string, k *kindSpec, items []*Resource, in args) (*reply, error) {
	sp := specs()[op]
	limit, _ := in.integer(keyMaxResults)

	pg, err := restroute.Paginate(items, in.str(keyNextToken), limit, defaultPageLimit, maxPageLimit)
	if err != nil {
		return nil, err
	}

	out := map[string]any{}

	for _, f := range sp.Out {
		if f.Key == keyNextToken {
			if pg.Next != "" {
				out[f.Key] = pg.Next
			}

			continue
		}

		if !f.List {
			continue
		}

		elems := make([]any, 0, len(pg.Data))
		for _, r := range pg.Data {
			elems = append(elems, b.project(k, r, f.Elem, f.Times, b.visible(r), nil))
		}

		out[f.Key] = elems
	}

	return &reply{body: out, status: http.StatusOK}, nil
}

// render builds the operation output for r: top-level members, or the resource nested under k.Wrap.
func (b *InMemoryBackend) render(op string, k *kindSpec, r *Resource, shown string, overrides map[string]any) *reply {
	sp := specs()[op]
	out := map[string]any{}

	for _, f := range sp.Out {
		if k.Wrap != "" && f.Key == k.Wrap {
			out[f.Key] = b.project(k, r, f.Elem, f.Times, shown, overrides)

			continue
		}

		if v, ok := b.member(k, r, f.Key, sp.OutTimes[f.Key], shown, overrides); ok {
			out[f.Key] = v
		}
	}

	status := http.StatusOK
	if !k.Terminal && slices.ContainsFunc([]string{"Create", "Start", "Delete", "Update"},
		func(p string) bool { return strings.HasPrefix(op, p) }) {
		status = http.StatusAccepted
	}

	return &reply{body: out, status: status}
}

func (b *InMemoryBackend) project(
	k *kindSpec, r *Resource, keys []string, times map[string]string, shown string, overrides map[string]any,
) map[string]any {
	out := map[string]any{}

	for _, key := range keys {
		if v, ok := b.member(k, r, key, times[key], shown, overrides); ok {
			out[key] = v
		}
	}

	return out
}

func fmtTime(t time.Time, format string) any {
	if t.IsZero() {
		return nil
	}

	if format == timeDateTime {
		return t.UTC().Format(time.RFC3339Nano)
	}

	return awstime.Epoch(t)
}

// member resolves one output member of r by wire name.
func (b *InMemoryBackend) member(
	k *kindSpec, r *Resource, key, timeFmt, shown string, overrides map[string]any,
) (any, bool) {
	if v, ok := overrides[key]; ok {
		return v, true
	}

	if v, ok := systemMember(k, r, key, timeFmt, shown); ok {
		return v, true
	}

	if v, ok := r.Doc[key]; ok {
		return reformatTime(v, timeFmt), true
	}

	return b.identityMember(k, r, key)
}

// systemMember resolves members the service owns: timestamps, status and tags.
func systemMember(k *kindSpec, r *Resource, key, timeFmt, shown string) (any, bool) {
	switch key {
	case "createdAt", "createdTime", "versionCreatedAt":
		return fmtTime(r.Created, timeFmt), true
	case "updatedAt", "lastUpdatedAt", "lastUpdatedTime":
		return fmtTime(r.Updated, timeFmt), true
	case keyStatus:
		if k.Terminal || shown == "" {
			return nil, false
		}

		return shown, true
	case keyTags:
		if len(r.Tags) == 0 {
			return nil, false
		}

		return r.Tags, true
	}

	return nil, false
}

// identityMember resolves identifier, ARN, parent and aliased members that the document does not hold.
func (b *InMemoryBackend) identityMember(k *kindSpec, r *Resource, key string) (any, bool) {
	switch {
	case key == k.IDKey:
		return r.ID, true
	case key == k.ARNKey && r.ARN != "":
		return r.ARN, true
	case key == k.ParentLabel && r.Parent != "":
		return r.Parent, true
	case r.ParentARN != "" && k.Parent != "" && key == b.kinds[k.Parent].ARNKey:
		return r.ParentARN, true
	}

	alias, ok := k.Aliases[key]
	if !ok {
		return nil, false
	}

	if alias == k.IDKey {
		return r.ID, true
	}

	v, found := r.Doc[alias]

	return v, found
}

func cloneResource(r *Resource) *Resource {
	c := *r
	c.Doc = maps.Clone(r.Doc)

	return &c
}

// reformatTime renders an RFC 3339 string stored by the backend in the wire format the shape asks for.
func reformatTime(v any, format string) any {
	s, ok := v.(string)
	if !ok || format == "" {
		return v
	}

	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return v
	}

	return fmtTime(t, format)
}

func systemTime() time.Time { return time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC) }

// systemResource returns the service-owned browser or code interpreter named id, if there is one.
func (b *InMemoryBackend) systemResource(sc scope, k *kindSpec, id string) (*Resource, bool) {
	var want, typ string

	switch k.Name {
	case kindBrowser:
		want, typ = "aws.browser.v1", "browser"
	case kindCodeInterpreter:
		want, typ = "aws.codeinterpreter.v1", "code-interpreter"
	default:
		return nil, false
	}

	if id != want {
		return nil, false
	}

	return &Resource{
		Kind: k.Name, Account: "aws", Region: sc.region, ID: id, System: true, Status: stateReady,
		Created: systemTime(), Updated: systemTime(), SettleAt: systemTime(),
		ARN: arn.Build(arnService, sc.region, "aws", typ+"/"+id),
		Doc: map[string]any{
			keyName: id, "type": "SYSTEM", keyDescription: "Service-provided " + typ,
			"networkConfiguration": map[string]any{"networkMode": modePublic},
		},
	}, true
}

func (b *InMemoryBackend) checkRename(sc scope, k *kindSpec, r *Resource, name string) error {
	if err := validateName(k, name); err != nil {
		return err
	}

	if k.IDMode == idModeNameKeyed || r.Doc[k.NameKey] == name {
		return nil
	}

	for _, o := range b.scoped(sc, k, r.Parent) {
		if o.ID != r.ID && o.Doc[k.NameKey] == name {
			return conflict("%s with name %q already exists", humanKind(k), name)
		}
	}

	return nil
}

// patchWrapped lists the update members the SDK models as {"optionalValue": x} wrappers.
// An empty wrapper clears the member.
var patchWrapped = map[string][]string{ //nolint:gochecknoglobals // immutable lookup
	kindCapacityProvider: {keyDescription},
	kindPolicyEngine:     {keyDescription},
	kindPolicy:           {keyDescription},
	kindRegistry:         {keyDescription, "approvalConfiguration", keyAuthorizerConfig},
	kindRegistryRecord:   {keyDescription, "descriptors", "synchronizationConfiguration", "synchronizationType"},
	kindHarness:          {keyAuthorizerConfig, "environmentArtifact", keyMemory},
}

const keyAuthorizerConfig = "authorizerConfiguration"

// putClears lists the update members the pinned SDK documents as cleared when omitted
// (UpdateGatewayTarget certificateConfigurations, UpdateGateway exceptionLevel); all others merge.
var putClears = map[string][]string{ //nolint:gochecknoglobals // immutable lookup
	kindGateway:       {"exceptionLevel"},
	kindGatewayTarget: {"certificateConfigurations"},
}

func clearOmitted(kind string, patch map[string]any) {
	for _, key := range putClears[kind] {
		if _, present := patch[key]; !present {
			patch[key] = nil
		}
	}
}

func unwrapUpdated(kind string, patch map[string]any) {
	for _, key := range patchWrapped[kind] {
		v, present := patch[key]
		if !present {
			continue
		}

		patch[key] = unwrapOptional(v, true)
	}
}

func unwrapOptional(v any, top bool) any {
	switch t := v.(type) {
	case map[string]any:
		if inner, ok := t["optionalValue"]; ok && len(t) == 1 {
			return unwrapOptional(inner, false)
		}

		if top && len(t) == 0 {
			return nil
		}

		out := make(map[string]any, len(t))
		for k, c := range t {
			out[k] = unwrapOptional(c, false)
		}

		return out
	case []any:
		out := make([]any, len(t))
		for i, c := range t {
			out[i] = unwrapOptional(c, false)
		}

		return out
	}

	return v
}
