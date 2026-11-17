package azuregraph

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	deltaPageSize      = 100
	maxDeltaSessions   = 64
	maxDeltaFilterIDs  = 50
	deltaTokenBytes    = 32
	deltaTokenLifetime = 7 * 24 * time.Hour
	codeSyncState      = "syncStateNotFound"
	propMembersDelta   = "members"
	removedChanged     = "changed"
	removedDeleted     = "deleted"
	paramSkipToken     = "$skiptoken"
	paramDeltaToken    = "$deltatoken"
)

var deltaTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

var deltaFilterTerm = regexp.MustCompile(`(?i)^\s*id\s+eq\s+'([^']*)'\s*$`)

// deltaState is what a delta session remembers of the objects it reported.
type deltaState struct {
	props   map[string]map[string]string
	members map[string][]string
	removed map[string]string
}

type deltaSession struct {
	expires time.Time
	tenant  string
	kind    string
	query   string
	state   deltaState
	pending []map[string]any
	paging  bool
}

// deltaQuery is the validated request of one delta call.
type deltaQuery struct {
	kind       string
	skipToken  string
	deltaToken string
	selects    []string
	filterIDs  []string
	minimal    bool
}

func (q deltaQuery) key() string {
	return strings.Join(q.selects, ",") + "|" + strings.Join(q.filterIDs, ",")
}

// DeltaPage is one response of a delta round.
type DeltaPage struct {
	NextToken  string
	DeltaToken string
	Items      []map[string]any
}

func hashJSON(v any) string {
	raw, _ := json.Marshal(v, jsonx.V1())
	sum := sha256.Sum256(raw)

	return hex.EncodeToString(sum[:8])
}

func newDeltaToken() (string, error) {
	buf := make([]byte, deltaTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("%w: %w", errNoRand, err)
	}

	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func syncStateGone(what string) *Error {
	return newErr(
		http.StatusGone,
		codeSyncState,
		"The %s is no longer valid; restart the delta query without a token.",
		what,
	)
}

// sweepDeltaSessions drops expired sessions. Callers hold the write lock.
func (b *Backend) sweepDeltaSessions() {
	now := b.now()

	for tok, s := range b.deltaSessions {
		if !s.expires.After(now) {
			delete(b.deltaSessions, tok)
		}
	}

	b.deltaOrder = slices.DeleteFunc(b.deltaOrder, func(tok string) bool {
		_, ok := b.deltaSessions[tok]

		return !ok
	})
}

func (b *Backend) putDeltaSession(s *deltaSession) (string, error) {
	tok, err := newDeltaToken()
	if err != nil {
		return "", err
	}

	for len(b.deltaOrder) >= maxDeltaSessions {
		delete(b.deltaSessions, b.deltaOrder[0])
		b.deltaOrder = b.deltaOrder[1:]
	}

	s.expires = b.now().Add(deltaTokenLifetime)
	b.deltaSessions[tok] = s
	b.deltaOrder = append(b.deltaOrder, tok)

	return tok, nil
}

func (b *Backend) takeDeltaSession(tok, kind string, paging bool, q deltaQuery) (*deltaSession, error) {
	what := "$deltatoken"
	if paging {
		what = "$skiptoken"
	}

	if !deltaTokenPattern.MatchString(tok) {
		return nil, badRequest("The %s is not valid.", what)
	}

	s, ok := b.deltaSessions[tok]
	if !ok {
		return nil, syncStateGone(what)
	}

	if s.tenant != b.cfg.TenantID || s.kind != kind || s.paging != paging {
		return nil, badRequest("The %s was not issued for this resource.", what)
	}

	if q.key() != "|" && q.key() != s.query {
		return nil, badRequest(
			"Query options cannot be changed after the first delta request; the %s already encodes them.",
			what,
		)
	}

	if paging {
		delete(b.deltaSessions, tok)
		b.deltaOrder = slices.DeleteFunc(b.deltaOrder, func(t string) bool { return t == tok })
	}

	return s, nil
}

// currentDelta renders the tracked objects of kind and the state that describes them.
func (b *Backend) currentDelta(q deltaQuery, tracked []string) (map[string]map[string]any, []string, deltaState) {
	var all []map[string]any

	switch q.kind {
	case kindUser:
		all = mapAll(b.users.slice())
	default:
		all = mapAll(b.groups.slice())
	}

	rendered := make(map[string]map[string]any, len(all))
	order := make([]string, 0, len(all))
	st := deltaState{
		props:   map[string]map[string]string{},
		members: map[string][]string{},
		removed: map[string]string{},
	}
	trackMembers := slices.Contains(q.selects, propMembersDelta)

	for _, m := range all {
		id, _ := m["id"].(string)
		if len(q.filterIDs) > 0 && !slices.Contains(q.filterIDs, lc(id)) {
			continue
		}

		props := make(map[string]string, len(tracked))

		for _, p := range tracked {
			if p != propMembersDelta {
				props[p] = hashJSON(m[p])
			}
		}

		st.props[lc(id)] = props
		rendered[lc(id)], order = m, append(order, lc(id))

		if trackMembers && q.kind == kindGroup {
			st.members[lc(id)] = slices.Clone(b.members[lc(id)])
		}
	}

	return rendered, order, st
}

func (b *Backend) memberAnnotation(ids []string, removed []string) []map[string]any {
	out := make([]map[string]any, 0, len(ids)+len(removed))

	for _, id := range ids {
		if o, ok := b.object(id); ok {
			out = append(out, map[string]any{"@odata.type": "#microsoft.graph." + o.Kind, "id": id})
		}
	}

	for _, id := range removed {
		out = append(out, map[string]any{"id": id, "@removed": map[string]any{"reason": removedDeleted}})
	}

	return out
}

func trackedProps(q deltaQuery) []string {
	switch {
	case len(q.selects) > 0:
		return q.selects
	case q.kind == kindUser:
		return userDefaultSelect()
	}

	return slices.Sorted(func(yield func(string) bool) {
		for f := range fieldsOf[Group]() {
			if !yield(f) {
				return
			}
		}
	})
}

func pickProps(m map[string]any, props []string, keepNil func(string) bool) map[string]any {
	out := map[string]any{"id": m["id"]}

	for _, p := range props {
		if p == propMembersDelta {
			continue
		}

		if v := m[p]; v != nil || keepNil(p) {
			out[p] = v
		}
	}

	return out
}

// deltaDiff lists the entries describing what changed between base and the current objects.
func (b *Backend) deltaDiff(q deltaQuery, base deltaState, tracked []string) ([]map[string]any, deltaState) {
	rendered, order, st := b.currentDelta(q, tracked)
	out := []map[string]any{}

	for _, id := range order {
		if entry, ok := b.deltaEntry(q, tracked, id, rendered[id], base, st); ok {
			out = append(out, entry)
		}
	}

	if base.props != nil {
		out = append(out, b.deltaRemovals(base, &st)...)
	}

	return out, st
}

func changedProps(tracked []string, prev, now map[string]string, known bool) []string {
	var changed []string

	for _, p := range tracked {
		if p != propMembersDelta && (!known || prev[p] != now[p]) {
			changed = append(changed, p)
		}
	}

	return changed
}

// deltaEntry renders object id for a delta round, or reports false when nothing it tracks changed.
func (b *Backend) deltaEntry(
	q deltaQuery,
	tracked []string,
	id string,
	obj map[string]any,
	base, st deltaState,
) (map[string]any, bool) {
	prev, known := base.props[id]

	changed := changedProps(tracked, prev, st.props[id], known)
	added, gone := memberDelta(base.members[id], st.members[id], known)
	if known && len(changed) == 0 && len(added) == 0 && len(gone) == 0 {
		return nil, false
	}

	keep := func(p string) bool { return known && slices.Contains(changed, p) }
	props := tracked

	if q.minimal && known {
		props = changed
	}

	entry := pickProps(obj, props, keep)

	if slices.Contains(q.selects, propMembersDelta) && q.kind == kindGroup && (!known || len(added)+len(gone) > 0) {
		entry["members@delta"] = b.memberAnnotation(added, gone)
	}

	return entry, true
}

func memberDelta(before, now []string, known bool) ([]string, []string) {
	if !known {
		return slices.Clone(now), nil
	}

	var added, gone []string

	for _, id := range now {
		if !slices.Contains(before, id) {
			added = append(added, id)
		}
	}

	for _, id := range before {
		if !slices.Contains(now, id) {
			gone = append(gone, id)
		}
	}

	return added, gone
}

func pageOf(entries []map[string]any) ([]map[string]any, []map[string]any) {
	if len(entries) <= deltaPageSize {
		return entries, nil
	}

	return entries[:deltaPageSize], entries[deltaPageSize:]
}

// Delta serves one request of a users or groups delta round.
func (b *Backend) Delta(q deltaQuery) (DeltaPage, error) {
	b.mu.Lock("Delta")
	defer b.mu.Unlock()

	b.sweepDeltaSessions()

	switch {
	case q.skipToken != "" && q.deltaToken != "":
		return DeltaPage{}, badRequest("$skiptoken and $deltatoken cannot be combined.")
	case q.skipToken != "":
		return b.deltaNextPage(q)
	case q.deltaToken != "":
		s, err := b.takeDeltaSession(q.deltaToken, q.kind, false, q)
		if err != nil {
			return DeltaPage{}, err
		}

		q.selects, q.filterIDs = splitKeyParts(s.query)
		entries, st := b.deltaDiff(q, s.state, trackedProps(q))

		return b.deltaFinish(q, entries, st)
	}

	entries, st := b.deltaDiff(q, deltaState{}, trackedProps(q))

	return b.deltaFinish(q, entries, st)
}

func splitKeyParts(key string) ([]string, []string) {
	sel, ids, _ := strings.Cut(key, "|")

	return splitNonEmpty(sel), splitNonEmpty(ids)
}

func splitNonEmpty(s string) []string {
	if s == "" {
		return nil
	}

	return strings.Split(s, ",")
}

func (b *Backend) deltaFinish(q deltaQuery, entries []map[string]any, st deltaState) (DeltaPage, error) {
	first, rest := pageOf(entries)
	s := &deltaSession{tenant: b.cfg.TenantID, kind: q.kind, query: q.key(), state: st}

	if len(rest) > 0 {
		s.paging, s.pending = true, rest
	}

	tok, err := b.putDeltaSession(s)
	if err != nil {
		return DeltaPage{}, err
	}

	if len(rest) > 0 {
		return DeltaPage{Items: first, NextToken: tok}, nil
	}

	return DeltaPage{Items: first, DeltaToken: tok}, nil
}

func (b *Backend) deltaNextPage(q deltaQuery) (DeltaPage, error) {
	s, err := b.takeDeltaSession(q.skipToken, q.kind, true, q)
	if err != nil {
		return DeltaPage{}, err
	}

	q.selects, q.filterIDs = splitKeyParts(s.query)

	return b.deltaFinish(q, s.pending, s.state)
}

func parseDeltaQuery(r *http.Request, kind string) (deltaQuery, error) {
	vals := parseQueryValues(r.URL.RawQuery)
	q := deltaQuery{
		kind: kind, skipToken: vals.Get(paramSkipToken), deltaToken: vals.Get(paramDeltaToken),
		minimal: strings.Contains(strings.ToLower(strings.Join(r.Header.Values("Prefer"), ",")), "return=minimal"),
	}

	for _, p := range []string{"$top", "$orderby", "$expand", "$search", paramCount, "$format", "$skip"} {
		if vals.Has(p) {
			return q, badRequest("The query parameter '%s' is not supported on the delta function.", p)
		}
	}

	fields := fieldsOf[User]()
	if kind == kindGroup {
		fields = fieldsOf[Group]()
		fields[propMembersDelta] = true
	}

	if spec := vals.Get("$select"); spec != "" {
		for f := range strings.SplitSeq(spec, ",") {
			f = strings.TrimSpace(f)
			if !fields[f] {
				return q, badRequest("Could not find a property named '%s' on type 'microsoft.graph.%s'.", f, kind)
			}

			if !slices.Contains(q.selects, f) {
				q.selects = append(q.selects, f)
			}
		}
	}

	ids, err := parseDeltaFilter(vals.Get("$filter"))
	q.filterIDs = ids

	return q, err
}

// parseDeltaFilter accepts only "id eq '<guid>'" terms joined by "or", at most maxDeltaFilterIDs of them.
func parseDeltaFilter(expr string) ([]string, error) {
	if strings.TrimSpace(expr) == "" {
		return nil, nil
	}

	var ids []string

	for _, term := range splitOr(expr) {
		m := deltaFilterTerm.FindStringSubmatch(term)
		if m == nil || !isGUID(m[1]) {
			return nil, unsupported(
				"Unsupported or invalid query filter clause specified for property 'id' of resource " +
					"'delta': only 'id eq {guid}' terms joined by 'or' are supported.",
			)
		}

		if !slices.Contains(ids, lc(m[1])) {
			ids = append(ids, lc(m[1]))
		}
	}

	if len(ids) > maxDeltaFilterIDs {
		return nil, badRequest("A delta $filter can name at most %d objects.", maxDeltaFilterIDs)
	}

	slices.Sort(ids)

	return ids, nil
}

var orSplitter = regexp.MustCompile(`(?i)\s+or\s+`)

func splitOr(expr string) []string { return orSplitter.Split(expr, -1) }

// delta serves GET users/delta and GET groups/delta.
func (h *Handler) delta(c *echo.Context, kind, name string) error {
	r := c.Request()
	if r.Method != http.MethodGet {
		return h.methodNotAllowed(c)
	}

	q, err := parseDeltaQuery(r, kind)
	if err != nil {
		return h.writeErr(c, err)
	}

	page, err := h.Backend.Delta(q)
	if err != nil {
		var ge *Error
		if errors.As(err, &ge) && ge.Status == http.StatusGone {
			c.Response().Header().Set("Location", odataBase(r)+"/"+name+"/delta?"+paramDeltaToken+"=")
		}

		return h.writeErr(c, err)
	}

	base := odataBase(r) + "/" + name + "/delta?"
	out := map[string]any{keyODataContext: odataBase(r) + "/$metadata#" + name, propValue: page.Items}

	if page.NextToken != "" {
		out["@odata.nextLink"] = odataBase(
			r,
		) + "/" + name + "/delta?" + paramSkipToken + "=" + url.QueryEscape(
			page.NextToken,
		)
	} else {
		out["@odata.deltaLink"] = base + paramDeltaToken + "=" + url.QueryEscape(page.DeltaToken)
	}

	return h.json(c, http.StatusOK, out)
}

func isDeltaSegment(s string) bool {
	return strings.EqualFold(s, "delta") || strings.EqualFold(s, "delta()")
}

// deltaRemovals reports objects that left the set since base, and soft-deleted ones that were purged since.
func (b *Backend) deltaRemovals(base deltaState, st *deltaState) []map[string]any {
	var out []map[string]any

	report := func(id, reason string) {
		out = append(out, map[string]any{"id": id, "@removed": map[string]any{"reason": reason}})
	}

	for _, id := range slices.Sorted(maps.Keys(base.props)) {
		if _, ok := st.props[id]; ok {
			continue
		}

		if _, soft := b.deleted.get(id); soft {
			st.removed[id] = removedChanged
			report(id, removedChanged)

			continue
		}

		report(id, removedDeleted)
	}

	for _, id := range slices.Sorted(maps.Keys(base.removed)) {
		if _, back := st.props[id]; back {
			continue
		}

		if _, soft := b.deleted.get(id); soft {
			st.removed[id] = removedChanged

			continue
		}

		report(id, removedDeleted)
	}

	return out
}
