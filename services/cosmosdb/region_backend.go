package cosmosdb

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ErrReadSessionNotAvailable is returned when a read names a session token the region has not caught up with
// (HTTP 404, substatus 1002).
var ErrReadSessionNotAvailable = errors.New("cosmosdb: read session is not available for the input session token")

// sessionTokenMinParts is the segments of a token with no per-region positions: "0:-1#<lsn>".
const sessionTokenMinParts = 2

// regionCall is the region a data-plane request runs in. The zero value is the unreplicated account.
type regionCall struct {
	token    map[string]int64
	region   string
	level    string
	hasToken bool
}

func (b *InMemoryBackend) notifyRegions() {
	if b.regionsChanged != nil {
		b.regionsChanged()
	}
}

func (b *InMemoryBackend) setRegionsLocked(next AccountRegions) {
	b.regions = next
	b.reconcileRepl()
	b.budgetEpoch++
}

// findDataContainer finds the container as the current request's region sees it. read adds the session check.
func (b *InMemoryBackend) findDataContainer(dbID, containerID string, read bool) (*storedContainer, error) {
	c, err := b.findContainer(dbID, containerID)
	if err != nil {
		return nil, err
	}

	rc := b.cur.Load()
	if rc == nil || c.repl == nil {
		return c, nil
	}

	r := c.repl
	if read && rc.level == ConsistencySession && rc.hasToken && !r.satisfies(rc.region, rc.token) {
		return nil, ErrReadSessionNotAvailable
	}

	dest := r.container(c, rc.region)
	if dest != c {
		copyConfig(dest, c)
	}

	return dest, nil
}

// satisfies reports whether the region has applied every write the token names.
func (r *replState) satisfies(region string, token map[string]int64) bool {
	have := r.appliedFor(region)

	for origin, lsn := range token {
		if have[origin] < lsn {
			return false
		}
	}

	return true
}

// sessionToken renders the region's progress as an x-ms-session-token value: the highest sequence number it holds
// and the per-origin positions (ids are the regions' 1-based order by name).
func (b *InMemoryBackend) sessionToken(c *storedContainer, region string) string {
	regs := b.regionsView()
	have := c.repl.appliedFor(region)

	var top int64

	parts := make([]string, 0, len(have))

	for _, reg := range regs.Regions {
		if lsn := have[reg.Name]; lsn > 0 {
			top = max(top, lsn)
			parts = append(parts, strconv.Itoa(regs.regionID(reg.Name))+"="+strconv.FormatInt(lsn, 10))
		}
	}

	return "0:-1#" + strconv.FormatInt(top, 10) + joinHash(parts)
}

func joinHash(parts []string) string {
	if len(parts) == 0 {
		return ""
	}

	return "#" + strings.Join(parts, "#")
}

// parseSessionToken reads the per-origin positions of a session token; an unreadable token names none.
func (b *InMemoryBackend) parseSessionToken(raw string) (map[string]int64, bool) {
	segs := strings.Split(raw, "#")
	if len(segs) < sessionTokenMinParts {
		return nil, false
	}

	regs := b.regionsView()
	out := map[string]int64{}

	for _, seg := range segs[2:] {
		id, lsn, ok := strings.Cut(seg, "=")
		n, idErr := strconv.Atoi(id)
		v, lsnErr := strconv.ParseInt(lsn, 10, 64)

		if !ok || idErr != nil || lsnErr != nil {
			continue
		}

		for _, reg := range regs.Regions {
			if regs.regionID(reg.Name) == n {
				out[reg.Name] = v
			}
		}
	}

	return out, true
}

// pumpDue delivers every write whose replication lag has elapsed.
func (b *InMemoryBackend) pumpDue() {
	b.mu.Lock("pumpDue")
	defer b.mu.Unlock()

	regs := b.regionsView()
	if !regs.replicated() {
		return
	}

	for _, d := range b.databases {
		for _, c := range d.Containers {
			if c.repl == nil {
				b.reconcileContainer(d.ID, c, regs)
			}

			b.deliver(d.ID, c, regs, false)
		}
	}
}

// SweepReplication delivers due replicated writes; the janitor calls it so regions converge without traffic.
func (b *InMemoryBackend) SweepReplication() int {
	b.regionGate.Lock("SweepReplication")
	defer b.regionGate.Unlock()

	before := b.pendingEvents()
	b.pumpDue()

	return max(0, before-b.pendingEvents())
}

func (b *InMemoryBackend) pendingEvents() int {
	b.mu.RLock("pendingEvents")
	defer b.mu.RUnlock()

	n := 0

	for _, d := range b.databases {
		for _, c := range d.Containers {
			if c.repl != nil {
				n += len(c.repl.events)
			}
		}
	}

	return n
}

// regionBackend runs the data-plane calls of one request in one region. Calls are serialised on the backend's
// region gate because the region is request state the backend methods read.
type regionBackend struct {
	*InMemoryBackend
	setToken func(string)
	call     regionCall
}

func gated[T any](r *regionBackend, dbID, collID string, fn func() (T, error)) (T, error) {
	b := r.InMemoryBackend

	b.regionGate.Lock("regionCall")
	defer b.regionGate.Unlock()

	rc := r.call
	b.cur.Store(&rc)

	defer b.cur.Store(nil)

	b.pumpDue()

	out, err := fn()

	b.pumpDue()
	r.stampToken(dbID, collID)

	return out, err
}

func (r *regionBackend) stampToken(dbID, collID string) {
	if r.setToken == nil {
		return
	}

	b := r.InMemoryBackend

	b.mu.RLock("stampToken")
	defer b.mu.RUnlock()

	if c, err := b.findContainer(dbID, collID); err == nil && c.repl != nil {
		r.setToken(b.sessionToken(c, r.call.region))
	}
}

func (r *regionBackend) CreateDocument(dbID, collID string, body map[string]any, upsert bool) (DocumentInfo, error) {
	return gated(r, dbID, collID, func() (DocumentInfo, error) {
		return r.InMemoryBackend.CreateDocument(dbID, collID, body, upsert)
	})
}

func (r *regionBackend) GetDocument(dbID, collID, pk, id string) (DocumentInfo, error) {
	return gated(r, dbID, collID, func() (DocumentInfo, error) {
		return r.InMemoryBackend.GetDocument(dbID, collID, pk, id)
	})
}

func (r *regionBackend) ReplaceDocument(
	dbID, collID, pk, id string, body map[string]any, ifMatch string,
) (DocumentInfo, error) {
	return gated(r, dbID, collID, func() (DocumentInfo, error) {
		return r.InMemoryBackend.ReplaceDocument(dbID, collID, pk, id, body, ifMatch)
	})
}

func (r *regionBackend) PatchDocument(
	dbID, collID, pk, id string, req patchRequest, ifMatch string,
) (DocumentInfo, error) {
	return gated(r, dbID, collID, func() (DocumentInfo, error) {
		return r.InMemoryBackend.PatchDocument(dbID, collID, pk, id, req, ifMatch)
	})
}

func (r *regionBackend) DeleteDocument(dbID, collID, pk, id, ifMatch string) error {
	_, err := gated(r, dbID, collID, func() (struct{}, error) {
		return struct{}{}, r.InMemoryBackend.DeleteDocument(dbID, collID, pk, id, ifMatch)
	})

	return err
}

func (r *regionBackend) ListDocuments(dbID, collID string) ([]DocumentInfo, error) {
	return gated(r, dbID, collID, func() ([]DocumentInfo, error) {
		return r.InMemoryBackend.ListDocuments(dbID, collID)
	})
}

func (r *regionBackend) ListDocumentsPaged(
	dbID, collID, token string, limit int,
) (page.Page[DocumentInfo], error) {
	return gated(r, dbID, collID, func() (page.Page[DocumentInfo], error) {
		return r.InMemoryBackend.ListDocumentsPaged(dbID, collID, token, limit)
	})
}

func (r *regionBackend) RunQuery(
	dbID, collID, query string, params []QueryParameter, pk string,
) (QueryResult, error) {
	return gated(r, dbID, collID, func() (QueryResult, error) {
		return r.InMemoryBackend.RunQuery(dbID, collID, query, params, pk)
	})
}

func (r *regionBackend) RunQueryOpts(
	dbID, collID, query string, params []QueryParameter, pk string, opts QueryOptions,
) (QueryResult, error) {
	return gated(r, dbID, collID, func() (QueryResult, error) {
		return r.InMemoryBackend.RunQueryOpts(dbID, collID, query, params, pk, opts)
	})
}

func (r *regionBackend) ExecuteBatch(dbID, collID, pk string, ops []batchOp) ([]BatchResult, bool, error) {
	var failed bool

	res, err := gated(r, dbID, collID, func() ([]BatchResult, error) {
		out, f, e := r.InMemoryBackend.ExecuteBatch(dbID, collID, pk, ops)
		failed = f

		return out, e
	})

	return res, failed, err
}

func (r *regionBackend) ExecuteSproc(dbID, collID, id, pk string, args []any) (ScriptOutput, error) {
	return gated(r, dbID, collID, func() (ScriptOutput, error) {
		return r.InMemoryBackend.ExecuteSproc(dbID, collID, id, pk, args)
	})
}

func (r *regionBackend) runTriggered(dbID, collID string, op docOp) (docOpResult, error) {
	return gated(r, dbID, collID, func() (docOpResult, error) {
		return r.InMemoryBackend.runTriggered(dbID, collID, op)
	})
}

func (r *regionBackend) readChangeFeed(dbID, collID string, req changeFeedRequest) (changeFeedPage, error) {
	return gated(r, dbID, collID, func() (changeFeedPage, error) {
		return r.InMemoryBackend.readChangeFeed(dbID, collID, req)
	})
}

func (r *regionBackend) ListConflicts(dbID, collID string) ([]ConflictInfo, error) {
	return gated(r, dbID, collID, func() ([]ConflictInfo, error) {
		return r.InMemoryBackend.ListConflicts(dbID, collID, r.call.region)
	})
}

func (r *regionBackend) GetConflict(dbID, collID, id string) (ConflictInfo, error) {
	return gated(r, dbID, collID, func() (ConflictInfo, error) {
		return r.InMemoryBackend.GetConflict(dbID, collID, r.call.region, id)
	})
}

func (r *regionBackend) DeleteConflict(dbID, collID, id string) error {
	_, err := gated(r, dbID, collID, func() (struct{}, error) {
		return struct{}{}, r.InMemoryBackend.DeleteConflict(dbID, collID, r.call.region, id)
	})

	return err
}

// consistencyRank orders the levels strongest first.
func consistencyRank(level string) int {
	for i, l := range []string{
		ConsistencyStrong, ConsistencyBoundedStaleness, ConsistencySession, ConsistencyConsistentPrefix,
		ConsistencyEventual,
	} {
		if strings.EqualFold(l, level) {
			return i
		}
	}

	return -1
}

func canonicalConsistency(level string) string {
	for _, l := range []string{
		ConsistencyStrong, ConsistencyBoundedStaleness, ConsistencySession, ConsistencyConsistentPrefix,
		ConsistencyEventual,
	} {
		if strings.EqualFold(l, level) {
			return l
		}
	}

	return ""
}

// checkRequestConsistency validates the x-ms-consistency-level override: a request can only weaken the account
// level.
func checkRequestConsistency(requested string, account ConsistencyPolicy) (string, error) {
	if requested == "" {
		return account.Level, nil
	}

	canon := canonicalConsistency(requested)
	if canon == "" {
		return "", fmt.Errorf("%w: unknown consistency level %q", ErrInvalidConsistency, requested)
	}

	if consistencyRank(canon) < consistencyRank(account.Level) {
		return "", fmt.Errorf("%w: %s is stronger than the account default %s", ErrInvalidConsistency, canon,
			account.Level)
	}

	return canon, nil
}

// ErrInvalidConsistency is returned for an x-ms-consistency-level the account cannot honour.
var ErrInvalidConsistency = errors.New("cosmosdb: invalid consistency level")
