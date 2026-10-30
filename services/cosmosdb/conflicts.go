package cosmosdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Conflict resolution modes (azcosmos ConflictResolutionMode) and the default last-writer-wins path.
const (
	conflictModeLWW    = "LastWriterWins"
	conflictModeCustom = "Custom"
	conflictPathTS     = "/_ts"
)

// ErrConflictNotFound is returned for an unknown conflicts-feed entry.
var ErrConflictNotFound = errors.New("cosmosdb: conflict not found")

type conflictPolicy struct {
	Mode      string `json:"mode"`
	Path      string `json:"conflictResolutionPath"`
	Procedure string `json:"conflictResolutionProcedure"`
}

func parseConflictPolicy(raw json.RawMessage) conflictPolicy {
	p := conflictPolicy{Mode: conflictModeLWW, Path: conflictPathTS}

	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &p)
	}

	if p.Mode == "" {
		p.Mode = conflictModeLWW
	}

	if p.Path == "" {
		p.Path = conflictPathTS
	}

	return p
}

// conflictRecord is one entry of a region's conflicts feed: a write that lost to, or could not be reconciled with,
// the version committed there.
type conflictRecord struct {
	Time             time.Time       `json:"Time"`
	Doc              *storedDocument `json:"Doc,omitempty"`
	ID               string          `json:"ID"`
	Region           string          `json:"Region"`
	Op               string          `json:"Op"`
	DocID            string          `json:"DocID"`
	PartitionKeyJSON string          `json:"PartitionKeyJSON"`
	LSN              int64           `json:"LSN"`
}

// conflictValue is the number last-writer-wins compares: the container's resolution path in the document, or _ts
// when the path is absent, not a number or the default.
func conflictValue(d *storedDocument, path string) float64 {
	ts := float64(d.Timestamp.Unix())

	if path == conflictPathTS || path == "" {
		return ts
	}

	var cur any = d.Body

	for seg := range strings.SplitSeq(strings.Trim(path, "/"), "/") {
		m, ok := cur.(map[string]any)
		if !ok {
			return ts
		}

		cur = m[seg]
	}

	switch n := cur.(type) {
	case json.Number:
		if f, err := n.Float64(); err == nil {
			return f
		}
	case float64:
		return n
	}

	return ts
}

// resolveConflict applies the container's policy to a write that arrived at a region holding a version the writer
// had not seen (cur, when exists), or a delete of the item (a tombstone) when !exists.
func (b *InMemoryBackend) resolveConflict(
	dbID string, c, dest *storedContainer, r *replState, ev *replEvent, regs AccountRegions, now time.Time,
	cur *storedDocument, exists bool,
) {
	pol := parseConflictPolicy(c.ConflictResolutionPolicy)
	tx := b.replTx(dbID, c, dest, r, ev.Dest, now, regs, true)

	if ev.Stale || pol.Mode == conflictModeCustom {
		if !ev.Stale && b.runResolver(dbID, c, dest, r, ev, regs, now, pol, cur, exists) {
			return
		}

		b.addConflict(c, ev, now)

		return
	}

	switch {
	case ev.Doc == nil:
		if exists {
			tx.remove(ev.Key)
			b.noteTomb(r, ev)
		}
	case !exists:
	default:
		in, ex := conflictValue(ev.Doc, pol.Path), conflictValue(cur, pol.Path)
		if in > ex || (in == ex && ev.LSN > cur.GLSN) {
			b.putReplicated(tx, r, ev)
		}
	}
}

// runResolver runs the container's merge stored procedure in the destination region as one transaction; it reports
// whether the conflict was resolved (a missing or failing procedure sends the conflict to the feed).
func (b *InMemoryBackend) runResolver(
	dbID string, c, dest *storedContainer, r *replState, ev *replEvent, regs AccountRegions, now time.Time,
	pol conflictPolicy, cur *storedDocument, exists bool,
) bool {
	name := pol.Procedure[strings.LastIndex(pol.Procedure, "/")+1:]
	if name == "" {
		return false
	}

	s, ok := dest.scriptMap(kindSproc, false)[name]
	if !ok {
		return false
	}

	tx := b.replTx(dbID, c, dest, r, ev.Dest, now, regs, false)

	args := []any{nil, nil, ev.Doc == nil && !exists, []any{}}

	if ev.Doc != nil {
		args[0] = documentAsMap(b.documentInfo(dbID, c.ID, ev.Doc))
	}

	if exists {
		args[1] = documentAsMap(b.documentInfo(dbID, c.ID, cur))
	}

	if _, err := tx.runSproc(s, ev.Key.PartitionKeyJSON, args); err != nil {
		tx.rollback()

		return false
	}

	return true
}

func (b *InMemoryBackend) addConflict(c *storedContainer, ev *replEvent, now time.Time) {
	var doc *storedDocument

	if ev.Doc != nil {
		cp := *ev.Doc
		doc = &cp
	}

	c.Conflicts = append(c.Conflicts, &conflictRecord{
		ID: newDocumentID(), Region: ev.Dest, Op: ev.Op, Doc: doc, DocID: ev.Key.ID,
		PartitionKeyJSON: ev.Key.PartitionKeyJSON, LSN: ev.LSN, Time: now,
	})
}

// ConflictInfo is a read-only conflicts-feed entry.
type ConflictInfo struct {
	Time             time.Time
	Content          *DocumentInfo
	ID               string
	RID              string
	Self             string
	Op               string
	DocID            string
	DocRID           string
	PartitionKeyJSON string
	LSN              int64
}

func (b *InMemoryBackend) conflictInfo(dbID string, c *storedContainer, rec *conflictRecord) ConflictInfo {
	path := "dbs/" + dbID + "/colls/" + c.ID + "/conflicts/" + rec.ID
	info := ConflictInfo{
		ID: rec.ID, RID: fakeRID(path), Self: path + "/", Op: rec.Op, DocID: rec.DocID, Time: rec.Time,
		DocRID: fakeRID("dbs/" + dbID + "/colls/" + c.ID + "/docs/" + rec.DocID), LSN: rec.LSN,
		PartitionKeyJSON: rec.PartitionKeyJSON,
	}

	if rec.Doc != nil {
		d := b.documentInfo(dbID, c.ID, rec.Doc)
		info.Content = &d
	}

	return info
}

// ListConflicts returns the region's conflicts feed, oldest first. An empty region means the write region.
func (b *InMemoryBackend) ListConflicts(dbID, collID, region string) ([]ConflictInfo, error) {
	b.mu.RLock("ListConflicts")
	defer b.mu.RUnlock()

	c, err := b.findContainer(dbID, collID)
	if err != nil {
		return nil, err
	}

	region = b.resolveRegion(region)
	out := make([]ConflictInfo, 0)

	for _, rec := range c.Conflicts {
		if strings.EqualFold(rec.Region, region) {
			out = append(out, b.conflictInfo(dbID, c, rec))
		}
	}

	return out, nil
}

// GetConflict returns one conflicts-feed entry.
func (b *InMemoryBackend) GetConflict(dbID, collID, region, id string) (ConflictInfo, error) {
	b.mu.RLock("GetConflict")
	defer b.mu.RUnlock()

	c, err := b.findContainer(dbID, collID)
	if err != nil {
		return ConflictInfo{}, err
	}

	region = b.resolveRegion(region)

	for _, rec := range c.Conflicts {
		if rec.ID == id && strings.EqualFold(rec.Region, region) {
			return b.conflictInfo(dbID, c, rec), nil
		}
	}

	return ConflictInfo{}, ErrConflictNotFound
}

// DeleteConflict removes a conflicts-feed entry once the application has merged it.
func (b *InMemoryBackend) DeleteConflict(dbID, collID, region, id string) error {
	b.mu.Lock("DeleteConflict")
	defer b.mu.Unlock()

	c, err := b.findContainer(dbID, collID)
	if err != nil {
		return err
	}

	region = b.resolveRegion(region)

	for i, rec := range c.Conflicts {
		if rec.ID == id && strings.EqualFold(rec.Region, region) {
			c.Conflicts = append(c.Conflicts[:i], c.Conflicts[i+1:]...)

			return nil
		}
	}

	return ErrConflictNotFound
}

// resolveRegion maps the empty region to the write region; the lock must be held.
func (b *InMemoryBackend) resolveRegion(region string) string {
	if region == "" {
		return b.regionsView().home()
	}

	return region
}

// validateConflictPolicy checks a container's conflictResolutionPolicy: mode LastWriterWins or Custom, a path that
// starts with /.
func validateConflictPolicy(raw json.RawMessage) error {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil
	}

	var p conflictPolicy
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("%w: conflictResolutionPolicy is malformed", ErrInvalidContainerPolicy)
	}

	if p.Mode != "" && p.Mode != conflictModeLWW && p.Mode != conflictModeCustom {
		return fmt.Errorf("%w: conflictResolutionPolicy.mode must be %s or %s", ErrInvalidContainerPolicy,
			conflictModeLWW, conflictModeCustom)
	}

	if p.Path != "" && !strings.HasPrefix(p.Path, "/") {
		return fmt.Errorf("%w: conflictResolutionPolicy.conflictResolutionPath must start with '/'",
			ErrInvalidContainerPolicy)
	}

	return nil
}

// conflictPolicyChanged reports whether a replace request names a policy different from the stored one; the policy
// can only be set at container creation (Learn, "Manage conflicts between regions").
func conflictPolicyChanged(stored, requested json.RawMessage) bool {
	return len(requested) > 0 && !isJSONNull(requested) &&
		parseConflictPolicy(stored) != parseConflictPolicy(requested)
}
