package cosmosdb

import (
	"encoding/json"
	"strings"
	"time"
)

const (
	// maxChangeLog is a memory guard, not a retention rule: the log keeps every version inside the continuous
	// backup window unless a container writes more than this many.
	maxChangeLog = 1_000_000

	// defaultChangeRetention is the window when no account sets a backup policy (the 7-day continuous tier).
	defaultChangeRetention = 7 * 24 * time.Hour
	noChangeLog            = time.Duration(-1)
	continuous30Retention  = 30 * 24 * time.Hour

	opCreate  = "create"
	opReplace = "replace"
	opDelete  = "delete"

	headerWireFormat = "X-Ms-Cosmos-Changefeed-Wire-Format-Version"
)

// changeRecord is one version of a document in the all-versions-and-deletes change feed.
type changeRecord struct {
	Time             time.Time       `json:"Time"`
	Current          *storedDocument `json:"Current,omitempty"`
	Previous         *storedDocument `json:"Previous,omitempty"`
	Op               string          `json:"Op"`
	ID               string          `json:"ID"`
	PartitionKeyJSON string          `json:"PartitionKeyJSON"`
	Seq              int64           `json:"Seq"`
	PrevSeq          int64           `json:"PrevSeq,omitempty"`
	TTLExpired       bool            `json:"TTLExpired,omitempty"`
}

// appendChange adds rec to the container's log and drops what has fallen out of the continuous backup window
// (keep: zero is the default seven days, negative keeps nothing, as on an account with periodic backup).
func (c *storedContainer) appendChange(rec *changeRecord, now time.Time) {
	if c.keep < 0 {
		c.ChangeFloor = max(c.ChangeFloor, rec.Seq)

		return
	}

	c.ChangeLog = append(c.ChangeLog, rec)

	keep := c.keep
	if keep == 0 {
		keep = defaultChangeRetention
	}

	cut := now.Add(-keep)
	n := 0

	for n < len(c.ChangeLog) && (c.ChangeLog[n].Time.Before(cut) || len(c.ChangeLog)-n > maxChangeLog) {
		c.ChangeFloor = max(c.ChangeFloor, c.ChangeLog[n].Seq)
		n++
	}

	if n > 0 {
		c.ChangeLog = append([]*changeRecord(nil), c.ChangeLog[n:]...)
	}
}

// truncateChangeLog drops records newer than seq, undoing an aborted transaction.
func (c *storedContainer) truncateChangeLog(seq int64) {
	n := len(c.ChangeLog)
	for n > 0 && c.ChangeLog[n-1].Seq > seq {
		n--
	}

	c.ChangeLog = c.ChangeLog[:n]
}

// recordExpired logs the TTL expiry of d as a delete.
func (c *storedContainer) recordExpired(d *storedDocument, now time.Time) {
	c.ChangeSeq++

	at, _ := c.expiry(d)
	c.appendChange(&changeRecord{
		Time: at, Previous: d, Op: opDelete, ID: d.ID, PartitionKeyJSON: d.PartitionKeyJSON,
		Seq: c.ChangeSeq, PrevSeq: d.Seq, TTLExpired: true,
	}, now)
}

// ChangeVersion is one entry of the all-versions-and-deletes change feed.
type ChangeVersion struct {
	Current      *DocumentInfo
	Previous     *DocumentInfo
	PartitionKey map[string]any
	Op           string
	ID           string
	LSN          int64
	PreviousLSN  int64
	CRTS         int64
	TTLExpired   bool
}

func (b *InMemoryBackend) changeVersion(dbID, collID string, c *storedContainer, r *changeRecord) ChangeVersion {
	v := ChangeVersion{
		Op: r.Op, ID: r.ID, LSN: r.Seq, PreviousLSN: r.PrevSeq, CRTS: r.Time.Unix(), TTLExpired: r.TTLExpired,
		PartitionKey: partitionKeyObject(c.paths(), r.PartitionKeyJSON),
	}

	if r.Current != nil {
		info := b.documentInfo(dbID, collID, r.Current)
		v.Current = &info
	}

	if r.Previous != nil {
		info := b.documentInfo(dbID, collID, r.Previous)
		v.Previous = &info
	}

	return v
}

// partitionKeyObject renders a canonical partition key as {"<path without slash>": value}.
func partitionKeyObject(paths []string, pkJSON string) map[string]any {
	out := make(map[string]any, len(paths))

	var vals []any
	if len(paths) == 1 {
		var v any
		if json.Unmarshal([]byte(pkJSON), &v) == nil {
			vals = []any{v}
		}
	} else if json.Unmarshal([]byte(pkJSON), &vals) != nil {
		vals = nil
	}

	for i, p := range paths {
		if i < len(vals) {
			out[strings.TrimPrefix(p, "/")] = vals[i]
		}
	}

	return out
}

// changedVersions selects the log records a request reads, oldest first.
func (c *storedContainer) changedVersions(req changeFeedRequest) []*changeRecord {
	out := make([]*changeRecord, 0)

	for _, r := range c.ChangeLog {
		switch {
		case r.Seq <= req.AfterSeq:
		case !req.Since.IsZero() && r.Time.Unix() < req.Since.Unix():
		case req.PartitionKey != "" && !partitionKeyMatches(r.PartitionKeyJSON, req.PartitionKey):
		case !req.hashes.contains(r.PartitionKeyJSON):
		default:
			out = append(out, r)
		}
	}

	return out
}
