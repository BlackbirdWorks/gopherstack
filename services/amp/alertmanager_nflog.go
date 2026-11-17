package amp

import (
	"encoding/hex"
	"slices"
	"strings"
	"time"
)

const (
	nflogRetention     = 120 * time.Hour
	maxNflogGroups     = 10000
	maxNflogFiringKeys = 10000
)

// nflogRecord is one persisted notification log entry: what a receiver integration last sent for a group.
type nflogRecord struct {
	At     time.Time `json:"at"`
	Group  string    `json:"group"`
	Firing []string  `json:"firing"`
	Index  int       `json:"index"`
}

// Fingerprints hold raw label bytes that do not survive JSON text, so records carry them hex encoded.
func (r nflogRecord) valid() bool {
	return r.Group != "" && r.Index >= 0 && !r.At.IsZero()
}

func (s *notifyState) record(group string, idx int) nflogRecord {
	fps := make([]string, 0, len(s.firing))
	for fp := range s.firing {
		fps = append(fps, hex.EncodeToString([]byte(fp)))
	}

	slices.Sort(fps)

	if len(fps) > maxNflogFiringKeys {
		fps = fps[:maxNflogFiringKeys]
	}

	return nflogRecord{Group: hex.EncodeToString([]byte(group)), Index: idx, At: s.at, Firing: fps}
}

// recordNflogLocked remembers the last successful notification of one integration of a group.
func (m *alertManager) recordNflogLocked(group string, idx int, st *notifyState) {
	if _, known := m.nflog[group]; !known && len(m.nflog) >= maxNflogGroups {
		return
	}

	if m.nflog[group] == nil {
		m.nflog[group] = map[int]*notifyState{}
	}

	m.nflog[group][idx] = st
}

func (m *alertManager) sweepNflogLocked(now time.Time) {
	for g, states := range m.nflog {
		for idx, st := range states {
			if now.Sub(st.at) > nflogRetention {
				delete(states, idx)
			}
		}

		if len(states) == 0 {
			delete(m.nflog, g)
		}
	}
}

func (m *alertManager) nflogRecords() []nflogRecord {
	m.mu.RLock("nflogRecords")
	defer m.mu.RUnlock()

	var out []nflogRecord

	for g, states := range m.nflog {
		for idx, st := range states {
			out = append(out, st.record(g, idx))
		}
	}

	return out
}

func (m *alertManager) loadNflog(recs []nflogRecord) {
	m.mu.Lock("loadNflog")
	defer m.mu.Unlock()

	for _, r := range recs {
		group, err := hex.DecodeString(r.Group)
		if err != nil {
			continue
		}

		st := &notifyState{at: r.At, firing: make(map[string]bool, len(r.Firing))}

		for _, fp := range r.Firing {
			if raw, derr := hex.DecodeString(fp); derr == nil {
				st.firing[string(raw)] = true
			}
		}

		m.recordNflogLocked(string(group), r.Index, st)
	}
}

func sortNflog(recs []nflogRecord) {
	slices.SortFunc(recs, func(a, b nflogRecord) int {
		if c := strings.Compare(a.Group, b.Group); c != 0 {
			return c
		}

		return a.Index - b.Index
	})
}

// nflogSnapshotLocked collects the live managers' logs plus restored entries no manager has claimed yet.
func (b *InMemoryBackend) nflogSnapshotLocked() map[string][]nflogRecord {
	out := map[string][]nflogRecord{}

	for ws, recs := range b.restoredLog {
		out[ws] = slices.Clone(recs)
	}

	for ws, m := range b.ams {
		if recs := m.nflogRecords(); len(recs) > 0 {
			out[ws] = recs
		}
	}

	for _, recs := range out {
		sortNflog(recs)
	}

	return out
}

// restoreNflogLocked keeps validated, bounded records until the workspace's manager exists again.
func (b *InMemoryBackend) restoreNflogLocked(in map[string][]nflogRecord) {
	b.restoredLog = map[string][]nflogRecord{}

	for ws, recs := range in {
		var keep []nflogRecord

		for _, r := range recs {
			if r.valid() && len(keep) < maxNflogGroups {
				keep = append(keep, r)
			}
		}

		if len(keep) > 0 {
			b.restoredLog[ws] = keep
		}
	}
}

func (b *InMemoryBackend) sweepRestoredNflogLocked(now time.Time) {
	for ws, recs := range b.restoredLog {
		recs = slices.DeleteFunc(recs, func(r nflogRecord) bool { return now.Sub(r.At) > nflogRetention })

		if _, ok := b.workspaces.Get(ws); !ok || len(recs) == 0 {
			delete(b.restoredLog, ws)

			continue
		}

		b.restoredLog[ws] = recs
	}
}
