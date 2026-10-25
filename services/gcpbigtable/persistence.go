package gcpbigtable

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type cellSnap struct {
	Val []byte `json:"val,omitempty"`
	Ts  int64  `json:"ts"`
}

type columnSnap struct {
	Family    string     `json:"family"`
	Qualifier []byte     `json:"qualifier,omitempty"`
	Cells     []cellSnap `json:"cells"`
}

type rowSnap struct {
	Key     []byte       `json:"key"`
	Columns []columnSnap `json:"columns"`
}

type tableSnap struct {
	Table  json.RawMessage `json:"table"`
	Policy json.RawMessage `json:"policy,omitempty"`
	Splits [][]byte        `json:"splits,omitempty"`
	Rows   []rowSnap       `json:"rows,omitempty"`
}

type backupSnap struct {
	Backup json.RawMessage `json:"backup"`
	Policy json.RawMessage `json:"policy,omitempty"`
	Table  tableSnap       `json:"table"`
}

type instSnap struct {
	Instance json.RawMessage            `json:"instance"`
	Policy   json.RawMessage            `json:"policy,omitempty"`
	Clusters map[string]json.RawMessage `json:"clusters,omitempty"`
	Profiles map[string]json.RawMessage `json:"profiles,omitempty"`
	Tables   map[string]tableSnap       `json:"tables,omitempty"`
	Backups  map[string]backupSnap      `json:"backups,omitempty"`
}

type backendSnapshot struct {
	Instances  map[string]instSnap        `json:"instances"`
	Operations map[string]json.RawMessage `json:"operations"`
	Version    int                        `json:"version"`
	Seq        uint64                     `json:"seq"`
}

var (
	marshalOpts   = protojson.MarshalOptions{}
	unmarshalOpts = protojson.UnmarshalOptions{DiscardUnknown: true}
)

func marshalPB(m proto.Message) json.RawMessage {
	if m == nil || !m.ProtoReflect().IsValid() {
		return nil
	}

	raw, err := marshalOpts.Marshal(m)
	if err != nil {
		return nil
	}

	return raw
}

func snapTable(t *tableRec, nowMicros int64) tableSnap {
	s := tableSnap{Table: marshalPB(t.pb), Policy: marshalPB(t.policy)}

	for _, sp := range t.splits {
		s.Splits = append(s.Splits, []byte(sp))
	}

	for _, key := range t.sortedKeys() {
		rs := rowSnap{Key: []byte(key)}
		r := t.rows[key]

		for _, fam := range sortedKeys(r.fams) {
			rule := t.pb.GetColumnFamilies()[fam].GetGcRule()

			for _, q := range sortedKeys(r.fams[fam]) {
				cs := columnSnap{Family: fam, Qualifier: []byte(q)}
				for _, c := range live(rule, r.fams[fam][q], nowMicros) {
					cs.Cells = append(cs.Cells, cellSnap{Ts: c.ts, Val: c.val})
				}

				if len(cs.Cells) > 0 {
					rs.Columns = append(rs.Columns, cs)
				}
			}
		}

		if len(rs.Columns) > 0 {
			s.Rows = append(s.Rows, rs)
		}
	}

	return s
}

// Snapshot serialises every instance with its clusters, profiles, tables, rows, backups and operations.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.Lock("Snapshot")
	defer b.mu.Unlock()

	now := b.nowMicros()
	snap := backendSnapshot{
		Version: snapshotVersion, Seq: b.seq,
		Instances:  make(map[string]instSnap, len(b.instances)),
		Operations: b.ops.Snapshot(),
	}

	for name, r := range b.instances {
		s := instSnap{
			Instance: marshalPB(r.pb), Policy: marshalPB(r.policy),
			Clusters: map[string]json.RawMessage{}, Profiles: map[string]json.RawMessage{},
			Tables: map[string]tableSnap{}, Backups: map[string]backupSnap{},
		}

		for id, c := range r.clusters {
			s.Clusters[id] = marshalPB(c)
		}

		for id, p := range r.profiles {
			s.Profiles[id] = marshalPB(p)
		}

		for id, t := range r.tables {
			s.Tables[id] = snapTable(t, now)
		}

		for n, bk := range r.backups {
			s.Backups[n] = backupSnap{Backup: marshalPB(bk.pb), Policy: marshalPB(bk.policy), Table: snapTable(bk.table, now)}
		}

		snap.Instances[name] = s
	}

	return persistence.MarshalSnapshot(ctx, "gcpbigtable", snap)
}

func restoreTable(s tableSnap) (*tableRec, error) {
	pb := &adminpb.Table{}
	if err := unmarshalOpts.Unmarshal(s.Table, pb); err != nil {
		return nil, fmt.Errorf("table: %w", err)
	}

	t := newTableRec(pb)

	if len(s.Policy) > 0 {
		t.policy = &iampb.Policy{}
		if err := unmarshalOpts.Unmarshal(s.Policy, t.policy); err != nil {
			return nil, fmt.Errorf("table policy: %w", err)
		}
	}

	for _, sp := range s.Splits {
		t.splits = append(t.splits, string(sp))
	}

	for _, rs := range s.Rows {
		row := t.row(string(rs.Key))
		for _, c := range rs.Columns {
			for _, cl := range c.Cells {
				row.set(c.Family, string(c.Qualifier), cl.Ts, cl.Val)
			}
		}
	}

	return t, nil
}

func restoreInstance(s instSnap) (*instRec, error) {
	pb := &adminpb.Instance{}
	if err := unmarshalOpts.Unmarshal(s.Instance, pb); err != nil {
		return nil, fmt.Errorf("instance: %w", err)
	}

	rec := newInstRec(pb)

	if len(s.Policy) > 0 {
		rec.policy = &iampb.Policy{}
		if err := unmarshalOpts.Unmarshal(s.Policy, rec.policy); err != nil {
			return nil, fmt.Errorf("instance policy: %w", err)
		}
	}

	for id, raw := range s.Clusters {
		c := &adminpb.Cluster{}
		if err := unmarshalOpts.Unmarshal(raw, c); err != nil {
			return nil, fmt.Errorf("cluster %q: %w", id, err)
		}

		rec.clusters[id] = c
	}

	for id, raw := range s.Profiles {
		p := &adminpb.AppProfile{}
		if err := unmarshalOpts.Unmarshal(raw, p); err != nil {
			return nil, fmt.Errorf("app profile %q: %w", id, err)
		}

		rec.profiles[id] = p
	}

	return rec, restoreTablesAndBackups(rec, s)
}

func restoreTablesAndBackups(rec *instRec, s instSnap) error {
	for id, ts := range s.Tables {
		t, err := restoreTable(ts)
		if err != nil {
			return fmt.Errorf("table %q: %w", id, err)
		}

		rec.tables[id] = t
	}

	for n, bs := range s.Backups {
		pb := &adminpb.Backup{}
		if err := unmarshalOpts.Unmarshal(bs.Backup, pb); err != nil {
			return fmt.Errorf("backup %q: %w", n, err)
		}

		t, err := restoreTable(bs.Table)
		if err != nil {
			return fmt.Errorf("backup %q table: %w", n, err)
		}

		bk := &backupRec{pb: pb, table: t}

		if len(bs.Policy) > 0 {
			bk.policy = &iampb.Policy{}
			if err = unmarshalOpts.Unmarshal(bs.Policy, bk.policy); err != nil {
				return fmt.Errorf("backup %q policy: %w", n, err)
			}
		}

		rec.backups[n] = bk
	}

	return nil
}

// Restore loads a snapshot, replacing all state.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpbigtable", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpbigtable: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	recs := make(map[string]*instRec, len(snap.Instances))

	for name, s := range snap.Instances {
		rec, err := restoreInstance(s)
		if err != nil {
			return fmt.Errorf("gcpbigtable: restore %q: %w", name, err)
		}

		recs[name] = rec
	}

	if err := b.ops.Restore(snap.Operations); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	b.instances, b.seq = recs, snap.Seq
	b.mu.Unlock()

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpbigtable: restore snapshot: %w", err)
	}

	return nil
}
