package gcpbigtable

import (
	"context"
	"encoding/json/jsontext"
	"fmt"
	"time"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
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
	TS  int64  `json:"ts"`
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

type viewSnap struct {
	View   jsontext.Value `json:"view"`
	Policy jsontext.Value `json:"policy,omitempty"`
}

type changeSnap struct {
	Key       []byte           `json:"key"`
	Mutations []jsontext.Value `json:"mutations"`
	Seq       uint64           `json:"seq"`
	At        int64            `json:"at"`
}

type tableSnap struct {
	Views   map[string]viewSnap       `json:"views,omitempty"`
	Bundles map[string]jsontext.Value `json:"bundles,omitempty"`
	Table   jsontext.Value            `json:"table"`
	Policy  jsontext.Value            `json:"policy,omitempty"`
	Splits  [][]byte                  `json:"splits,omitempty"`
	Rows    []rowSnap                 `json:"rows,omitempty"`
	Changes []changeSnap              `json:"changes,omitempty"`
	ChangeN uint64                    `json:"changeN,omitempty"`
}

type backupSnap struct {
	Backup jsontext.Value `json:"backup"`
	Policy jsontext.Value `json:"policy,omitempty"`
	Table  tableSnap      `json:"table"`
}

type deletedSnap struct {
	Table     tableSnap `json:"table"`
	DeletedAt int64     `json:"deletedAt"`
	CMEK      bool      `json:"cmek,omitempty"`
}

type instSnap struct {
	Deleted  map[string]deletedSnap    `json:"deleted,omitempty"`
	Clusters map[string]jsontext.Value `json:"clusters,omitempty"`
	Profiles map[string]jsontext.Value `json:"profiles,omitempty"`
	Tables   map[string]tableSnap      `json:"tables,omitempty"`
	Backups  map[string]backupSnap     `json:"backups,omitempty"`
	Logical  map[string]jsontext.Value `json:"logicalViews,omitempty"`
	Matviews map[string]jsontext.Value `json:"materializedViews,omitempty"`
	Instance jsontext.Value            `json:"instance"`
	Policy   jsontext.Value            `json:"policy,omitempty"`
}

type backendSnapshot struct {
	Instances  map[string]instSnap       `json:"instances"`
	Operations map[string]jsontext.Value `json:"operations"`
	Version    int                       `json:"version"`
	Seq        uint64                    `json:"seq"`
}

func marshalPB(m proto.Message) jsontext.Value {
	if m == nil || !m.ProtoReflect().IsValid() {
		return nil
	}

	raw, err := protojson.Marshal(m)
	if err != nil {
		return nil
	}

	return raw
}

func snapViews(t *tableRec) map[string]viewSnap {
	if len(t.views) == 0 {
		return nil
	}

	out := make(map[string]viewSnap, len(t.views))
	for id, v := range t.views {
		out[id] = viewSnap{View: marshalPB(v.pb), Policy: marshalPB(v.policy)}
	}

	return out
}

func snapBundles(t *tableRec) map[string]jsontext.Value {
	if len(t.bundles) == 0 {
		return nil
	}

	out := make(map[string]jsontext.Value, len(t.bundles))
	for id, sb := range t.bundles {
		out[id] = marshalPB(sb)
	}

	return out
}

func snapChanges(t *tableRec) []changeSnap {
	out := make([]changeSnap, 0, len(t.changes))

	for _, c := range t.changes {
		cs := changeSnap{Key: []byte(c.key), Seq: c.seq, At: c.at}
		for _, m := range c.muts {
			cs.Mutations = append(cs.Mutations, marshalPB(m))
		}

		out = append(out, cs)
	}

	return out
}

func snapTable(t *tableRec, nowMicros int64) tableSnap {
	s := tableSnap{Table: marshalPB(t.pb), Policy: marshalPB(t.policy), ChangeN: t.changeN}

	s.Views, s.Bundles, s.Changes = snapViews(t), snapBundles(t), snapChanges(t)

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
					cs.Cells = append(cs.Cells, cellSnap{TS: c.ts, Val: c.val})
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

func snapProtoMap[T proto.Message](in map[string]T) map[string]jsontext.Value {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]jsontext.Value, len(in))
	for id, v := range in {
		out[id] = marshalPB(v)
	}

	return out
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
			Clusters: map[string]jsontext.Value{}, Profiles: map[string]jsontext.Value{},
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

		for id, d := range r.deleted {
			if s.Deleted == nil {
				s.Deleted = map[string]deletedSnap{}
			}

			s.Deleted[id] = deletedSnap{Table: snapTable(d.table, now), DeletedAt: d.at.UnixMicro(), CMEK: d.cmek}
		}

		s.Logical, s.Matviews = snapProtoMap(r.logical), snapProtoMap(r.matviews)

		for n, bk := range r.backups {
			s.Backups[n] = backupSnap{
				Backup: marshalPB(bk.pb),
				Policy: marshalPB(bk.policy),
				Table:  snapTable(bk.table, now),
			}
		}

		snap.Instances[name] = s
	}

	return persistence.MarshalSnapshot(ctx, "gcpbigtable", snap)
}

func restoreTable(s tableSnap) (*tableRec, error) {
	pb := &adminpb.Table{}
	if err := unmarshalPB(s.Table, pb); err != nil {
		return nil, fmt.Errorf("table: %w", err)
	}

	t := newTableRec(pb)

	if len(s.Policy) > 0 {
		t.policy = &iampb.Policy{}
		if err := unmarshalPB(s.Policy, t.policy); err != nil {
			return nil, fmt.Errorf("table policy: %w", err)
		}
	}

	for _, sp := range s.Splits {
		t.splits = append(t.splits, string(sp))
	}

	if err := restoreTableExtras(t, s); err != nil {
		return nil, err
	}

	for _, rs := range s.Rows {
		row := t.row(string(rs.Key))
		for _, c := range rs.Columns {
			for _, cl := range c.Cells {
				row.set(c.Family, string(c.Qualifier), cl.TS, cl.Val)
			}
		}
	}

	return t, nil
}

func restoreTableExtras(t *tableRec, s tableSnap) error {
	t.changeN = s.ChangeN

	for id, vs := range s.Views {
		v := &viewRec{pb: &adminpb.AuthorizedView{}}
		if err := unmarshalPB(vs.View, v.pb); err != nil {
			return fmt.Errorf("authorized view %q: %w", id, err)
		}

		if len(vs.Policy) > 0 {
			v.policy = &iampb.Policy{}
			if err := unmarshalPB(vs.Policy, v.policy); err != nil {
				return fmt.Errorf("authorized view %q policy: %w", id, err)
			}
		}

		t.views[id] = v
	}

	for id, raw := range s.Bundles {
		sb := &adminpb.SchemaBundle{}
		if err := unmarshalPB(raw, sb); err != nil {
			return fmt.Errorf("schema bundle %q: %w", id, err)
		}

		t.bundles[id] = sb
	}

	for _, cs := range s.Changes {
		c := changeRec{key: string(cs.Key), seq: cs.Seq, at: cs.At}

		for _, raw := range cs.Mutations {
			m := &btpb.Mutation{}
			if err := unmarshalPB(raw, m); err != nil {
				return fmt.Errorf("change record %d: %w", cs.Seq, err)
			}

			c.muts = append(c.muts, m)
		}

		t.changes = append(t.changes, c)
	}

	return nil
}

func restoreInstance(s instSnap) (*instRec, error) {
	pb := &adminpb.Instance{}
	if err := unmarshalPB(s.Instance, pb); err != nil {
		return nil, fmt.Errorf("instance: %w", err)
	}

	rec := newInstRec(pb)

	if len(s.Policy) > 0 {
		rec.policy = &iampb.Policy{}
		if err := unmarshalPB(s.Policy, rec.policy); err != nil {
			return nil, fmt.Errorf("instance policy: %w", err)
		}
	}

	for id, raw := range s.Clusters {
		c := &adminpb.Cluster{}
		if err := unmarshalPB(raw, c); err != nil {
			return nil, fmt.Errorf("cluster %q: %w", id, err)
		}

		rec.clusters[id] = c
	}

	for id, raw := range s.Profiles {
		p := &adminpb.AppProfile{}
		if err := unmarshalPB(raw, p); err != nil {
			return nil, fmt.Errorf("app profile %q: %w", id, err)
		}

		rec.profiles[id] = p
	}

	for id, raw := range s.Logical {
		v := &adminpb.LogicalView{}
		if err := unmarshalPB(raw, v); err != nil {
			return nil, fmt.Errorf("logical view %q: %w", id, err)
		}

		rec.logical[id] = v
	}

	for id, raw := range s.Matviews {
		v := &adminpb.MaterializedView{}
		if err := unmarshalPB(raw, v); err != nil {
			return nil, fmt.Errorf("materialized view %q: %w", id, err)
		}

		rec.matviews[id] = v
	}

	return rec, restoreTablesAndBackups(rec, s)
}

func restoreTablesAndBackups(rec *instRec, s instSnap) error {
	for id, ds := range s.Deleted {
		t, err := restoreTable(ds.Table)
		if err != nil {
			return fmt.Errorf("deleted table %q: %w", id, err)
		}

		rec.deleted[id] = &deletedTable{table: t, at: time.UnixMicro(ds.DeletedAt), size: tableBytes(t), cmek: ds.CMEK}
	}

	for id, ts := range s.Tables {
		t, err := restoreTable(ts)
		if err != nil {
			return fmt.Errorf("table %q: %w", id, err)
		}

		rec.tables[id] = t
	}

	for n, bs := range s.Backups {
		pb := &adminpb.Backup{}
		if err := unmarshalPB(bs.Backup, pb); err != nil {
			return fmt.Errorf("backup %q: %w", n, err)
		}

		t, err := restoreTable(bs.Table)
		if err != nil {
			return fmt.Errorf("backup %q table: %w", n, err)
		}

		bk := &backupRec{pb: pb, table: t}

		if len(bs.Policy) > 0 {
			bk.policy = &iampb.Policy{}
			if err = unmarshalPB(bs.Policy, bk.policy); err != nil {
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

func unmarshalPB(raw []byte, m proto.Message) error {
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, m)
}
