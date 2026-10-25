package gcpbigtable

import (
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxFamilies        = 100
	defaultTablePage   = 100
	minChangeStream    = 24 * time.Hour
	maxChangeStream    = 7 * 24 * time.Hour
	minBackupRetention = 3 * 24 * time.Hour
	maxBackupRetention = 90 * 24 * time.Hour
	dayDuration        = 24 * time.Hour
)

func validateFamily(id string, cf *adminpb.ColumnFamily) error {
	if !tableIDRe.MatchString(id) || len(id) > maxFamilyIDLen {
		return errInvalid("Invalid column family id %q: must match [_a-zA-Z0-9][-_.a-zA-Z0-9]*.", id)
	}

	if cf.GetGcRule().GetRule() == nil {
		cf.GcRule = nil
	}

	if cf.GetGcRule() != nil {
		if err := validateGC(cf.GetGcRule(), 0); err != nil {
			return err
		}
	}

	if cf.GetValueType().GetAggregateType() != nil {
		return errUnimplemented("Aggregate column families are not supported by this emulator.")
	}

	return nil
}

func (b *InMemoryBackend) tableLocked(name string) (*instRec, *tableRec, string, error) {
	inst, id, err := parseTable(name)
	if err != nil {
		return nil, nil, "", err
	}

	rec, err := b.instanceLocked(inst)
	if err != nil {
		return nil, nil, "", err
	}

	t, ok := rec.tables[id]
	if !ok {
		return nil, nil, "", errTableNotFound(name)
	}

	return rec, t, id, nil
}

// tableView renders a table for a Table.View.
func tableView(rec *instRec, t *tableRec, view adminpb.Table_View, def adminpb.Table_View) *adminpb.Table {
	if view == adminpb.Table_VIEW_UNSPECIFIED {
		view = def
	}

	out := &adminpb.Table{Name: t.pb.GetName()}

	if view == adminpb.Table_NAME_ONLY {
		return out
	}

	if view == adminpb.Table_SCHEMA_VIEW || view == adminpb.Table_FULL {
		full := clone(t.pb)
		full.ClusterStates = nil

		if p := full.GetAutomatedBackupPolicy(); p != nil && !p.GetDisabled() {
			full.EffectiveAutomatedBackupPolicy = clone(p)
		}

		out = full
	}

	if view == adminpb.Table_REPLICATION_VIEW || view == adminpb.Table_ENCRYPTION_VIEW || view == adminpb.Table_FULL {
		out.ClusterStates = make(map[string]*adminpb.Table_ClusterState, len(rec.clusters))
		for id, c := range rec.clusters {
			out.ClusterStates[id] = &adminpb.Table_ClusterState{
				ReplicationState: adminpb.Table_ClusterState_READY,
				EncryptionInfo:   []*adminpb.EncryptionInfo{encryptionInfo(c)},
			}
		}
	}

	return out
}

// CreateTable creates a table with its column families and split points.
func (b *InMemoryBackend) CreateTable(in *adminpb.CreateTableRequest) (*adminpb.Table, error) {
	id := in.GetTableId()
	if !tableIDRe.MatchString(id) || len(id) > maxTableIDLen {
		return nil, errInvalid(
			"Invalid table id %q: must match [_a-zA-Z0-9][-_.a-zA-Z0-9]* and be at most %d characters.",
			id,
			maxTableIDLen,
		)
	}

	pb := clone(in.GetTable())
	if pb == nil {
		pb = &adminpb.Table{}
	}

	if err := validateNewTable(pb); err != nil {
		return nil, err
	}

	splits, err := splitKeys(in.GetInitialSplits())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateTable")
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	if _, exists := rec.tables[id]; exists {
		return nil, errExists("Table %q already exists.", tableName(in.GetParent(), id))
	}

	pb.Name = tableName(in.GetParent(), id)
	pb.ClusterStates, pb.RestoreInfo, pb.EffectiveAutomatedBackupPolicy = nil, nil, nil

	t := newTableRec(pb)
	t.splits = splits
	rec.tables[id] = t

	return tableView(rec, t, adminpb.Table_SCHEMA_VIEW, adminpb.Table_SCHEMA_VIEW), nil
}

func validateNewTable(pb *adminpb.Table) error {
	if pb.GetGranularity() == adminpb.Table_TIMESTAMP_GRANULARITY_UNSPECIFIED {
		pb.Granularity = adminpb.Table_MILLIS
	}

	if len(pb.GetColumnFamilies()) > maxFamilies {
		return errInvalid("At most %d column families are allowed per table.", maxFamilies)
	}

	for id, cf := range pb.GetColumnFamilies() {
		if err := validateFamily(id, cf); err != nil {
			return err
		}

		if cf == nil {
			pb.ColumnFamilies[id] = &adminpb.ColumnFamily{}
		}
	}

	return validateTableConfig(pb)
}

func validateTableConfig(pb *adminpb.Table) error {
	if cs := pb.GetChangeStreamConfig(); cs != nil {
		if rp := cs.GetRetentionPeriod().AsDuration(); rp < minChangeStream || rp > maxChangeStream {
			return errInvalid("change_stream_config.retention_period must be between 1 and 7 days.")
		}
	}

	if p := pb.GetAutomatedBackupPolicy(); p != nil && !p.GetDisabled() {
		if rp := p.GetRetentionPeriod().AsDuration(); rp < minBackupRetention || rp > maxBackupRetention {
			return errInvalid("automated_backup_policy.retention_period must be between 3 and 90 days.")
		}

		if f := p.GetFrequency(); f != nil && f.AsDuration() != dayDuration {
			return errInvalid("automated_backup_policy.frequency must be 24 hours.")
		}
	}

	return nil
}

func splitKeys(in []*adminpb.CreateTableRequest_Split) ([]string, error) {
	out := make([]string, 0, len(in))

	for _, s := range in {
		if len(s.GetKey()) == 0 {
			return nil, errInvalid("Split keys must not be empty.")
		}

		out = append(out, string(s.GetKey()))
	}

	slices.Sort(out)

	if len(slices.Compact(slices.Clone(out))) != len(out) {
		return nil, errInvalid("Split keys must be unique.")
	}

	return out, nil
}

// GetTable returns a table in the requested view (SCHEMA_VIEW by default).
func (b *InMemoryBackend) GetTable(in *adminpb.GetTableRequest) (*adminpb.Table, error) {
	b.mu.RLock("GetTable")
	defer b.mu.RUnlock()

	rec, t, _, err := b.tableLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	return tableView(rec, t, in.GetView(), adminpb.Table_SCHEMA_VIEW), nil
}

// ListTables lists the tables of an instance (NAME_ONLY by default).
func (b *InMemoryBackend) ListTables(in *adminpb.ListTablesRequest) (*adminpb.ListTablesResponse, error) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListTables")
	defer b.mu.RUnlock()

	rec, err := b.instanceLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	all := make([]*adminpb.Table, 0, len(rec.tables))
	for _, id := range collections.SortedKeys(rec.tables) {
		all = append(all, tableView(rec, rec.tables[id], in.GetView(), adminpb.Table_NAME_ONLY))
	}

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultTablePage)

	return &adminpb.ListTablesResponse{Tables: pg.Data, NextPageToken: pg.Next}, nil
}

// DeleteTable deletes a table unless it is protected.
func (b *InMemoryBackend) DeleteTable(name string) (*emptypb.Empty, error) {
	b.mu.Lock("DeleteTable")
	defer b.mu.Unlock()

	rec, t, id, err := b.tableLocked(name)
	if err != nil {
		return nil, err
	}

	if t.pb.GetDeletionProtection() {
		return nil, errPrecondition("Table %q has deletion protection enabled.", name)
	}

	delete(rec.tables, id)
	delete(rec.tokens, name)

	return &emptypb.Empty{}, nil
}

// UpdateTable updates the masked table-level settings.
func (b *InMemoryBackend) UpdateTable(in *adminpb.UpdateTableRequest) (*longrunningpb.Operation, error) {
	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("update_mask must be set.")
	}

	for _, p := range paths {
		top, _, _ := strings.Cut(p, ".")
		if !slices.Contains(updatableTableFields, top) {
			return nil, errInvalid("Table field %q cannot be updated.", p)
		}
	}

	b.mu.Lock("UpdateTable")
	defer b.mu.Unlock()

	rec, t, _, err := b.tableLocked(in.GetTable().GetName())
	if err != nil {
		return nil, err
	}

	next := clone(t.pb)

	if err = applyMask(next, in.GetTable(), paths); err != nil {
		return nil, err
	}

	if err = validateTableConfig(next); err != nil {
		return nil, err
	}

	t.pb = next

	return b.finish(rec.pb.GetName(), &adminpb.UpdateTableMetadata{
		Name: next.GetName(), StartTime: b.ts(), EndTime: b.ts(),
	}, tableView(rec, t, adminpb.Table_SCHEMA_VIEW, adminpb.Table_SCHEMA_VIEW))
}

var updatableTableFields = []string{ //nolint:gochecknoglobals // immutable catalog
	"deletion_protection", "change_stream_config", "automated_backup_policy", "row_key_schema", "tiered_storage_config",
}

// ModifyColumnFamilies creates, updates and drops column families atomically.
func (b *InMemoryBackend) ModifyColumnFamilies(in *adminpb.ModifyColumnFamiliesRequest) (*adminpb.Table, error) {
	if len(in.GetModifications()) == 0 {
		return nil, errInvalid("At least one modification is required.")
	}

	b.mu.Lock("ModifyColumnFamilies")
	defer b.mu.Unlock()

	rec, t, _, err := b.tableLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	next := clone(t.pb)
	if next.ColumnFamilies == nil {
		next.ColumnFamilies = map[string]*adminpb.ColumnFamily{}
	}

	seen := map[string]bool{}
	dropped := []string{}

	for _, m := range in.GetModifications() {
		if seen[m.GetId()] {
			return nil, errInvalid("Column family %q is modified more than once.", m.GetId())
		}

		seen[m.GetId()] = true

		drop, merr := applyModification(next, m, t.pb.GetDeletionProtection())
		if merr != nil {
			return nil, merr
		}

		if drop {
			dropped = append(dropped, m.GetId())
		}
	}

	if len(next.GetColumnFamilies()) > maxFamilies {
		return nil, errInvalid("At most %d column families are allowed per table.", maxFamilies)
	}

	t.pb = next

	for _, f := range dropped {
		t.dropFamily(f)
	}

	t.compact(b.nowMicros())

	return tableView(rec, t, adminpb.Table_SCHEMA_VIEW, adminpb.Table_SCHEMA_VIEW), nil
}

func applyModification(
	tbl *adminpb.Table, m *adminpb.ModifyColumnFamiliesRequest_Modification, protected bool,
) (bool, error) {
	id := m.GetId()
	cur, exists := tbl.GetColumnFamilies()[id]

	switch mod := m.GetMod().(type) {
	case *adminpb.ModifyColumnFamiliesRequest_Modification_Create:
		if exists {
			return false, errExists("Column family %q already exists.", id)
		}

		cf := mod.Create
		if cf == nil {
			cf = &adminpb.ColumnFamily{}
		}

		if err := validateFamily(id, cf); err != nil {
			return false, err
		}

		tbl.ColumnFamilies[id] = clone(cf)
	case *adminpb.ModifyColumnFamiliesRequest_Modification_Update:
		if !exists {
			return false, errNotFound("Column family %q not found.", id)
		}

		return false, updateFamily(cur, mod.Update, m.GetUpdateMask().GetPaths(), id)
	case *adminpb.ModifyColumnFamiliesRequest_Modification_Drop:
		if !mod.Drop {
			return false, errInvalid("drop must be true when set.")
		}

		if !exists {
			return false, errNotFound("Column family %q not found.", id)
		}

		if protected {
			return false, errPrecondition("Table has deletion protection enabled; column families cannot be dropped.")
		}

		delete(tbl.GetColumnFamilies(), id)

		return true, nil
	default:
		return false, errInvalid("Modification of column family %q has no create, update or drop.", id)
	}

	return false, nil
}

func updateFamily(cur, src *adminpb.ColumnFamily, paths []string, id string) error {
	if src == nil {
		src = &adminpb.ColumnFamily{}
	}

	if len(paths) == 0 {
		paths = []string{"gc_rule"}
	}

	if err := validateFamily(id, src); err != nil {
		return err
	}

	for _, p := range paths {
		switch p {
		case "gc_rule":
			cur.GcRule = clone(src.GetGcRule())
		case "value_type":
			if cur.GetValueType().GetAggregateType() != nil {
				return errInvalid("Immutable fields 'value_type.aggregate_type' cannot be updated.")
			}

			cur.ValueType = clone(src.GetValueType())
		default:
			return errInvalid("Unsupported update_mask path %q for ColumnFamily.", p)
		}
	}

	return nil
}

// DropRowRange deletes the rows with a key prefix, or all rows.
func (b *InMemoryBackend) DropRowRange(in *adminpb.DropRowRangeRequest) (*emptypb.Empty, error) {
	b.mu.Lock("DropRowRange")
	defer b.mu.Unlock()

	_, t, _, err := b.tableLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	switch tg := in.GetTarget().(type) {
	case *adminpb.DropRowRangeRequest_RowKeyPrefix:
		if len(tg.RowKeyPrefix) == 0 {
			return nil, errInvalid("row_key_prefix must not be empty.")
		}

		for k := range t.rows {
			if strings.HasPrefix(k, string(tg.RowKeyPrefix)) {
				t.delRow(k)
			}
		}
	case *adminpb.DropRowRangeRequest_DeleteAllDataFromTable:
		if !tg.DeleteAllDataFromTable {
			return nil, errInvalid("delete_all_data_from_table must be true when set.")
		}

		t.rows, t.keys, t.dirty = make(map[string]*rowRec), nil, false
	default:
		return nil, errInvalid("One of row_key_prefix or delete_all_data_from_table is required.")
	}

	return &emptypb.Empty{}, nil
}

// GenerateConsistencyToken issues a token for CheckConsistency.
func (b *InMemoryBackend) GenerateConsistencyToken(
	in *adminpb.GenerateConsistencyTokenRequest,
) (*adminpb.GenerateConsistencyTokenResponse, error) {
	b.mu.Lock("GenerateConsistencyToken")
	defer b.mu.Unlock()

	rec, _, _, err := b.tableLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	tok := uuid.NewString()
	if rec.tokens[in.GetName()] == nil {
		rec.tokens[in.GetName()] = map[string]bool{}
	}

	rec.tokens[in.GetName()][tok] = true

	return &adminpb.GenerateConsistencyTokenResponse{ConsistencyToken: tok}, nil
}

// CheckConsistency reports whether all writes before the token reached every cluster; replication is synchronous here.
func (b *InMemoryBackend) CheckConsistency(
	in *adminpb.CheckConsistencyRequest,
) (*adminpb.CheckConsistencyResponse, error) {
	b.mu.RLock("CheckConsistency")
	defer b.mu.RUnlock()

	rec, _, _, err := b.tableLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	if !rec.tokens[in.GetName()][in.GetConsistencyToken()] {
		return nil, errInvalid("Invalid consistency token %q.", in.GetConsistencyToken())
	}

	return &adminpb.CheckConsistencyResponse{Consistent: true}, nil
}

func encryptionInfo(c *adminpb.Cluster) *adminpb.EncryptionInfo {
	key := c.GetEncryptionConfig().GetKmsKeyName()
	if key == "" {
		return &adminpb.EncryptionInfo{EncryptionType: adminpb.EncryptionInfo_GOOGLE_DEFAULT_ENCRYPTION}
	}

	return &adminpb.EncryptionInfo{
		EncryptionType:   adminpb.EncryptionInfo_CUSTOMER_MANAGED_ENCRYPTION,
		EncryptionStatus: &status.Status{},
		KmsKeyVersion:    key + "/cryptoKeyVersions/1",
	}
}
