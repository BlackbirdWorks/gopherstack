package gcpbigtable

import (
	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxMutations = 100000
	maxCellBytes = 100 << 20
	serverTime   = -1
	defaultApp   = "default"
)

// dataTable resolves a data-plane table and checks the app profile; the caller holds b.mu.
func (b *InMemoryBackend) dataTable(name, authorizedView, appProfile string, transactional bool) (*tableRec, error) {
	if authorizedView != "" {
		return nil, errUnimplemented("Authorized views are not supported by this emulator.")
	}

	rec, t, _, err := b.tableLocked(name)
	if err != nil {
		return nil, err
	}

	if appProfile == "" || appProfile == defaultApp {
		return t, nil
	}

	pr, ok := rec.profiles[appProfile]
	if !ok {
		return nil, errNotFound("App profile %q not found.", profileName(rec.pb.GetName(), appProfile))
	}

	if transactional && !allowsTransactions(pr) {
		return nil, errPrecondition("App profile %q does not allow single-row transactions.", appProfile)
	}

	return t, nil
}

func allowsTransactions(p *adminpb.AppProfile) bool {
	return p.GetSingleClusterRouting().GetAllowTransactionalWrites()
}

func validateRowKey(key []byte) error {
	if len(key) == 0 {
		return errInvalid("Row keys must be non-empty.")
	}

	if len(key) > maxKeyLen {
		return errInvalid("Row key is too long: %d bytes (maximum %d).", len(key), maxKeyLen)
	}

	return nil
}

func (t *tableRec) checkFamily(fam string) error {
	if _, ok := t.pb.GetColumnFamilies()[fam]; !ok {
		return status.Errorf(codes.NotFound, "Requested column family not found: %q.", fam)
	}

	return nil
}

func (t *tableRec) validateMutations(muts []*btpb.Mutation) error {
	if len(muts) == 0 {
		return errInvalid("At least one mutation is required.")
	}

	if len(muts) > maxMutations {
		return errInvalid("At most %d mutations are allowed per row.", maxMutations)
	}

	for _, m := range muts {
		if err := t.validateMutation(m); err != nil {
			return err
		}
	}

	return nil
}

func (t *tableRec) validateMutation(m *btpb.Mutation) error {
	switch mt := m.GetMutation().(type) {
	case *btpb.Mutation_SetCell_:
		sc := mt.SetCell
		if err := t.checkFamily(sc.GetFamilyName()); err != nil {
			return err
		}

		if len(sc.GetColumnQualifier()) > maxQualLen {
			return errInvalid("Column qualifier is too long (maximum %d bytes).", maxQualLen)
		}

		if len(sc.GetValue()) > maxCellBytes {
			return errInvalid("Cell value is too large (maximum %d bytes).", maxCellBytes)
		}

		ts := sc.GetTimestampMicros()
		if ts < serverTime || (ts != serverTime && ts%t.gran() != 0) {
			return errInvalid("Timestamp granularity mismatch. Expected a multiple of %d microseconds, got %d.", t.gran(), ts)
		}
	case *btpb.Mutation_DeleteFromColumn_:
		dc := mt.DeleteFromColumn
		if err := t.checkFamily(dc.GetFamilyName()); err != nil {
			return err
		}

		tr := dc.GetTimeRange()
		if tr.GetStartTimestampMicros() < 0 || tr.GetEndTimestampMicros() < 0 ||
			(tr.GetEndTimestampMicros() != 0 && tr.GetStartTimestampMicros() >= tr.GetEndTimestampMicros()) {
			return errInvalid("Invalid time range in DeleteFromColumn.")
		}
	case *btpb.Mutation_DeleteFromFamily_:
		return t.checkFamily(mt.DeleteFromFamily.GetFamilyName())
	case *btpb.Mutation_DeleteFromRow_:
	case *btpb.Mutation_AddToCell_, *btpb.Mutation_MergeToCell_:
		return errUnimplemented("Aggregate mutations are not supported by this emulator.")
	default:
		return errInvalid("Mutation has no operation set.")
	}

	return nil
}

// applyMutations applies validated mutations to a row and runs GC on it.
func (b *InMemoryBackend) applyMutations(t *tableRec, key string, muts []*btpb.Mutation) {
	now := b.nowMicros()
	serverTS := now - now%t.gran()

	for _, m := range muts {
		switch mt := m.GetMutation().(type) {
		case *btpb.Mutation_SetCell_:
			sc := mt.SetCell

			ts := sc.GetTimestampMicros()
			if ts == serverTime {
				ts = serverTS
			}

			t.row(key).set(sc.GetFamilyName(), string(sc.GetColumnQualifier()), ts, append([]byte(nil), sc.GetValue()...))
		case *btpb.Mutation_DeleteFromColumn_:
			dc := mt.DeleteFromColumn
			if r, ok := t.rows[key]; ok {
				tr := dc.GetTimeRange()
				r.deleteRange(dc.GetFamilyName(), string(dc.GetColumnQualifier()),
					tr.GetStartTimestampMicros(), tr.GetEndTimestampMicros())
			}
		case *btpb.Mutation_DeleteFromFamily_:
			if r, ok := t.rows[key]; ok {
				delete(r.fams, mt.DeleteFromFamily.GetFamilyName())
			}
		case *btpb.Mutation_DeleteFromRow_:
			t.delRow(key)
		}
	}

	if r, ok := t.rows[key]; ok {
		t.compactRow(key, r, now)
	}
}

// MutateRow atomically applies mutations to one row.
func (b *InMemoryBackend) MutateRow(req *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
	if err := validateRowKey(req.GetRowKey()); err != nil {
		return nil, err
	}

	b.mu.Lock("MutateRow")
	defer b.mu.Unlock()

	t, err := b.dataTable(req.GetTableName(), req.GetAuthorizedViewName(), req.GetAppProfileId(), false)
	if err != nil {
		return nil, err
	}

	if err = t.validateMutations(req.GetMutations()); err != nil {
		return nil, err
	}

	b.applyMutations(t, string(req.GetRowKey()), req.GetMutations())

	return &btpb.MutateRowResponse{}, nil
}

// MutateRows applies each entry atomically and independently; the result holds one status per entry.
func (b *InMemoryBackend) MutateRows(req *btpb.MutateRowsRequest) ([]*btpb.MutateRowsResponse_Entry, error) {
	if len(req.GetEntries()) == 0 {
		return nil, errInvalid("At least one entry is required.")
	}

	if len(req.GetEntries()) > maxMutations {
		return nil, errInvalid("At most %d entries are allowed per request.", maxMutations)
	}

	b.mu.Lock("MutateRows")
	defer b.mu.Unlock()

	t, err := b.dataTable(req.GetTableName(), req.GetAuthorizedViewName(), req.GetAppProfileId(), false)
	if err != nil {
		return nil, err
	}

	out := make([]*btpb.MutateRowsResponse_Entry, len(req.GetEntries()))

	for i, e := range req.GetEntries() {
		st := status.New(codes.OK, "")

		if verr := validateRowKey(e.GetRowKey()); verr != nil {
			st = status.Convert(verr)
		} else if verr = t.validateMutations(e.GetMutations()); verr != nil {
			st = status.Convert(verr)
		} else {
			b.applyMutations(t, string(e.GetRowKey()), e.GetMutations())
		}

		out[i] = &btpb.MutateRowsResponse_Entry{Index: int64(i), Status: st.Proto()}
	}

	return out, nil
}

// CheckAndMutateRow applies one of two mutation lists depending on whether the predicate matches the row.
func (b *InMemoryBackend) CheckAndMutateRow(
	req *btpb.CheckAndMutateRowRequest,
) (*btpb.CheckAndMutateRowResponse, error) {
	if err := validateRowKey(req.GetRowKey()); err != nil {
		return nil, err
	}

	if len(req.GetTrueMutations()) == 0 && len(req.GetFalseMutations()) == 0 {
		return nil, errInvalid("At least one of true_mutations or false_mutations is required.")
	}

	pred, err := compileFilter(req.GetPredicateFilter())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CheckAndMutateRow")
	defer b.mu.Unlock()

	t, err := b.dataTable(req.GetTableName(), req.GetAuthorizedViewName(), req.GetAppProfileId(), true)
	if err != nil {
		return nil, err
	}

	for _, muts := range [][]*btpb.Mutation{req.GetTrueMutations(), req.GetFalseMutations()} {
		if len(muts) > 0 {
			if err = t.validateMutations(muts); err != nil {
				return nil, err
			}
		}
	}

	key := string(req.GetRowKey())
	matched := false

	if r, ok := t.rows[key]; ok {
		matched = len(runFilter(pred, key, t.liveCells(r, b.nowMicros()))) > 0
	}

	muts := req.GetFalseMutations()
	if matched {
		muts = req.GetTrueMutations()
	}

	if len(muts) > 0 {
		b.applyMutations(t, key, muts)
	}

	return &btpb.CheckAndMutateRowResponse{PredicateMatched: matched}, nil
}
