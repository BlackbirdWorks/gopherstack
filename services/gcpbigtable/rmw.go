package gcpbigtable

import (
	"encoding/binary"
	"slices"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

const int64Len = 8

// ReadModifyWriteRow applies append and increment rules to a row and returns the cells it wrote.
func (b *InMemoryBackend) ReadModifyWriteRow(
	req *btpb.ReadModifyWriteRowRequest,
) (*btpb.ReadModifyWriteRowResponse, error) {
	if err := validateRowKey(req.GetRowKey()); err != nil {
		return nil, err
	}

	if len(req.GetRules()) == 0 {
		return nil, errInvalid("At least one rule is required.")
	}

	if len(req.GetRules()) > maxMutations {
		return nil, errInvalid("At most %d rules are allowed.", maxMutations)
	}

	b.mu.Lock("ReadModifyWriteRow")
	defer b.mu.Unlock()

	t, err := b.dataTable(req.GetTableName(), req.GetAuthorizedViewName(), req.GetAppProfileId(), true)
	if err != nil {
		return nil, err
	}

	key := string(req.GetRowKey())

	var existing *rowRec
	if r, ok := t.rows[key]; ok {
		existing = r
	}

	now := b.nowMicros()
	staged, err := stageRules(t, existing, req.GetRules(), now-now%t.gran())
	if err != nil {
		return nil, err
	}

	row := t.row(key)
	out := &btpb.Row{Key: req.GetRowKey()}

	for _, fam := range sortedKeys(staged) {
		pf := &btpb.Family{Name: fam}

		for _, q := range sortedKeys(staged[fam]) {
			c := staged[fam][q]
			row.set(fam, q, c.ts, c.val)
			pf.Columns = append(pf.Columns, &btpb.Column{
				Qualifier: []byte(q),
				Cells:     []*btpb.Cell{{TimestampMicros: c.ts, Value: slices.Clone(c.val)}},
			})
		}

		out.Families = append(out.Families, pf)
	}

	t.compactRow(key, row, now)

	return &btpb.ReadModifyWriteRowResponse{Row: out}, nil
}

// stageRules computes the new latest cell of every touched column without mutating the table.
func stageRules(
	t *tableRec, existing *rowRec, rules []*btpb.ReadModifyWriteRule, serverTS int64,
) (map[string]map[string]cell, error) {
	staged := map[string]map[string]cell{}

	for _, rule := range rules {
		fam, q := rule.GetFamilyName(), string(rule.GetColumnQualifier())
		if err := t.checkFamily(fam); err != nil {
			return nil, err
		}

		if len(q) > maxQualLen {
			return nil, errInvalid("Column qualifier is too long (maximum %d bytes).", maxQualLen)
		}

		prev, has := staged[fam][q]
		if !has && existing != nil {
			if cs := existing.fams[fam][q]; len(cs) > 0 {
				prev, has = cs[0], true
			}
		}

		ts := serverTS
		if has {
			ts = max(ts, prev.ts)
		}

		next, err := applyRule(rule, prev, has, ts)
		if err != nil {
			return nil, err
		}

		if staged[fam] == nil {
			staged[fam] = map[string]cell{}
		}

		staged[fam][q] = next
	}

	return staged, nil
}

func applyRule(rule *btpb.ReadModifyWriteRule, prev cell, has bool, ts int64) (cell, error) {
	switch rt := rule.GetRule().(type) {
	case *btpb.ReadModifyWriteRule_AppendValue:
		val := make([]byte, 0, len(prev.val)+len(rt.AppendValue))
		val = append(val, prev.val...)
		val = append(val, rt.AppendValue...)

		if len(val) > maxCellBytes {
			return cell{}, errInvalid("Cell value is too large (maximum %d bytes).", maxCellBytes)
		}

		return cell{ts: ts, val: val}, nil
	case *btpb.ReadModifyWriteRule_IncrementAmount:
		var cur int64

		if has {
			if len(prev.val) != int64Len {
				return cell{}, errPrecondition("Increment requires an existing 64-bit big-endian value, found %d bytes.",
					len(prev.val))
			}

			cur = int64(binary.BigEndian.Uint64(prev.val)) //nolint:gosec // two's complement reinterpretation
		}

		val := make([]byte, int64Len)
		binary.BigEndian.PutUint64(val, uint64(cur+rt.IncrementAmount)) //nolint:gosec // two's complement wrap

		return cell{ts: ts, val: val}, nil
	default:
		return cell{}, errInvalid("Rule for %s:%q has neither append_value nor increment_amount.",
			rule.GetFamilyName(), rule.GetColumnQualifier())
	}
}
