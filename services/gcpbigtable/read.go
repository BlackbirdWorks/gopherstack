package gcpbigtable

import (
	"bytes"
	"slices"
	"sort"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// ReadResult is the outcome of one ReadRows call.
type ReadResult struct {
	Rows  []*ResultRow
	Stats *btpb.ReadIterationStats
}

// ResultRow is one scanned row; a row the filter emptied has no cells.
type ResultRow struct {
	Key   string
	Cells []fcell
}

func validateRowSet(rs *btpb.RowSet) error {
	for i, rr := range rs.GetRowRanges() {
		var sc, so, ec, eo []byte

		switch s := rr.GetStartKey().(type) {
		case *btpb.RowRange_StartKeyClosed:
			sc = s.StartKeyClosed
		case *btpb.RowRange_StartKeyOpen:
			so = s.StartKeyOpen
		}

		switch e := rr.GetEndKey().(type) {
		case *btpb.RowRange_EndKeyClosed:
			ec = e.EndKeyClosed
		case *btpb.RowRange_EndKeyOpen:
			eo = e.EndKeyOpen
		}

		if msg := badRange(sc, so, ec, eo); msg != "" {
			return errInvalid("Error in element #%d of row_ranges: %s", i, msg)
		}
	}

	return nil
}

func outOfOrder(start, end []byte) bool {
	return len(start) != 0 && len(end) != 0 && bytes.Compare(start, end) > 0
}

func badRange(sc, so, ec, eo []byte) string {
	switch {
	case outOfOrder(sc, ec):
		return "start_key_closed must be less than end_key_closed"
	case outOfOrder(sc, eo):
		return "start_key_closed must be less than end_key_open"
	case outOfOrder(so, ec):
		return "start_key_open must be less than end_key_closed"
	case outOfOrder(so, eo):
		return "start_key_open must be less than end_key_open"
	}

	return ""
}

// candidateKeys returns the sorted, de-duplicated row keys selected by rs (all keys when rs selects nothing).
func candidateKeys(t *tableRec, rs *btpb.RowSet) []string {
	all := t.sortedKeys()
	if len(rs.GetRowKeys()) == 0 && len(rs.GetRowRanges()) == 0 {
		return all
	}

	var out []string

	for _, k := range rs.GetRowKeys() {
		if _, ok := t.rows[string(k)]; ok {
			out = append(out, string(k))
		}
	}

	for _, rr := range rs.GetRowRanges() {
		lo, hi := 0, len(all)

		switch s := rr.GetStartKey().(type) {
		case *btpb.RowRange_StartKeyClosed:
			lo = sort.SearchStrings(all, string(s.StartKeyClosed))
		case *btpb.RowRange_StartKeyOpen:
			lo = sort.SearchStrings(all, string(s.StartKeyOpen)+"\x00")
		}

		switch e := rr.GetEndKey().(type) {
		case *btpb.RowRange_EndKeyClosed:
			hi = sort.SearchStrings(all, string(e.EndKeyClosed)+"\x00")
		case *btpb.RowRange_EndKeyOpen:
			hi = sort.SearchStrings(all, string(e.EndKeyOpen))
		}

		if lo < hi {
			out = append(out, all[lo:hi]...)
		}
	}

	sort.Strings(out)

	return slices.Compact(out)
}

// ReadRows evaluates a read: row selection, filter, reversal and the row limit.
func (b *InMemoryBackend) ReadRows(req *btpb.ReadRowsRequest) (*ReadResult, error) {
	if req.GetRowsLimit() < 0 {
		return nil, errInvalid("rows_limit must not be negative.")
	}

	if err := validateRowSet(req.GetRows()); err != nil {
		return nil, err
	}

	fn, err := compileFilter(req.GetFilter())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("ReadRows")
	defer b.mu.Unlock()

	t, err := b.dataTable(req.GetTableName(), req.GetAuthorizedViewName(), req.GetAppProfileId(), false)
	if err != nil {
		return nil, err
	}

	keys := candidateKeys(t, req.GetRows())
	if req.GetReversed() {
		keys = slices.Clone(keys)
		slices.Reverse(keys)
	}

	now := b.nowMicros()
	res := &ReadResult{Stats: &btpb.ReadIterationStats{}}

	for _, k := range keys {
		if limit := int64(req.GetRowsLimit()); limit > 0 && res.Stats.RowsReturnedCount >= limit {
			break
		}

		cells := t.liveCells(t.rows[k], now)
		if len(cells) == 0 {
			continue
		}

		res.Stats.RowsSeenCount++
		res.Stats.CellsSeenCount += int64(len(cells))

		kept := runFilter(fn, k, cells)
		if len(kept) == 0 {
			res.Rows = append(res.Rows, &ResultRow{Key: k})

			continue
		}

		res.Stats.RowsReturnedCount++
		res.Stats.CellsReturnedCount += int64(len(kept))
		res.Rows = append(res.Rows, &ResultRow{Key: k, Cells: kept})
	}

	return res, nil
}

// SampleRowKeys returns approximate split points with the cumulative size of the rows before each.
func (b *InMemoryBackend) SampleRowKeys(req *btpb.SampleRowKeysRequest) ([]*btpb.SampleRowKeysResponse, error) {
	b.mu.Lock("SampleRowKeys")
	defer b.mu.Unlock()

	t, err := b.dataTable(req.GetTableName(), req.GetAuthorizedViewName(), req.GetAppProfileId(), false)
	if err != nil {
		return nil, err
	}

	keys := t.sortedKeys()
	out := make([]*btpb.SampleRowKeysResponse, 0, len(t.splits)+1)

	var offset int64

	pos := 0

	for _, split := range t.splits {
		for pos < len(keys) && keys[pos] < split {
			offset += t.rows[keys[pos]].size()
			pos++
		}

		out = append(out, &btpb.SampleRowKeysResponse{RowKey: []byte(split), OffsetBytes: offset})
	}

	for ; pos < len(keys); pos++ {
		offset += t.rows[keys[pos]].size()
	}

	return append(out, &btpb.SampleRowKeysResponse{OffsetBytes: offset}), nil
}

// PingAndWarm checks that the instance exists.
func (b *InMemoryBackend) PingAndWarm(req *btpb.PingAndWarmRequest) error {
	b.mu.RLock("PingAndWarm")
	defer b.mu.RUnlock()

	_, err := b.instanceLocked(req.GetName())

	return err
}
