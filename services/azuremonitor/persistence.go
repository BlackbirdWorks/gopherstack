package azuremonitor

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/azuremonitor/kql"
)

const snapshotVersion = 1

type tableSnap struct {
	Cells [][]any `json:"rows"`
	Table
}

type workspaceSnap struct {
	Workspace
	TableList []tableSnap `json:"tables"`
}

type backendSnapshot struct {
	Workspaces map[string]*workspaceSnap `json:"workspaces"`
	DCEs       map[string]*DCE           `json:"dataCollectionEndpoints"`
	DCRs       map[string]*DCR           `json:"dataCollectionRules"`
	Version    int                       `json:"version"`
}

func encodeCell(t kql.Type, v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return kql.ToString(x)
		}

		return x
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	}

	_ = t

	return v
}

func decodeCell(t kql.Type, v any) (any, error) {
	if v == nil {
		return nil, nil //nolint:nilnil // null cell
	}

	switch t { //nolint:exhaustive // other types decode through the generic converter
	case kql.TInt, kql.TLong:
		if s, ok := v.(string); ok {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("decode %s cell: %w", t, err)
			}

			return n, nil
		}
	case kql.TReal:
		if s, ok := v.(string); ok {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return nil, fmt.Errorf("decode real cell: %w", err)
			}

			return f, nil
		}
	}

	return convertValue(t, v)
}

// Snapshot serialises the backend state. It implements persistence.Persistable.
func (b *Backend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: snapshotVersion, DCEs: b.dces, DCRs: b.dcrs, Workspaces: map[string]*workspaceSnap{},
	}

	for k, w := range b.workspaces {
		ws := &workspaceSnap{Workspace: *w}

		for _, name := range sortedKeys(w.Tables) {
			t := w.Tables[name]
			ts := tableSnap{Table: *t, Cells: make([][]any, len(t.Rows))}

			for i, row := range t.Rows {
				cells := make([]any, len(row))
				for j, v := range row {
					cells[j] = encodeCell(kql.Type(t.Columns[j].Type), v)
				}

				ts.Cells[i] = cells
			}

			ws.TableList = append(ws.TableList, ts)
		}

		snap.Workspaces[k] = ws
	}

	return persistence.MarshalSnapshot(ctx, "azuremonitor", snap)
}

// Restore loads backend state from a snapshot. It implements persistence.Persistable.
func (b *Backend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azuremonitor", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "azuremonitor: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	workspaces := make(map[string]*Workspace, len(snap.Workspaces))

	for k, ws := range snap.Workspaces {
		if ws == nil {
			return fmt.Errorf("%w: null workspace %q", ErrInvalid, k)
		}

		w := ws.Workspace
		w.Tables = make(map[string]*Table, len(ws.TableList))

		for i := range ws.TableList {
			ts := ws.TableList[i]
			t := ts.Table

			rows, err := decodeRows(&t, ts.Cells)
			if err != nil {
				return err
			}

			t.Rows = rows
			w.Tables[t.Name] = &t
		}

		workspaces[k] = &w
	}

	for k, d := range snap.DCEs {
		if d == nil {
			return fmt.Errorf("%w: null data collection endpoint %q", ErrInvalid, k)
		}
	}

	for k, d := range snap.DCRs {
		if d == nil {
			return fmt.Errorf("%w: null data collection rule %q", ErrInvalid, k)
		}
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.workspaces = workspaces
	b.dces = nonNil(snap.DCEs)
	b.dcrs = nonNil(snap.DCRs)

	return nil
}

func nonNil[V any](m map[string]V) map[string]V {
	if m == nil {
		return map[string]V{}
	}

	return m
}

func decodeRows(t *Table, cells [][]any) ([][]any, error) {
	rows := make([][]any, len(cells))

	for i, row := range cells {
		if len(row) != len(t.Columns) {
			return nil, fmt.Errorf("%w: table %q row %d has %d cells for %d columns",
				ErrInvalid, t.Name, i, len(row), len(t.Columns))
		}

		out := make([]any, len(row))

		for j, v := range row {
			cv, err := decodeCell(kql.Type(t.Columns[j].Type), v)
			if err != nil {
				return nil, err
			}

			out[j] = cv
		}

		rows[i] = out
	}

	return rows, nil
}
