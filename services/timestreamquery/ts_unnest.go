package timestreamquery

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"

	"modernc.org/sqlite/vtab"
)

const (
	unnestNoArgCost  = 1e12
	unnestModuleName = "ts_unnest"
	unnestTablePfx   = unnestModuleName + "_"
	unnestTableArg   = 2
)

var (
	errUnnestColumn = errors.New("UNNEST has no such column")
	errUnnestName   = errors.New("malformed ts_unnest table name")
)

// unnestModuleFor returns the per-query virtual table name carrying cols; the single process-wide module decodes them.
func unnestModuleFor(cols []string) string {
	return unnestTablePfx + hex.EncodeToString([]byte(strings.Join(cols, "\x00")))
}

func registerUnnestModule() {
	if err := vtab.RegisterModule(nil, unnestModuleName, unnestModule{}); err != nil {
		panic(err)
	}
}

// unnestModule is a table-valued function over a timeseries (time, value rows), array or object.
type unnestModule struct{}

func (m unnestModule) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Connect(ctx, args)
}

func (unnestModule) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) <= unnestTableArg {
		return nil, errUnnestName
	}

	raw, err := hex.DecodeString(strings.TrimPrefix(args[unnestTableArg], unnestTablePfx))
	if err != nil {
		return nil, errUnnestName
	}

	cols := strings.Split(string(raw), "\x00")

	decl := make([]string, 0, len(cols)+argsTwo)
	for _, c := range cols {
		decl = append(decl, quoteIdent(c))
	}

	decl = append(decl, "_unnest_arg HIDDEN", "_unnest_pos HIDDEN")

	if err = ctx.Declare("CREATE TABLE x(" + strings.Join(decl, ", ") + ")"); err != nil {
		return nil, err
	}

	return &unnestTable{cols: len(cols)}, nil
}

type unnestTable struct{ cols int }

func (t *unnestTable) BestIndex(info *vtab.IndexInfo) error {
	for i, c := range info.Constraints {
		if c.Usable && c.Op == vtab.OpEQ && c.Column == t.cols {
			info.Constraints[i].ArgIndex = 0
			info.Constraints[i].Omit = true
			info.EstimatedCost = 1

			return nil
		}
	}

	info.EstimatedCost = unnestNoArgCost

	return nil
}

func (t *unnestTable) Open() (vtab.Cursor, error) { return &unnestCursor{cols: t.cols}, nil }
func (*unnestTable) Disconnect() error            { return nil }
func (*unnestTable) Destroy() error               { return nil }

type unnestRow struct{ key, val driver.Value }

type unnestCursor struct {
	rows []unnestRow
	pos  int
	cols int
}

func (c *unnestCursor) Filter(_ int, _ string, vals []vtab.Value) error {
	c.rows, c.pos = nil, 0

	if len(vals) == 0 || vals[0] == nil {
		return nil
	}

	if isSeriesText(vals[0]) {
		pts, err := decodeSeries(vals[0])
		if err != nil {
			return err
		}

		for _, p := range pts {
			c.rows = append(c.rows, unnestRow{key: formatTS(p.t), val: p.v})
		}

		return nil
	}

	s, ok := textOf(vals[0])
	if !ok {
		return nil
	}

	var root any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(s, arrayMark)), &root); err != nil {
		return nil //nolint:nilerr // not JSON: no rows
	}

	switch t := root.(type) {
	case []any:
		for i, e := range t {
			c.rows = append(c.rows, unnestRow{key: int64(i), val: unnestValue(e)})
		}
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		for _, k := range keys {
			c.rows = append(c.rows, unnestRow{key: k, val: unnestValue(t[k])})
		}
	}

	return nil
}

func unnestValue(v any) driver.Value {
	switch t := v.(type) {
	case nil:
		return nil
	case bool:
		if t {
			return int64(1)
		}

		return int64(0)
	case float64:
		if t == math.Trunc(t) && math.Abs(t) < 1<<53 {
			return int64(t)
		}

		return t
	case string:
		return t
	default:
		b, _ := json.Marshal(t)

		return arrayMark + string(b)
	}
}

func (c *unnestCursor) Next() error {
	c.pos++

	return nil
}

func (c *unnestCursor) Eof() bool { //nolint:revive,staticcheck // name fixed by vtab.Cursor
	return c.pos >= len(c.rows)
}

func (c *unnestCursor) Column(col int) (vtab.Value, error) {
	row := c.rows[c.pos]

	switch {
	case col == c.cols+1:
		return int64(c.pos), nil
	case col >= c.cols:
		return nil, errUnnestColumn
	case c.cols == 1, col == 1:
		return row.val, nil
	default:
		return row.key, nil
	}
}

func (c *unnestCursor) Rowid() (int64, error) { return int64(c.pos), nil }
func (*unnestCursor) Close() error            { return nil }
