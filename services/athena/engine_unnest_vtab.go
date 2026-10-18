package athena

import (
	"database/sql/driver"
	"errors"
	"sort"

	"modernc.org/sqlite/vtab"
)

const (
	unnestModuleName = "athena_unnest"
	unnestColKey     = "_unnest_key"
	unnestColValue   = "_unnest_value"
	unnestColArg     = "_unnest_arg"
	unnestArgColumn  = 2
	unnestSchema     = "CREATE TABLE x(" + unnestColKey + ", " + unnestColValue + ", " + unnestColArg + " HIDDEN)"
)

var (
	errUnnestArg    = errors.New("UNNEST requires an array or map argument")
	errUnnestColumn = errors.New("UNNEST has no such column")
)

// unnestModule is a table-valued function over a JSON array or object; it
// avoids json_each's id/type/path column names leaking into query scope.
type unnestModule struct{}

func (unnestModule) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	return unnestModule{}.Connect(ctx, args)
}

func (unnestModule) Connect(ctx vtab.Context, _ []string) (vtab.Table, error) {
	if err := ctx.Declare(unnestSchema); err != nil {
		return nil, err
	}

	return &unnestTable{}, nil
}

type unnestTable struct{}

func (*unnestTable) BestIndex(info *vtab.IndexInfo) error {
	for i, c := range info.Constraints {
		if c.Usable && c.Op == vtab.OpEQ && c.Column == unnestArgColumn {
			info.Constraints[i].ArgIndex = 0
			info.Constraints[i].Omit = true
			info.EstimatedCost = 1

			return nil
		}
	}

	return errUnnestArg
}

func (*unnestTable) Open() (vtab.Cursor, error) { return &unnestCursor{}, nil }

func (*unnestTable) Disconnect() error { return nil }

func (*unnestTable) Destroy() error { return nil }

type unnestRow struct{ key, val driver.Value }

type unnestCursor struct {
	rows []unnestRow
	pos  int
}

func (c *unnestCursor) Filter(_ int, _ string, vals []vtab.Value) error {
	c.rows, c.pos = nil, 0

	if len(vals) == 0 {
		return nil
	}

	root, ok := parseJSONArg(vals[0])
	if !ok {
		return nil
	}

	switch t := root.(type) {
	case []any:
		for i, e := range t {
			c.rows = append(c.rows, unnestRow{key: int64(i), val: sqlValue(e)})
		}
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		for _, k := range keys {
			c.rows = append(c.rows, unnestRow{key: k, val: sqlValue(t[k])})
		}
	}

	return nil
}

func (c *unnestCursor) Next() error {
	c.pos++

	return nil
}

func (c *unnestCursor) Eof() bool { //nolint:revive,staticcheck // name fixed by vtab.Cursor
	return c.pos >= len(c.rows)
}

func (c *unnestCursor) Column(col int) (vtab.Value, error) {
	switch col {
	case 0:
		return c.rows[c.pos].key, nil
	case 1:
		return c.rows[c.pos].val, nil
	default:
		return nil, errUnnestColumn
	}
}

func (c *unnestCursor) Rowid() (int64, error) { return int64(c.pos), nil }

func (*unnestCursor) Close() error { return nil }

func registerUnnestModule() {
	if err := vtab.RegisterModule(nil, unnestModuleName, unnestModule{}); err != nil {
		panic(err)
	}
}
