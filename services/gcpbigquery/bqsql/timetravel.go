package bqsql

import (
	"time"
)

// TimeTravelCatalog resolves a table as it was at a point in time.
type TimeTravelCatalog interface {
	TableAt(ref TableRef, at time.Time) (*TableData, error)
}

// constTimestamp evaluates a constant TIMESTAMP expression, as FOR SYSTEM_TIME AS OF requires.
func (c *compiler) constTimestamp(n *node) (time.Time, error) {
	g, err := c.expr(n, nil)
	if err != nil {
		return time.Time{}, err
	}

	if g.typ.Kind != KindTimestamp {
		return time.Time{}, invalid("FOR SYSTEM_TIME AS OF requires a TIMESTAMP expression")
	}

	db, err := openDB(c.ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer db.Close()

	rows, err := queryRows(c.ctx, db, "SELECT "+g.sql, []Field{TypeField("t", g.typ)}, c.binds...)
	if err != nil {
		return time.Time{}, err
	}

	if len(rows) != 1 || rows[0][0] == nil {
		return time.Time{}, invalid("FOR SYSTEM_TIME AS OF requires a non-NULL timestamp")
	}

	return ParseTemporal(KindTimestamp, rows[0][0])
}

// tableAsOf resolves a table at the FOR SYSTEM_TIME AS OF timestamp of f.
func (c *compiler) tableAsOf(f *fromItem, ref TableRef) (*TableData, error) {
	if len(f.path) == 1 && c.findCTE(f.path[0]) != nil {
		return nil, invalid("FOR SYSTEM_TIME AS OF cannot be used with a WITH clause table")
	}

	tc, ok := c.in.Catalog.(TimeTravelCatalog)
	if !ok {
		return nil, unsupported("FOR SYSTEM_TIME AS OF on this catalog")
	}

	at, err := c.constTimestamp(f.asOf)
	if err != nil {
		return nil, err
	}

	if prev, seen := c.snaps[ref]; seen && prev != at.UnixMicro() {
		return nil, invalid("Table %s cannot be referenced at two different points in time in one query", ref)
	}

	if c.snaps == nil {
		c.snaps = map[TableRef]int64{}
	}

	c.snaps[ref] = at.UnixMicro()

	return tc.TableAt(ref, at)
}
