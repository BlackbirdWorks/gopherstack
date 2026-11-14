package dmsengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const (
	maxBindParams    = 30000
	updateArgsFactor = 2
	varcharKey       = "varchar(255)"
)

var errNoRowImage = errors.New("delete has no row image")

type sqlTarget struct {
	db  *sql.DB
	ep  EndpointConfig
	fam Family
	src Family
}

func openSQLTarget(_ context.Context, ep EndpointConfig, fam, src Family) (Target, error) {
	var (
		db  *sql.DB
		err error
	)

	if fam == FamilyPostgres {
		db, err = pgDB(ep)
	} else {
		db, err = mysqlDB(ep, true)
	}

	if err != nil {
		return nil, err
	}

	return &sqlTarget{db: db, fam: fam, src: src, ep: ep}, nil
}

func (t *sqlTarget) Ping(ctx context.Context) error {
	return explainTLS(t.ep, t.db.PingContext(ctx))
}

func (t *sqlTarget) Close() error { return t.db.Close() }

func (t *sqlTarget) Commit(context.Context) error { return nil }

func (t *sqlTarget) q(name string) string { return QuoteIdent(t.fam, name) }

func (t *sqlTarget) qualified(tt TargetTable) string { return t.q(tt.Schema) + "." + t.q(tt.Name) }

func (t *sqlTarget) ph(n int) string {
	if t.fam == FamilyPostgres {
		return "$" + strconv.Itoa(n)
	}

	return "?"
}

func (t *sqlTarget) columnType(tt TargetTable, c TargetColumn) string {
	var typ string

	if c.DMS.Name != "" {
		typ = c.DMS.SQLType(t.fam)
	} else {
		typ = TargetType(tt.SourceFamily, t.fam, c.Native)
	}

	if t.fam == FamilyMySQL && (typ == tLongtext || typ == tText) && slicesContain(tt.PK, c.Name) {
		return varcharKey
	}

	return typ
}

func slicesContain(list []string, s string) bool { return slices.Contains(list, s) }

func (t *sqlTarget) createSQL(tt TargetTable) string {
	defs := make([]string, 0, len(tt.Columns)+1)

	for _, c := range tt.Columns {
		def := t.q(c.Name) + " " + t.columnType(tt, c)
		if !c.Nullable || (slicesContain(tt.PK, c.Name) && !tt.KeyUnique) {
			def += " NOT NULL"
		}

		defs = append(defs, def)
	}

	if len(tt.PK) > 0 {
		defs = append(defs, t.keyDef(tt))
	}

	return "CREATE TABLE IF NOT EXISTS " + t.qualified(tt) + " (" + strings.Join(defs, ", ") + ")"
}

func (t *sqlTarget) keyDef(tt TargetTable) string {
	kind := "PRIMARY KEY"
	if tt.KeyUnique {
		kind = "UNIQUE"
	}

	def := kind + " (" + t.quoteAll(tt.PK) + ")"
	if tt.KeyName != "" {
		def = "CONSTRAINT " + t.q(tt.KeyName) + " " + def
	}

	return def
}

func (t *sqlTarget) quoteAll(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = t.q(n)
	}

	return strings.Join(q, ", ")
}

func (t *sqlTarget) Prepare(ctx context.Context, tt TargetTable, mode PrepMode) error {
	create := stmt("CREATE SCHEMA IF NOT EXISTS ", t.q(tt.Schema))
	if t.fam == FamilyMySQL {
		create = stmt("CREATE DATABASE IF NOT EXISTS ", t.q(tt.Schema))
	}

	if _, err := t.db.ExecContext(ctx, create); err != nil {
		return err
	}

	if mode == PrepDropAndCreate {
		if _, err := t.db.ExecContext(ctx, stmt("DROP TABLE IF EXISTS ", t.qualified(tt))); err != nil {
			return err
		}
	}

	if _, err := t.db.ExecContext(ctx, t.createSQL(tt)); err != nil {
		return err
	}

	if mode == PrepTruncateBefore {
		_, err := t.db.ExecContext(ctx, stmt("TRUNCATE TABLE ", t.qualified(tt)))

		return err
	}

	return nil
}

func (t *sqlTarget) EndLoad(context.Context, TargetTable) error { return nil }

func (t *sqlTarget) Load(ctx context.Context, tt TargetTable, rows []Row) error {
	if len(rows) == 0 || len(tt.Columns) == 0 {
		return nil
	}

	per := max(1, maxBindParams/len(tt.Columns))
	cols := t.quoteAll(columnNames(tt))

	for lo := 0; lo < len(rows); lo += per {
		chunk := rows[lo:min(lo+per, len(rows))]
		args := make([]any, 0, len(chunk)*len(tt.Columns))
		groups := make([]string, len(chunk))
		n := 0

		for i, r := range chunk {
			ph := make([]string, len(tt.Columns))

			for j := range tt.Columns {
				n++
				ph[j] = t.ph(n)
				args = append(args, r[j])
			}

			groups[i] = "(" + strings.Join(ph, ", ") + ")"
		}

		q := stmt("INSERT INTO ", t.qualified(tt), " (", cols, ") VALUES ", strings.Join(groups, ", "))
		if _, err := t.db.ExecContext(ctx, q, args...); err != nil {
			return err
		}
	}

	return nil
}

func columnNames(tt TargetTable) []string {
	out := make([]string, len(tt.Columns))
	for i, c := range tt.Columns {
		out[i] = c.Name
	}

	return out
}

func (t *sqlTarget) Apply(ctx context.Context, tt TargetTable, c Change) error {
	switch c.Op {
	case OpInsert:
		return t.upsert(ctx, tt, c.After)
	case OpDelete:
		return t.deleteRow(ctx, tt, c.Before)
	case OpUpdate:
		return t.update(ctx, tt, c)
	}

	return nil
}

func (t *sqlTarget) upsert(ctx context.Context, tt TargetTable, row Row) error {
	cols := t.quoteAll(columnNames(tt))
	ph := make([]string, len(tt.Columns))

	for i := range ph {
		ph[i] = t.ph(i + 1)
	}

	q := stmt("INSERT INTO ", t.qualified(tt), " (", cols, ") VALUES (", strings.Join(ph, ", "), ")")

	var sets []string

	for _, c := range tt.Columns {
		if !slicesContain(tt.PK, c.Name) {
			if t.fam == FamilyPostgres {
				sets = append(sets, t.q(c.Name)+" = EXCLUDED."+t.q(c.Name))
			} else {
				sets = append(sets, t.q(c.Name)+" = VALUES("+t.q(c.Name)+")")
			}
		}
	}

	switch {
	case len(tt.PK) == 0:
	case t.fam == FamilyPostgres && len(sets) == 0:
		q += " ON CONFLICT (" + t.quoteAll(tt.PK) + ") DO NOTHING"
	case t.fam == FamilyPostgres:
		q += " ON CONFLICT (" + t.quoteAll(tt.PK) + ") DO UPDATE SET " + strings.Join(sets, ", ")
	case len(sets) > 0:
		q += " ON DUPLICATE KEY UPDATE " + strings.Join(sets, ", ")
	default:
		q = strings.Replace(q, "INSERT INTO", "INSERT IGNORE INTO", 1)
	}

	_, err := t.db.ExecContext(ctx, q, rowArgs(row)...)

	return err
}

func rowArgs(r Row) []any {
	out := make([]any, len(r))
	copy(out, r)

	return out
}

// where builds a key predicate from the image: the primary key when present, otherwise every column.
func (t *sqlTarget) where(tt TargetTable, image Row, start int) (string, []any) {
	var (
		parts []string
		args  []any
	)

	for i, c := range tt.Columns {
		if len(tt.PK) > 0 && !slicesContain(tt.PK, c.Name) {
			continue
		}

		if image[i] == nil {
			parts = append(parts, t.q(c.Name)+" IS NULL")

			continue
		}

		parts = append(parts, t.q(c.Name)+" = "+t.ph(start+len(args)+1))
		args = append(args, image[i])
	}

	return strings.Join(parts, " AND "), args
}

func (t *sqlTarget) deleteRow(ctx context.Context, tt TargetTable, before Row) error {
	if before == nil {
		return fmt.Errorf("%w: %s", errNoRowImage, tt.Key())
	}

	w, args := t.where(tt, before, 0)
	_, err := t.db.ExecContext(ctx, stmt("DELETE FROM ", t.qualified(tt), " WHERE ", w), args...)

	return err
}

func (t *sqlTarget) update(ctx context.Context, tt TargetTable, c Change) error {
	image := c.Before
	if image == nil || !keyComplete(tt, image) {
		image = c.After
	}

	sets := make([]string, len(tt.Columns))
	args := make([]any, 0, len(tt.Columns)*updateArgsFactor)

	for i, col := range tt.Columns {
		sets[i] = t.q(col.Name) + " = " + t.ph(i+1)
		args = append(args, c.After[i])
	}

	w, wargs := t.where(tt, image, len(tt.Columns))
	args = append(args, wargs...)

	update := stmt("UPDATE ", t.qualified(tt), " SET ", strings.Join(sets, ", "), " WHERE ", w)
	res, err := t.db.ExecContext(
		ctx,
		update,
		args...)
	if err != nil {
		return err
	}

	if n, nerr := res.RowsAffected(); nerr == nil && n == 0 {
		return t.upsert(ctx, tt, c.After)
	}

	return nil
}

func keyComplete(tt TargetTable, image Row) bool {
	if len(tt.PK) == 0 {
		return true
	}

	for i, c := range tt.Columns {
		if slicesContain(tt.PK, c.Name) && image[i] == nil {
			return false
		}
	}

	return true
}

// stmt joins SQL fragments whose identifiers were already quoted with QuoteIdent.
func stmt(parts ...string) string { return strings.Join(parts, "") }
