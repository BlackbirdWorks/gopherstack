package azuresearch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const sqlQueryTimeout = 30 * time.Second

// SQLSource runs read-only queries on a server of the in-process Azure SQL emulator.
type SQLSource interface {
	Query(ctx context.Context, server, database, text string) (cols []string, rows [][]string, err error)
}

type noSQL struct{}

func (noSQL) Query(context.Context, string, string, string) ([]string, [][]string, error) {
	return nil, nil, ErrSourceUnavailable
}

// SetSQLSource wires the in-process Azure SQL emulator used by azuresql data sources.
func (b *InMemoryBackend) SetSQLSource(s SQLSource) {
	b.mu.Lock("SetSQLSource")
	defer b.mu.Unlock()

	b.sql = s
}

var sqlObjectRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

// sqlServerName reads the server name out of "Server=tcp:name.database.windows.net,1433;".
func sqlServerName(conn string) string {
	v := connValue(conn, "Server")
	if v == "" {
		v = connValue(conn, "Data Source")
	}

	v = strings.TrimPrefix(strings.TrimPrefix(v, "tcp:"), "TCP:")
	host, _, _ := strings.Cut(v, ",")
	name, _, _ := strings.Cut(host, ".")

	return name
}

func quoteSQLName(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = "[" + p + "]"
	}

	return strings.Join(parts, ".")
}

// loadSQL reads every row of the table or view; the first column identifies the row.
func (b *InMemoryBackend) loadSQL(conn, object string, ds Object) ([]sourceDoc, error) {
	if !sqlObjectRE.MatchString(object) {
		return nil, wrapf(ErrBadRequest, "the SQL table or view name %q is invalid", object)
	}

	server, db := sqlServerName(conn), connValue(conn, "Database")
	if db == "" {
		db = connValue(conn, "Initial Catalog")
	}

	ctx, cancel := context.WithTimeout(context.Background(), sqlQueryTimeout)
	defer cancel()

	cols, rows, err := b.sql.Query(ctx, server, db, "SELECT * FROM "+quoteSQLName(object))
	if err != nil {
		return nil, err
	}

	col, marker := deletionMarker(ds)
	out := make([]sourceDoc, 0, len(rows))

	for _, row := range rows {
		f := make(map[string]any, len(cols))
		for i, c := range cols {
			if i < len(row) {
				f[c] = row[i]
			}
		}

		sum := sha256.Sum256([]byte(strings.Join(row, "\x1f")))
		id := ""

		if len(row) > 0 {
			id = row[0]
		}

		sd := sourceDoc{fields: f, id: id, version: hex.EncodeToString(sum[:8])}
		sd.deleted = col != "" && f[col] != nil && fmt.Sprint(f[col]) == marker
		out = append(out, sd)
	}

	return out, nil
}
