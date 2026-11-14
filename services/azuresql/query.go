package azuresql

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Query limits.
const (
	MaxQueryLen   = 4000
	MaxQueryRows  = 500
	MaxCellLen    = 1000
	QueryDeadline = 20 * time.Second
	maxQueryBytes = 1 << 20
	colSep        = "\x1f"
	headerLines   = 2
)

// ErrNoEngine is returned when the server has no SQL engine (no container runtime) to run a query.
var ErrNoEngine = fmt.Errorf("%w: server has no SQL engine (state-only)", ErrInvalid)

// QueryResult is a capped result set; every cell is text.
type QueryResult struct {
	Columns   []string
	Rows      [][]string
	Truncated bool
}

// Querier is implemented by engines that run read-only queries.
type Querier interface {
	Query(ctx context.Context, database, text string, maxRows int) (QueryResult, error)
}

var (
	forbiddenWord = regexp.MustCompile(`(?i)\b(` + strings.Join([]string{
		"into", "exec", "execute", "openrowset", "openquery", "opendatasource", "openxml", "bulk", "waitfor",
		"insert", "update", "delete", "merge", "drop", "alter", "create", "truncate", "grant", "revoke", "deny",
		"shutdown", "kill", "dbcc", "backup", "restore", "use", "set", "declare", "go", "reconfigure",
		"sql_logins", "syslogins", "sysxlogins", "credentials", "master_key_passwords", "key_encryptions",
	}, "|") + `)\b`)
	forbiddenPrefix = regexp.MustCompile(`(?i)\b(xp_|sp_|fn_)`)
	firstSelect     = regexp.MustCompile(`(?is)^\s*select\b`)
	literalOrQuoted = regexp.MustCompile(`'(?:[^']|'')*'|\[(?:[^\]]|\]\])*\]|"[^"]*"`)
	databaseName    = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_ .-]{0,127}$`)
)

// ValidateQuery accepts one plain SELECT statement: no comments, no batch separators and none of the keywords
// that write, execute or reach outside the database.
func ValidateQuery(text string) error {
	if strings.TrimSpace(text) == "" {
		return fmt.Errorf("%w: query is empty", ErrInvalid)
	}

	if len(text) > MaxQueryLen {
		return fmt.Errorf("%w: query is longer than %d characters", ErrInvalid, MaxQueryLen)
	}

	if err := checkPlainText(text); err != nil {
		return err
	}

	bare := literalOrQuoted.ReplaceAllString(text, "''")
	bare = strings.TrimSpace(strings.TrimRight(strings.TrimSpace(bare), ";"))

	switch {
	case strings.Contains(bare, ";"):
		return fmt.Errorf("%w: only a single statement is allowed", ErrInvalid)
	case !firstSelect.MatchString(bare):
		return fmt.Errorf("%w: only SELECT statements are allowed", ErrInvalid)
	}

	if w := forbiddenWord.FindString(bare); w != "" {
		return fmt.Errorf("%w: %q is not allowed in a read-only query", ErrInvalid, strings.ToUpper(w))
	}

	if forbiddenPrefix.MatchString(bare) {
		return fmt.Errorf("%w: system procedures and functions are not allowed", ErrInvalid)
	}

	return nil
}

func checkPlainText(text string) error {
	for _, r := range text {
		if (r < ' ' && r != '\t' && r != '\n' && r != '\r') || r == 0x7f {
			return fmt.Errorf("%w: query contains a control character", ErrInvalid)
		}
	}

	if strings.Contains(text, "--") || strings.Contains(text, "/*") || strings.Contains(text, "*/") {
		return fmt.Errorf("%w: comments are not allowed", ErrInvalid)
	}

	return nil
}

// ValidateDatabaseName accepts a plain database name.
func ValidateDatabaseName(name string) error {
	if !databaseName.MatchString(name) {
		return fmt.Errorf("%w: database name", ErrInvalid)
	}

	return nil
}

// Query runs a validated read-only SELECT on database and returns at most MaxQueryRows rows.
func (b *InMemoryBackend) Query(ctx context.Context, id, database, text string) (QueryResult, error) {
	if err := ValidateQuery(text); err != nil {
		return QueryResult{}, err
	}

	if err := ValidateDatabaseName(database); err != nil {
		return QueryResult{}, err
	}

	s, ok := b.lookup(id)
	if !ok {
		return QueryResult{}, ErrNotFound
	}

	q, ok := s.engine.(Querier)
	if !ok || s.engine == nil {
		return QueryResult{}, ErrNoEngine
	}

	ctx, cancel := context.WithTimeout(ctx, QueryDeadline)
	defer cancel()

	return q.Query(ctx, database, text, MaxQueryRows)
}

// Query implements Querier.
func (e *dockerEngine) Query(ctx context.Context, database, text string, maxRows int) (QueryResult, error) {
	batch := "SET NOCOUNT ON; SET ROWCOUNT " + strconv.Itoa(maxRows+1) + "; USE [" +
		strings.ReplaceAll(database, "]", "]]") + "]; " + text

	out, err := e.runQuery(ctx, batch)
	if err != nil {
		return QueryResult{}, err
	}

	return parseQueryOutput(out, maxRows), nil
}

func parseQueryOutput(out string, maxRows int) QueryResult {
	if len(out) > maxQueryBytes {
		out = out[:maxQueryBytes]
	}

	lines := strings.Split(strings.ReplaceAll(out, "\r\n", "\n"), "\n")
	if len(lines) < headerLines {
		return QueryResult{Columns: []string{}, Rows: [][]string{}}
	}

	res := QueryResult{Columns: strings.Split(lines[0], colSep), Rows: [][]string{}}

	for _, line := range lines[headerLines:] {
		if line == "" {
			break
		}

		if len(res.Rows) == maxRows {
			res.Truncated = true

			break
		}

		cells := strings.Split(line, colSep)
		for i, c := range cells {
			if len(c) > MaxCellLen {
				cells[i] = strings.ToValidUTF8(c[:MaxCellLen], "?") + "..."
			}
		}

		res.Rows = append(res.Rows, cells)
	}

	return res
}
