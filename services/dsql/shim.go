package dsql

import (
	"regexp"
	"strings"
)

// Unsupported-feature list: docs.aws.amazon.com/aurora-dsql/latest/userguide/
// working-with-postgresql-compatibility-migration-guide.html (no temporary tables, triggers, TRUNCATE, VACUUM,
// tablespaces, PL/pgSQL, a single built-in database). The error text is not documented.
const (
	unsupportedSQLState = "0A000"
	// plainIndexMessage: troubleshooting.html titles the error "use CREATE INDEX ASYNC instead" without the full text.
	plainIndexMessage = "unsupported mode. please use CREATE INDEX ASYNC"
)

var (
	asyncIndexRE = regexp.MustCompile(`(?is)^create\s+(unique\s+)?index\s+async\b(.*)$`)
	plainIndexRE = regexp.MustCompile(`(?is)^create\s+(unique\s+)?index\b`)
	iamGrantRE   = regexp.MustCompile(
		`(?is)^aws\s+iam\s+(grant|revoke)\s+("(?:[^"]|"")+"|[a-z_][a-z0-9_$]*)\s+(?:to|from)\s+'((?:[^']|'')*)'$`)
)

var (
	triggerRE    = regexp.MustCompile(`(?is)^create\s+(or\s+replace\s+)?(constraint\s+)?trigger\b`)
	tempObjectRE = regexp.MustCompile(`(?is)^create\s+((global|local)\s+)?(temp|temporary)\s+(table|sequence|view)\b`)
	truncateRE   = regexp.MustCompile(`(?is)^truncate\b`)
	vacuumRE     = regexp.MustCompile(`(?is)^vacuum\b`)
	tablespaceRE = regexp.MustCompile(`(?is)^create\s+tablespace\b`)
	databaseRE   = regexp.MustCompile(`(?is)^create\s+database\b`)
	plpgsqlRE    = regexp.MustCompile(
		`(?is)^create\s+(or\s+replace\s+)?(function|procedure)\b.*\blanguage\s+'?plpgsql\b`,
	)
)

type unsupportedRule struct {
	re      *regexp.Regexp
	feature string
}

func unsupportedStatements() []unsupportedRule {
	return []unsupportedRule{
		{triggerRE, "CREATE TRIGGER"},
		{tempObjectRE, "temporary objects"},
		{truncateRE, "TRUNCATE"},
		{vacuumRE, "VACUUM"},
		{tablespaceRE, "CREATE TABLESPACE"},
		{databaseRE, "CREATE DATABASE"},
		{plpgsqlRE, "PL/pgSQL"},
	}
}

// rewriteSQL maps DSQL-only statements onto the PostgreSQL container. Statements it does not know pass through.
func rewriteSQL(sql string) string {
	stmt := strings.TrimRight(skipLeading(sql), "; \t\r\n")
	if stmt == "" {
		return sql
	}

	if m := asyncIndexRE.FindStringSubmatch(stmt); m != nil {
		return rewriteAsyncIndex(m)
	}

	if plainIndexRE.MatchString(stmt) {
		return raiseWith(unsupportedSQLState, plainIndexMessage)
	}

	if m := iamGrantRE.FindStringSubmatch(stmt); m != nil {
		return rewriteIAMGrant(m)
	}

	for _, u := range unsupportedStatements() {
		if u.re.MatchString(stmt) {
			return raiseUnsupported(u.feature)
		}
	}

	return sql
}

func rewriteAsyncIndex(m []string) string {
	rest := m[2]
	if hasStatementBreak(rest) {
		return raiseUnsupported("multiple statements with CREATE INDEX ASYNC")
	}

	ddl := "CREATE " + strings.ToUpper(strings.TrimSpace(m[1]))
	if m[1] != "" {
		ddl += " "
	}

	ddl += "INDEX" + rest

	return "SELECT sys.gs_create_index_async(" + quoteLiteral(ddl) + ") AS job_id"
}

func rewriteIAMGrant(m []string) string {
	fn := "sys.gs_iam_grant"
	if strings.EqualFold(m[1], "revoke") {
		fn = "sys.gs_iam_revoke"
	}

	role := m[2]
	if strings.HasPrefix(role, `"`) {
		role = strings.ReplaceAll(role[1:len(role)-1], `""`, `"`)
	} else {
		role = strings.ToLower(role)
	}

	return "SELECT " + fn + "(" + quoteLiteral(role) + ", " + quoteLiteral(strings.ReplaceAll(m[3], "''", "'")) + ")"
}

func raiseUnsupported(feature string) string {
	return raiseWith(unsupportedSQLState, feature+" is not supported in Aurora DSQL")
}

func raiseWith(code, msg string) string {
	return "DO $$ BEGIN RAISE EXCEPTION " + quoteLiteral(msg) + " USING ERRCODE = '" + code + "'; END $$"
}

func quoteLiteral(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

// skipLeading drops leading whitespace and SQL comments.
func skipLeading(sql string) string {
	for {
		sql = strings.TrimLeft(sql, " \t\r\n")

		switch {
		case strings.HasPrefix(sql, "--"):
			i := strings.IndexByte(sql, '\n')
			if i < 0 {
				return ""
			}

			sql = sql[i+1:]
		case strings.HasPrefix(sql, "/*"):
			i := strings.Index(sql, "*/")
			if i < 0 {
				return ""
			}

			sql = sql[i+2:]
		default:
			return sql
		}
	}
}

// hasStatementBreak reports a semicolon outside quotes followed by more text.
func hasStatementBreak(sql string) bool {
	var quote byte

	for i := range len(sql) {
		c := sql[i]

		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == ';':
			return strings.TrimSpace(strings.Trim(sql[i:], "; \t\r\n")) != ""
		}
	}

	return false
}
