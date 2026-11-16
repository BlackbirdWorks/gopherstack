package gcpspanner

import (
	"slices"
	"strings"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

const (
	kindTable = "TABLE"
	kindIndex = "INDEX"
	kindView  = "VIEW"
	kindRole  = "ROLE"

	posName = 2
	posType = 3
)

// schemaKinds share one namespace in a database; ROLE has its own.
func schemaKinds() []string {
	return []string{
		kindTable, kindIndex, kindView, "SEQUENCE", "CHANGE STREAM", "MODEL", "SEARCH INDEX", "VECTOR INDEX",
		"PROPERTY GRAPH",
	}
}

type ddlObject struct {
	kind, key, name, text string
}

type ddlSchema struct {
	objs     []ddlObject
	postgres bool
}

func foldKey(s string) string { return strings.ToLower(s) }

func (s *ddlSchema) find(kind, name string) int {
	return slices.IndexFunc(s.objs, func(o ddlObject) bool {
		return o.key == foldKey(name) && (o.kind == kind || kind == "" && o.kind != kindRole)
	})
}

func (s *ddlSchema) texts() []string {
	out := make([]string, len(s.objs))
	for i, o := range s.objs {
		out[i] = o.text
	}

	return out
}

func isCreateModifier(t ddlTok) bool {
	return t.is("OR") || t.is("REPLACE") || t.is("UNIQUE") || t.is("NULL_FILTERED")
}

func classify(toks []ddlTok) (string, int) {
	i := 1

	for i < len(toks) && isCreateModifier(toks[i]) {
		i++
	}

	for _, k := range schemaKinds() {
		words := strings.Fields(k)
		if i+len(words) > len(toks) {
			continue
		}

		match := true

		for j, w := range words {
			if !toks[i+j].is(w) {
				match = false

				break
			}
		}

		if match {
			return k, i + len(words)
		}
	}

	if i < len(toks) && toks[i].is(kindRole) {
		return kindRole, i + 1
	}

	return "", 0
}

func skipIf(toks []ddlTok, i int, words ...string) (int, bool) {
	for j, w := range words {
		if i+j >= len(toks) || !toks[i+j].is(w) {
			return i, false
		}
	}

	return i + len(words), true
}

func objectName(toks []ddlTok, at int) (string, int, error) {
	if at >= len(toks) || !toks[at].word {
		return "", 0, errInvalid("Syntax error: expected a name.")
	}

	parts := []string{identName(toks[at])}
	at++

	for at+1 < len(toks) && toks[at].text == "." && toks[at+1].word {
		parts = append(parts, identName(toks[at+1]))
		at += posName
	}

	return strings.Join(parts, "."), at, nil
}

// loadSchema rebuilds the object list from stored statements. Statements that name no object (GRANT, ALTER
// DATABASE, unmodelled ALTER forms) are kept as anonymous objects.
func loadSchema(stmts []string, postgres bool) *ddlSchema {
	s := &ddlSchema{postgres: postgres}

	for _, st := range stmts {
		toks, err := tokenizeDDL(st)
		if err != nil || len(toks) == 0 || !toks[0].is("CREATE") {
			s.objs = append(s.objs, ddlObject{text: st})

			continue
		}

		kind, at := classify(toks)
		if kind == "" {
			s.objs = append(s.objs, ddlObject{text: st})

			continue
		}

		at, _ = skipIf(toks, at, "IF", "NOT", "EXISTS")

		name, _, nerr := objectName(toks, at)
		if nerr != nil {
			s.objs = append(s.objs, ddlObject{text: st})

			continue
		}

		s.objs = append(s.objs, ddlObject{kind: kind, key: foldKey(name), name: name, text: st})
	}

	return s
}

// foldDDL applies stmts to the stored DDL in order and returns the new DDL plus the status of the first
// failing statement; statements before it stay applied, as in a partially applied batch.
func foldDDL(current, stmts []string, postgres bool) ([]string, error) {
	s := loadSchema(current, postgres)

	for _, st := range stmts {
		if err := s.apply(st); err != nil {
			return s.texts(), err
		}
	}

	return s.texts(), nil
}

func (s *ddlSchema) apply(stmt string) error {
	toks, err := tokenizeDDL(stmt)
	if err != nil {
		return err
	}

	if len(toks) == 0 {
		return errInvalid("DDL statements must not be empty.")
	}

	switch {
	case toks[0].is("CREATE"):
		return s.create(stmt, toks)
	case toks[0].is("DROP"):
		return s.drop(toks)
	case toks[0].is("ALTER"):
		return s.alter(stmt, toks)
	case toks[0].is("GRANT"), toks[0].is("REVOKE"), toks[0].is("RENAME"):
		s.objs = append(s.objs, ddlObject{text: stmt})

		return nil
	case toks[0].is("ANALYZE"):
		return nil
	default:
		return errInvalid(
			"Syntax error: unexpected %q; expected CREATE, ALTER, DROP, GRANT, REVOKE, RENAME or ANALYZE.",
			toks[0].text,
		)
	}
}

func (s *ddlSchema) create(stmt string, toks []ddlTok) error {
	kind, at := classify(toks)
	if kind == "" {
		return errInvalid("Syntax error: unsupported CREATE statement.")
	}

	at, ifNot := skipIf(toks, at, "IF", "NOT", "EXISTS")

	name, at, err := objectName(toks, at)
	if err != nil {
		return err
	}

	replace := toks[1].is("OR") && kind == kindView

	lookupKind := kind
	if kind != kindRole {
		lookupKind = ""
	}

	if i := s.find(lookupKind, name); i >= 0 {
		switch {
		case replace && s.objs[i].kind == kindView:
			s.objs[i].text = stmt

			return nil
		case ifNot:
			return nil
		default:
			return errExists("Duplicate name in schema: %s.", name)
		}
	}

	if err = s.checkCreate(kind, toks, at); err != nil {
		return err
	}

	s.objs = append(s.objs, ddlObject{kind: kind, key: foldKey(name), name: name, text: stmt})

	return nil
}

func (s *ddlSchema) checkCreate(kind string, toks []ddlTok, at int) error {
	switch kind {
	case kindTable:
		return s.checkTable(toks, at)
	case kindIndex:
		return s.checkIndex(toks, at)
	default:
		return nil
	}
}

func (s *ddlSchema) checkTable(toks []ddlTok, at int) error {
	if at >= len(toks) || toks[at].text != "(" {
		return errInvalid("Syntax error: expected '(' after the table name.")
	}

	closeIdx := matchParen(toks, at)
	if closeIdx < 0 {
		return errInvalid("Syntax error: unbalanced parentheses.")
	}

	items := splitTop(toks, at, closeIdx)
	if len(items) == 1 && len(items[0]) == 0 {
		return errInvalid("Syntax error: a table needs at least one column.")
	}

	seen := map[string]bool{}

	for _, it := range items {
		if len(it) == 0 {
			return errInvalid("Syntax error: empty column definition.")
		}

		if n, ok := columnName(it); ok {
			if seen[foldKey(n)] {
				return errExists("Duplicate column name %s.", n)
			}

			seen[foldKey(n)] = true
		}
	}

	return s.checkTableTail(toks[closeIdx+1:], seen)
}

func (s *ddlSchema) checkTableTail(tail []ddlTok, cols map[string]bool) error {
	pk := slices.IndexFunc(tail, func(t ddlTok) bool { return t.is("PRIMARY") })
	if pk < 0 && !s.postgres {
		return errInvalid("Syntax error: a table requires a PRIMARY KEY clause.")
	}

	if pk >= 0 && pk+posName < len(tail) && tail[pk+1].is("KEY") && tail[pk+posName].text == "(" {
		end := matchParen(tail, pk+posName)
		if end < 0 {
			return errInvalid("Syntax error: unbalanced parentheses.")
		}

		for _, k := range splitTop(tail, pk+posName, end) {
			if len(k) > 0 && k[0].word && !cols[foldKey(identName(k[0]))] {
				return errNotFound("Table primary key column %s is not defined.", identName(k[0]))
			}
		}
	}

	if i := slices.IndexFunc(tail, func(t ddlTok) bool { return t.is("INTERLEAVE") }); i >= 0 {
		return s.checkInterleave(tail[i:])
	}

	return nil
}

func (s *ddlSchema) checkInterleave(tail []ddlTok) error {
	if len(tail) <= posType || !tail[1].is("IN") || !tail[posName].is("PARENT") {
		return errInvalid("Syntax error: expected INTERLEAVE IN PARENT.")
	}

	parent, _, err := objectName(tail, posType)
	if err != nil {
		return err
	}

	if s.find(kindTable, parent) < 0 {
		return errNotFound("Parent table %s not found.", parent)
	}

	return nil
}

func columnName(item []ddlTok) (string, bool) {
	if len(item) == 0 || !item[0].word {
		return "", false
	}

	switch strings.ToUpper(item[0].text) {
	case "CONSTRAINT", "FOREIGN", "CHECK", "PRIMARY", "UNIQUE":
		return "", false
	}

	return identName(item[0]), true
}

func (s *ddlSchema) checkIndex(toks []ddlTok, at int) error {
	on := slices.IndexFunc(toks[at:], func(t ddlTok) bool { return t.is("ON") })
	if on < 0 {
		return errInvalid("Syntax error: expected ON after the index name.")
	}

	table, next, err := objectName(toks, at+on+1)
	if err != nil {
		return err
	}

	ti := s.find(kindTable, table)
	if ti < 0 {
		return errNotFound("Table not found: %s.", table)
	}

	if next >= len(toks) || toks[next].text != "(" {
		return errInvalid("Syntax error: expected '(' with the index columns.")
	}

	end := matchParen(toks, next)
	if end < 0 {
		return errInvalid("Syntax error: unbalanced parentheses.")
	}

	cols := tableColumns(s.objs[ti].text)

	for _, k := range splitTop(toks, next, end) {
		if len(k) == 0 || !k[0].word {
			return errInvalid("Syntax error: expected an index column.")
		}

		if cols != nil && !cols[foldKey(identName(k[0]))] {
			return errNotFound("Column not found in table %s: %s.", table, identName(k[0]))
		}
	}

	return nil
}

// tableColumns returns the lower-cased column names of a stored CREATE TABLE, or nil when it cannot be read.
func tableColumns(text string) map[string]bool {
	toks, err := tokenizeDDL(text)
	if err != nil {
		return nil
	}

	_, at := classify(toks)
	at, _ = skipIf(toks, at, "IF", "NOT", "EXISTS")

	_, at, err = objectName(toks, at)
	if err != nil || at >= len(toks) || toks[at].text != "(" {
		return nil
	}

	closeIdx := matchParen(toks, at)
	if closeIdx < 0 {
		return nil
	}

	out := map[string]bool{}

	for _, it := range splitTop(toks, at, closeIdx) {
		if n, ok := columnName(it); ok {
			out[foldKey(n)] = true
		}
	}

	return out
}

func (s *ddlSchema) drop(toks []ddlTok) error {
	if len(toks) <= posName {
		return errInvalid("Syntax error: incomplete DROP statement.")
	}

	kind, at := classify(append([]ddlTok{toks[0]}, toks[1:]...))
	if kind == "" {
		return errInvalid("Syntax error: unsupported DROP statement.")
	}

	at, ifExists := skipIf(toks, at, "IF", "EXISTS")

	name, _, err := objectName(toks, at)
	if err != nil {
		return err
	}

	lookupKind := kind
	if kind != kindRole {
		lookupKind = ""
	}

	i := s.find(lookupKind, name)

	switch {
	case i < 0 && ifExists:
		return nil
	case i < 0:
		return errNotFound("%s not found: %s.", kind, name)
	case s.objs[i].kind != kind:
		return errNotFound("%s not found: %s.", kind, name)
	}

	if kind == kindTable {
		if dep := s.dependent(name); dep != "" {
			return errPrecondition("Cannot drop table %s: %s depends on it.", name, dep)
		}
	}

	s.objs = slices.Delete(s.objs, i, i+1)

	return nil
}

// dependent names an index or interleaved child table that blocks dropping table.
func (s *ddlSchema) dependent(table string) string {
	for _, o := range s.objs {
		if o.kind != kindIndex && o.kind != kindTable {
			continue
		}

		toks, err := tokenizeDDL(o.text)
		if err != nil {
			continue
		}

		for i, t := range toks {
			ref := o.kind == kindIndex && t.is("ON") || o.kind == kindTable && t.is("PARENT")
			if !ref || i+1 >= len(toks) {
				continue
			}

			if n, _, nerr := objectName(toks, i+1); nerr == nil && foldKey(n) == foldKey(table) {
				return o.kind + " " + o.name
			}
		}
	}

	return ""
}

func databaseIsPostgres(d *databasepb.Database) bool {
	return d.GetDatabaseDialect() == databasepb.DatabaseDialect_POSTGRESQL
}
