package gcpspanner

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// tailClauses splits the text after a CREATE TABLE body at top-level commas.
func tailClauses(tail string, postgres bool) ([]string, error) {
	toks, err := tokenizeDDL(tail, postgres)
	if err != nil {
		return nil, err
	}

	var out []string

	depth, from := 0, 0

	for _, t := range toks {
		switch t.text {
		case "(", "[":
			depth++
		case ")", "]":
			depth--
		case ",":
			if depth == 0 {
				out = append(out, strings.TrimSpace(tail[from:t.start]))
				from = t.end
			}
		}
	}

	if rest := strings.TrimSpace(tail[from:]); rest != "" {
		out = append(out, rest)
	}

	return slices.DeleteFunc(out, func(c string) bool { return c == "" }), nil
}

func joinTail(cl []string) string {
	if len(cl) == 0 {
		return ""
	}

	return " " + strings.Join(cl, ", ")
}

func clauseStarts(c string, words ...string) bool {
	toks, err := tokenizeDDL(c, false)

	return err == nil && hasPrefix(toks, words...)
}

func clauseIndex(cl []string, words ...string) int {
	return slices.IndexFunc(cl, func(c string) bool { return clauseStarts(c, words...) })
}

// insertClause places c after the leading PRIMARY KEY and INTERLEAVE clauses and before ROW DELETION POLICY and
// OPTIONS, the order of the CREATE TABLE grammar.
func insertClause(cl []string, c string, rank int) []string {
	at := len(cl)

	for i, x := range cl {
		if clauseRank(x) > rank {
			at = i

			break
		}
	}

	return slices.Insert(cl, at, c)
}

func clauseRank(c string) int {
	switch {
	case clauseStarts(c, "PRIMARY"):
		return 0
	case clauseStarts(c, "INTERLEAVE"):
		return 1
	case clauseStarts(c, "ROW"):
		return 2 //nolint:mnd // grammar order
	default:
		return 3 //nolint:mnd // grammar order
	}
}

func (t *tableText) updateTail(fn func([]string) ([]string, error)) error {
	cl, err := tailClauses(t.tail, t.postgres)
	if err != nil {
		return err
	}

	if cl, err = fn(cl); err != nil {
		return err
	}

	t.tail = joinTail(cl)

	return nil
}

func (t *tableText) synonym() int {
	return slices.IndexFunc(t.toks, func(it []ddlTok) bool {
		return len(it) > 1 && it[0].is("SYNONYM") && it[1].text == "("
	})
}

func (s *ddlSchema) addSynonym(tt *tableText, stmt string, act []ddlTok) error {
	if len(act) == 0 || !act[0].word {
		return errInvalid("Syntax error: expected a synonym name.")
	}

	if tt.synonym() >= 0 {
		return errExists("The table already has a synonym.")
	}

	name := identName(act[0])
	if s.find("", name) >= 0 {
		return errExists("Duplicate name in schema: %s.", name)
	}

	item := "SYNONYM (" + stmt[act[0].start:act[0].end] + ")"

	toks, err := tokenizeDDL(item, s.postgres)
	if err != nil {
		return err
	}

	tt.items = append(tt.items, item)
	tt.names = append(tt.names, "")
	tt.toks = append(tt.toks, toks)

	return nil
}

func (t *tableText) dropSynonym(act []ddlTok) error {
	i := t.synonym()
	if i < 0 {
		return errNotFound("The table has no synonym.")
	}

	if len(act) > 0 && act[0].word && foldKey(identName(act[0])) != foldKey(identName(t.toks[i][2])) {
		return errNotFound("Synonym not found: %s.", identName(act[0]))
	}

	t.remove(i)

	return nil
}

func (s *ddlSchema) tableAction(i int, tt *tableText, stmt string, act []ddlTok) (bool, error) {
	switch {
	case hasPrefix(act, "ADD", "SYNONYM"):
		return true, s.addSynonym(tt, stmt, act[posName:])
	case hasPrefix(act, "DROP", "SYNONYM"):
		return true, tt.dropSynonym(act[posName:])
	case hasPrefix(act, "RENAME", "TO"):
		return true, s.renameTable(i, tt, stmt, act[posName:])
	case s.postgres:
		return pgTableAction(tt, stmt, act)
	case hasPrefix(act, "SET", "INTERLEAVE", "IN"):
		return true, s.setInterleave(tt, stmt, act[posType:])
	case hasPrefix(act, "SET", "OPTIONS"):
		return true, tt.setTableOptions(stmt, act)
	case hasPrefix(act, "ADD", "ROW", "DELETION", "POLICY"):
		return true, tt.rowDeletion(stmt, act[posType+1:], true, false)
	case hasPrefix(act, "REPLACE", "ROW", "DELETION", "POLICY"):
		return true, tt.rowDeletion(stmt, act[posType+1:], false, true)
	case hasPrefix(act, "DROP", "ROW", "DELETION", "POLICY"):
		return true, tt.dropRowDeletion()
	}

	return false, nil
}

func pgTableAction(tt *tableText, stmt string, act []ddlTok) (bool, error) {
	switch {
	case hasPrefix(act, "ADD", "TTL"):
		return true, tt.pgTTL(stmt, act[posName:], true)
	case hasPrefix(act, "ALTER", "TTL"):
		return true, tt.pgTTL(stmt, act[posName:], false)
	}

	return false, nil
}

func (t *tableText) setTableOptions(stmt string, act []ddlTok) error {
	pairs, _, err := pairsAfter(stmt, act, posName)
	if err != nil {
		return err
	}

	allowed := map[string]func(optPair) error{
		optLocalityGroup: quotedString, optColumnarPolicy: quotedString, "fulltext_dictionary_staleness": quotedString,
	}

	for _, p := range pairs {
		if p.key == "fulltext_dictionary_table" {
			return errInvalid("fulltext_dictionary_table can only be set when the table is created.")
		}
	}

	if err = checkOptionKeys(pairs, allowed, "table"); err != nil {
		return err
	}

	tail, err := applyOptions(t.tail, t.postgres, pairs, ", ")
	if err != nil {
		return err
	}

	t.tail = tail

	return nil
}

func (t *tableText) columnTypeOf(name string) (string, bool) {
	i := t.column(name)
	if i < 0 {
		return "", false
	}

	toks, err := tokenizeDDL(t.items[i], t.postgres)
	if err != nil {
		return "", false
	}

	return columnType(toks, t.postgres), true
}

// policyText validates `(OLDER_THAN(col, INTERVAL n DAY))` against the table and renders the clause.
func (t *tableText) policyText(stmt string, toks []ddlTok) (string, error) {
	const policyTokens = 10

	if len(toks) != policyTokens || toks[0].text != "(" || !toks[1].is("OLDER_THAN") || toks[2].text != "(" ||
		toks[4].text != "," || !toks[5].is("INTERVAL") || !toks[7].is("DAY") || toks[8].text != ")" ||
		toks[9].text != ")" {
		return "", errInvalid("Syntax error: expected (OLDER_THAN(column, INTERVAL n DAY)).")
	}

	days, ok := atoiBounded(toks[6].text, 0, 1<<30) //nolint:mnd // bound on the day count
	if !ok {
		return "", errInvalid("The row deletion policy interval must be a non-negative number of days.")
	}

	col := identName(toks[3])

	typ, found := t.columnTypeOf(col)
	if !found {
		return "", errNotFound("Column not found: %s.", col)
	}

	if !strings.HasPrefix(typ, "TIMESTAMP") {
		return "", errInvalid("The row deletion policy column %s must be of type TIMESTAMP.", col)
	}

	return "ROW DELETION POLICY (OLDER_THAN(" + stmt[toks[3].start:toks[3].end] + ", INTERVAL " +
		strconv.Itoa(days) + " DAY))", nil
}

func (t *tableText) rowDeletion(stmt string, toks []ddlTok, add, replace bool) error {
	policy, err := t.policyText(stmt, toks)
	if err != nil {
		return err
	}

	return t.updateTail(func(cl []string) ([]string, error) {
		i := clauseIndex(cl, "ROW", "DELETION")

		switch {
		case add && i >= 0:
			return nil, errExists("The table already has a row deletion policy.")
		case replace && i < 0:
			return nil, errNotFound("The table has no row deletion policy.")
		case i >= 0:
			cl[i] = policy

			return cl, nil
		}

		return insertClause(cl, policy, clauseRank(policy)), nil
	})
}

func (t *tableText) dropRowDeletion() error {
	return t.updateTail(func(cl []string) ([]string, error) {
		i := clauseIndex(cl, "ROW", "DELETION")
		if i < 0 {
			return nil, errNotFound("The table has no row deletion policy.")
		}

		return slices.Delete(cl, i, i+1), nil
	})
}

type interleaveSpec struct {
	text     string
	parented bool
	cascade  bool
}

// interleaveClause renders the INTERLEAVE clause of SET INTERLEAVE IN; toks follow INTERLEAVE IN.
func (s *ddlSchema) interleaveClause(stmt string, toks []ddlTok) (interleaveSpec, error) {
	var spec interleaveSpec

	at := 0
	if len(toks) > 0 && toks[0].is("PARENT") {
		at, spec.parented = 1, true
	}

	parent, next, err := objectName(toks, at)
	if err != nil {
		return spec, err
	}

	if s.find(kindTable, parent) < 0 {
		return spec, errNotFound("Parent table %s not found.", parent)
	}

	spec.text = "INTERLEAVE IN " + strings.Repeat("PARENT ", btoi(spec.parented)) +
		stmt[toks[at].start:toks[next-1].end]
	if next >= len(toks) {
		return spec, nil
	}

	rest := toks[next:]
	if !hasPrefix(rest, "ON", "DELETE") || len(rest) < posType {
		return spec, errInvalid("Syntax error: expected ON DELETE CASCADE or ON DELETE NO ACTION.")
	}

	switch {
	case rest[posName].is("CASCADE"):
		spec.text, spec.cascade = spec.text+" ON DELETE CASCADE", true
	case rest[posName].is("NO") && len(rest) > posType && rest[posType].is("ACTION"):
		spec.text += " ON DELETE NO ACTION"
	default:
		return spec, errInvalid("Syntax error: expected CASCADE or NO ACTION.")
	}

	return spec, nil
}

// setInterleave folds SET INTERLEAVE IN [PARENT] parent [ON DELETE action]; toks follow INTERLEAVE IN.
func (s *ddlSchema) setInterleave(tt *tableText, stmt string, toks []ddlTok) error {
	spec, err := s.interleaveClause(stmt, toks)
	if err != nil {
		return err
	}

	return tt.updateTail(func(cl []string) ([]string, error) {
		i := clauseIndex(cl, "INTERLEAVE", "IN")
		if i >= 0 && spec.cascade && spec.parented && !clauseStarts(cl[i], "INTERLEAVE", "IN", "PARENT") {
			return nil, errInvalid("Changing INTERLEAVE IN to INTERLEAVE IN PARENT ... ON DELETE CASCADE " +
				"directly is not supported; change to INTERLEAVE IN PARENT first.")
		}

		if i >= 0 {
			cl[i] = spec.text

			return cl, nil
		}

		return insertClause(cl, spec.text, clauseRank(spec.text)), nil
	})
}

func btoi(b bool) int {
	if b {
		return 1
	}

	return 0
}

// renameTable folds RENAME TO new [, ADD SYNONYM synonym] and rewrites every reference to the table.
func (s *ddlSchema) renameTable(i int, tt *tableText, stmt string, act []ddlTok) error {
	newName, at, err := objectName(act, 0)
	if err != nil {
		return err
	}

	if s.find("", newName) >= 0 {
		return errExists("Duplicate name in schema: %s.", newName)
	}

	var synonym []ddlTok

	if at < len(act) {
		if act[at].text != "," || !hasPrefix(act[at+1:], "ADD", "SYNONYM") {
			return errInvalid("Syntax error: expected , ADD SYNONYM after the new table name.")
		}

		synonym = act[at+1+posName:]
	}

	old := s.objs[i].name
	rawNew := stmt[act[0].start:act[at-1].end]

	head, err := renameHead(tt.head, rawNew, s.postgres)
	if err != nil {
		return err
	}

	tt.head = head

	if synonym != nil {
		if err = s.addSynonym(tt, stmt, synonym); err != nil {
			return err
		}
	}

	s.objs[i].key, s.objs[i].name = s.key(newName), newName
	s.renameReferences(old, rawNew, i, tt)

	return nil
}

// renameHead replaces the table name in the text before a table body.
func renameHead(head, raw string, postgres bool) (string, error) {
	toks, err := tokenizeDDL(head, postgres)
	if err != nil {
		return "", err
	}

	_, at := classify(toks)
	at, _ = skipIf(toks, at, "IF", "NOT", "EXISTS")

	_, end, err := objectName(toks, at)
	if err != nil {
		return "", err
	}

	return head[:toks[at].start] + raw + head[toks[end-1].end:], nil
}

func (s *ddlSchema) renameReferences(old, raw string, self int, tt *tableText) {
	k := s.key(old)
	match := func(n string) bool { return s.key(n) == k }

	for j := range s.objs {
		o := &s.objs[j]
		if o.kind == "" || o.kind == kindGrant {
			continue
		}

		if j == self {
			for n := range tt.items {
				tt.items[n] = rewriteRefs(tt.items[n], o.kind, s.postgres, match, raw)
			}

			tt.tail = rewriteRefs(tt.tail, o.kind, s.postgres, match, raw)

			continue
		}

		o.text = rewriteRefs(o.text, o.kind, s.postgres, match, raw)
	}

	s.renameGrantTable(old, raw)
}

type refSpan struct {
	name     string
	from, to int
}

// refSpans finds the table names an object's statement refers to.
func refSpans(text, kind string, postgres bool) []refSpan {
	toks, err := tokenizeDDL(text, postgres)
	if err != nil {
		return nil
	}

	c := &refCollector{toks: toks, kind: kind}
	for i := range toks {
		c.visit(i)
	}

	return c.out
}

type refCollector struct {
	kind  string
	toks  []ddlTok
	out   []refSpan
	inFor bool
}

func (c *refCollector) add(at int) {
	if at >= len(c.toks) {
		return
	}

	if name, end, err := objectName(c.toks, at); err == nil {
		c.out = append(c.out, refSpan{name: name, from: c.toks[at].start, to: c.toks[end-1].end})
	}
}

func (c *refCollector) visit(i int) {
	t := c.toks[i]

	switch {
	case t.is("INTERLEAVE") && i+1 < len(c.toks) && c.toks[i+1].is("IN"):
		at := i + posName
		if at < len(c.toks) && c.toks[at].is("PARENT") {
			at++
		}

		c.add(at)
	case t.is("REFERENCES"):
		c.add(i + 1)
	case c.kind == kindChangeStream:
		c.streamToken(i)
	case isIndexKind(c.kind) && t.is("ON") && len(c.out) == 0:
		c.add(i + 1)
	}
}

// streamToken follows the table list of a change stream's FOR clause.
func (c *refCollector) streamToken(i int) {
	t := c.toks[i]

	switch {
	case t.is("FOR") && depthZero(c.toks, i):
		c.inFor = true
		c.add(i + 1)
	case c.inFor && t.text == "," && depthZero(c.toks, i):
		c.add(i + 1)
	case t.is("OPTIONS") && depthZero(c.toks, i):
		c.inFor = false
	}
}

func isIndexKind(kind string) bool {
	return kind == kindIndex || kind == "SEARCH INDEX" || kind == "VECTOR INDEX"
}

func rewriteRefs(text, kind string, postgres bool, match func(string) bool, raw string) string {
	spans := refSpans(text, kind, postgres)

	for _, sp := range slices.Backward(spans) {
		if match(sp.name) {
			text = text[:sp.from] + raw + text[sp.to:]
		}
	}

	return text
}

var pgIntervalDays = regexp.MustCompile(`^\s*(\d+)\s+days?\s*$`)

// pgTTLClause validates INTERVAL 'n days' ON column and renders the TTL clause.
func (t *tableText) pgTTLClause(stmt string, toks []ddlTok) (string, error) {
	if len(toks) != posType+1 || !toks[0].is("INTERVAL") || !toks[posName].is("ON") ||
		len(toks[1].text) < posName || toks[1].text[0] != '\'' {
		return "", errInvalid("Syntax error: expected TTL INTERVAL 'n days' ON column.")
	}

	m := pgIntervalDays.FindStringSubmatch(toks[1].text[1 : len(toks[1].text)-1])
	if m == nil {
		return "", errInvalid("The TTL interval must be a whole number of days.")
	}

	col := identName(toks[posType])

	typ, ok := t.columnTypeOf(col)
	if !ok {
		return "", errNotFound("Column not found: %s.", col)
	}

	if typ != "TIMESTAMPTZ" && typ != "TIMESTAMPWITHTIMEZONE" {
		return "", errInvalid("The TTL column %s must be of type timestamptz.", col)
	}

	return "TTL INTERVAL '" + m[1] + " days' ON " + stmt[toks[posType].start:toks[posType].end], nil
}

// pgTTL folds ADD TTL and ALTER TTL INTERVAL 'n days' ON column of the PostgreSQL dialect.
func (t *tableText) pgTTL(stmt string, toks []ddlTok, add bool) error {
	clause, err := t.pgTTLClause(stmt, toks)
	if err != nil {
		return err
	}

	toksTail, err := tokenizeDDL(t.tail, true)
	if err != nil {
		return err
	}

	at := slices.IndexFunc(toksTail, func(k ddlTok) bool { return k.is("TTL") })

	switch {
	case add && at >= 0:
		return errExists("The table already has a TTL policy.")
	case !add && at < 0:
		return errNotFound("The table has no TTL policy.")
	case at >= 0:
		t.tail = strings.TrimRight(t.tail[:toksTail[at].start], " ") + " " + clause
	default:
		t.tail = strings.TrimRight(t.tail, " ") + " " + clause
	}

	return nil
}
