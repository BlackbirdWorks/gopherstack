package gcpspanner

import (
	"strconv"
	"strings"
)

var googleTypeStops = map[string]bool{ //nolint:gochecknoglobals // immutable keyword set
	"NOT": true, "DEFAULT": true, "AS": true, "GENERATED": true, "HIDDEN": true, kwPrimary: true, "OPTIONS": true,
}

var pgTypeStops = map[string]bool{ //nolint:gochecknoglobals // immutable keyword set
	"NOT": true, "NULL": true, "DEFAULT": true, "GENERATED": true, kwPrimary: true, "REFERENCES": true,
	"CHECK": true, "CONSTRAINT": true, "UNIQUE": true,
}

// colSpan is a clause of a column definition as token indexes [from, to).
type colSpan struct{ from, to int }

func (c colSpan) ok() bool { return c.to > c.from }

func typeEnd(toks []ddlTok, postgres bool) int {
	stops, depth := googleTypeStops, 0
	if postgres {
		stops = pgTypeStops
	}

	for i := 1; i < len(toks); i++ {
		switch toks[i].text {
		case "(", "[", "<":
			depth++
		case ")", "]", ">":
			depth--
		}

		if depth <= 0 && toks[i].word && stops[strings.ToUpper(toks[i].text)] {
			return i
		}
	}

	return len(toks)
}

func depthZero(toks []ddlTok, i int) bool {
	depth := 0

	for _, t := range toks[:i] {
		switch t.text {
		case "(", "[":
			depth++
		case ")", "]":
			depth--
		}
	}

	return depth == 0
}

func hasWord(toks []ddlTok, word string) bool {
	for i, t := range toks {
		if t.is(word) && depthZero(toks, i) {
			return true
		}
	}

	return false
}

// defaultSpan locates `DEFAULT (expr)`; PostgreSQL expressions run to the next column attribute.
func defaultSpan(toks []ddlTok, postgres bool) colSpan {
	for i, t := range toks {
		if !t.is("DEFAULT") || !depthZero(toks, i) {
			continue
		}

		if !postgres {
			if i+1 < len(toks) && toks[i+1].text == "(" {
				if end := matchParen(toks, i+1); end >= 0 {
					return colSpan{i, end + 1}
				}
			}

			return colSpan{}
		}

		end := i + 1
		for end < len(toks) && !pgAttribute(toks, end) {
			end++
		}

		return colSpan{i, end}
	}

	return colSpan{}
}

func pgAttribute(toks []ddlTok, i int) bool {
	return toks[i].word && pgTypeStops[strings.ToUpper(toks[i].text)] && depthZero(toks, i)
}

func onUpdateSpan(toks []ddlTok) colSpan {
	for i := 0; i+2 < len(toks); i++ {
		if toks[i].is("ON") && toks[i+1].is("UPDATE") && toks[i+2].text == "(" && depthZero(toks, i) {
			if end := matchParen(toks, i+posName); end >= 0 {
				return colSpan{i, end + 1}
			}
		}
	}

	return colSpan{}
}

func cutSpan(item string, toks []ddlTok, c colSpan) string {
	return strings.TrimSpace(strings.TrimRight(item[:toks[c.from].start], " \n\t") + " " +
		strings.TrimLeft(item[toks[c.to-1].end:], " \n\t"))
}

func insertText(item string, pos int, ins string) string {
	return strings.TrimSpace(strings.TrimRight(item[:pos], " \n\t") + " " + ins + " " +
		strings.TrimLeft(item[pos:], " \n\t"))
}

// afterType is the character position after the type and an optional NOT NULL.
func afterType(item string, toks []ddlTok, postgres bool) int {
	at := typeEnd(toks, postgres)
	if at+1 < len(toks) && toks[at].is("NOT") && toks[at+1].is("NULL") {
		at += posName
	}

	if at < len(toks) {
		return toks[at].start
	}

	return len(item)
}

func isGenerated(toks []ddlTok) bool { return hasWord(toks, "AS") || hasWord(toks, "GENERATED") }

func columnType(toks []ddlTok, postgres bool) string {
	end := typeEnd(toks, postgres)
	if end <= 1 {
		return ""
	}

	parts := make([]string, 0, end-1)
	for _, t := range toks[1:end] {
		parts = append(parts, strings.ToUpper(t.text))
	}

	return strings.Join(parts, "")
}

func setColumnDefault(item string, postgres bool, expr string) (string, error) {
	toks, err := tokenizeDDL(item, postgres)
	if err != nil {
		return "", err
	}

	if isGenerated(toks) {
		return "", errInvalid("Cannot set a default on a generated or identity column.")
	}

	if d := defaultSpan(toks, postgres); d.ok() {
		item = cutSpan(item, toks, d)
		if toks, err = tokenizeDDL(item, postgres); err != nil {
			return "", err
		}
	}

	return insertText(item, afterType(item, toks, postgres), "DEFAULT "+expr), nil
}

func dropColumnDefault(item string, postgres bool) (string, error) {
	toks, err := tokenizeDDL(item, postgres)
	if err != nil {
		return "", err
	}

	d := defaultSpan(toks, postgres)
	if !d.ok() {
		return item, nil
	}

	if onUpdateSpan(toks).ok() {
		return "", errPrecondition("Cannot drop the default of a column that has an ON UPDATE expression.")
	}

	return cutSpan(item, toks, d), nil
}

func setOnUpdate(item, expr string) (string, error) {
	toks, err := tokenizeDDL(item, false)
	if err != nil {
		return "", err
	}

	d := defaultSpan(toks, false)
	if !d.ok() {
		return "", errPrecondition("ON UPDATE requires a column with a DEFAULT expression.")
	}

	if u := onUpdateSpan(toks); u.ok() {
		item = cutSpan(item, toks, u)
		if toks, err = tokenizeDDL(item, false); err != nil {
			return "", err
		}

		d = defaultSpan(toks, false)
	}

	return insertText(item, toks[d.to-1].end, "ON UPDATE "+expr), nil
}

func dropOnUpdate(item string) (string, error) {
	toks, err := tokenizeDDL(item, false)
	if err != nil {
		return "", err
	}

	u := onUpdateSpan(toks)
	if !u.ok() {
		return item, nil
	}

	return cutSpan(item, toks, u), nil
}

func setColumnOptions(item string, postgres bool, upd []optPair) (string, error) {
	toks, err := tokenizeDDL(item, postgres)
	if err != nil {
		return "", err
	}

	allowed := map[string]func(optPair) error{
		optCommitTimestamp: boolValue,
		optLocalityGroup:   quotedString,
	}

	if err = checkOptionKeys(upd, allowed, "column"); err != nil {
		return "", err
	}

	for _, p := range upd {
		if p.key == optCommitTimestamp && !p.null() && !strings.EqualFold(p.val, "true") {
			return "", errInvalid("allow_commit_timestamp must be true or null.")
		}

		if p.key == optCommitTimestamp && !p.null() && !strings.HasPrefix(columnType(toks, postgres), "TIMESTAMP") {
			return "", errInvalid("allow_commit_timestamp requires a TIMESTAMP column.")
		}
	}

	return applyOptions(item, postgres, upd, " ")
}

func setPGType(item string, act []ddlTok, stmt string) (string, error) {
	toks, err := tokenizeDDL(item, true)
	if err != nil {
		return "", err
	}

	if len(act) == 0 {
		return "", errInvalid("Syntax error: expected a type.")
	}

	end := len(act)
	for i, t := range act {
		if t.is("USING") {
			end = i

			break
		}
	}

	if end == 0 {
		return "", errInvalid("Syntax error: expected a type.")
	}

	typ := stmt[act[0].start:act[end-1].end]
	te := typeEnd(toks, true)

	rest := ""
	if te < len(toks) {
		rest = " " + item[toks[te].start:]
	}

	return item[:toks[0].end] + " " + typ + rest, nil
}

func setPGNotNull(item string, notNull bool) (string, error) {
	toks, err := tokenizeDDL(item, true)
	if err != nil {
		return "", err
	}

	at := -1

	for i := 0; i+1 < len(toks); i++ {
		if toks[i].is("NOT") && toks[i+1].is("NULL") && depthZero(toks, i) {
			at = i
		}
	}

	switch {
	case notNull && at >= 0, !notNull && at < 0:
		return item, nil
	case notNull:
		return insertText(item, toks[typeEnd(toks, true)-1].end, "NOT NULL"), nil
	default:
		return cutSpan(item, toks, colSpan{at, at + posName}), nil
	}
}

// alterColumn folds ALTER [COLUMN] name <action> into tt. It reports false for actions it does not model.
func (s *ddlSchema) alterColumn(tt *tableText, stmt string, act []ddlTok) (bool, error) {
	col := 1
	if act[1].is("COLUMN") {
		col = 2
	}

	if len(act) <= col+1 || !act[col].word {
		return false, nil
	}

	name, sub := identName(act[col]), act[col+1:]

	i := tt.column(name)
	if i < 0 {
		return true, errNotFound("Column not found: %s.", name)
	}

	item, handled, err := s.columnEdit(tt.items[i], stmt, sub)
	if !handled || err != nil {
		return handled, err
	}

	toks, terr := tokenizeDDL(item, s.postgres)
	if terr != nil {
		return true, terr
	}

	tt.items[i], tt.toks[i] = item, toks

	return true, nil
}

func (s *ddlSchema) columnEdit(item, stmt string, sub []ddlTok) (string, bool, error) {
	if s.postgres {
		return pgColumnEdit(item, stmt, sub)
	}

	switch {
	case startsWith(sub, "SET", "OPTIONS"):
		pairs, _, err := pairsAfter(stmt, sub, posName)
		if err != nil {
			return "", true, err
		}

		out, err := setColumnOptions(item, false, pairs)

		return out, true, err
	case startsWith(sub, "SET", "DEFAULT"):
		out, err := setColumnDefault(item, false, exprText(stmt, sub[posName:], false))

		return out, true, err
	case hasPrefix(sub, "DROP", "DEFAULT"):
		out, err := dropColumnDefault(item, false)

		return out, true, err
	case startsWith(sub, "SET", "ON", "UPDATE"):
		out, err := setOnUpdate(item, exprText(stmt, sub[posType:], false))

		return out, true, err
	case hasPrefix(sub, "DROP", "ON", "UPDATE"):
		out, err := dropOnUpdate(item)

		return out, true, err
	}

	return "", false, nil
}

func pgColumnEdit(item, stmt string, sub []ddlTok) (string, bool, error) {
	var (
		out string
		err error
	)

	switch {
	case startsWith(sub, "SET", "DEFAULT"):
		out, err = setColumnDefault(item, true, exprText(stmt, sub[posName:], true))
	case hasPrefix(sub, "DROP", "DEFAULT"):
		out, err = dropColumnDefault(item, true)
	case startsWith(sub, "TYPE"):
		out, err = setPGType(item, sub[1:], stmt)
	case hasPrefix(sub, "SET", "NOT", "NULL"):
		out, err = setPGNotNull(item, true)
	case hasPrefix(sub, "DROP", "NOT", "NULL"):
		out, err = setPGNotNull(item, false)
	default:
		return "", false, nil
	}

	return out, true, err
}

// exprText is the expression of SET DEFAULT/ON UPDATE: a parenthesised GoogleSQL expression, or the rest of
// the statement in PostgreSQL.
func exprText(stmt string, toks []ddlTok, postgres bool) string {
	if len(toks) == 0 {
		return ""
	}

	if !postgres && toks[0].text == "(" {
		if end := matchParen(toks, 0); end >= 0 {
			return stmt[toks[0].start:toks[end].end]
		}
	}

	return stmt[toks[0].start:toks[len(toks)-1].end]
}

func hasPrefix(toks []ddlTok, words ...string) bool {
	if len(toks) < len(words) {
		return false
	}

	for i, w := range words {
		if !toks[i].is(w) {
			return false
		}
	}

	return true
}

func atoiBounded(s string, lo, hi int) (int, bool) {
	n, err := strconv.Atoi(s)

	return n, err == nil && n >= lo && n <= hi
}
