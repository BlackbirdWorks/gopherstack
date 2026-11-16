package gcpspanner

import (
	"slices"
	"strings"
)

// tableText is a stored CREATE TABLE split into its column items and the text after the closing parenthesis.
type tableText struct {
	head  string
	tail  string
	items []string
	names []string
	toks  [][]ddlTok

	postgres bool
}

func parseTableText(text string, postgres bool) (*tableText, bool) {
	toks, err := tokenizeDDL(text, postgres)
	if err != nil {
		return nil, false
	}

	_, at := classify(toks)
	at, _ = skipIf(toks, at, "IF", "NOT", "EXISTS")

	_, at, err = objectName(toks, at)
	if err != nil || at >= len(toks) || toks[at].text != "(" {
		return nil, false
	}

	closeIdx := matchParen(toks, at)
	if closeIdx < 0 {
		return nil, false
	}

	t := &tableText{head: text[:toks[at].start], tail: text[toks[closeIdx].end:], postgres: postgres}

	for _, it := range splitTop(toks, at, closeIdx) {
		if len(it) == 0 {
			return nil, false
		}

		t.items = append(t.items, text[it[0].start:it[len(it)-1].end])
		t.toks = append(t.toks, it)

		n, _ := columnName(it)
		t.names = append(t.names, foldKey(n))
	}

	return t, true
}

func (t *tableText) String() string {
	return t.head + "(\n  " + strings.Join(t.items, ",\n  ") + "\n)" + t.tail
}

func (t *tableText) column(name string) int { return slices.Index(t.names, foldKey(name)) }

func (t *tableText) constraint(name string) int {
	return slices.IndexFunc(t.toks, func(it []ddlTok) bool {
		return len(it) > 1 && it[0].is("CONSTRAINT") && foldKey(identName(it[1])) == foldKey(name)
	})
}

func (t *tableText) inPrimaryKey(name string) bool {
	toks, err := tokenizeDDL(t.tail, t.postgres)
	if err != nil {
		return false
	}

	for i, tk := range toks {
		if tk.is("PRIMARY") && i+posName < len(toks) && toks[i+posName].text == "(" {
			end := matchParen(toks, i+posName)
			if end < 0 {
				return false
			}

			for _, k := range splitTop(toks, i+posName, end) {
				if len(k) > 0 && foldKey(identName(k[0])) == foldKey(name) {
					return true
				}
			}
		}
	}

	return false
}

func (s *ddlSchema) alter(stmt string, toks []ddlTok) error {
	if len(toks) <= posName || !toks[1].is(kindTable) {
		s.objs = append(s.objs, ddlObject{text: stmt})

		return nil
	}

	at, ifExists := skipIf(toks, posName, "IF", "EXISTS")

	name, at, err := objectName(toks, at)
	if err != nil {
		return err
	}

	i := s.find(kindTable, name)
	if i < 0 {
		if ifExists {
			return nil
		}

		return errNotFound("Table not found: %s.", name)
	}

	tt, ok := parseTableText(s.objs[i].text, s.postgres)
	if !ok || at >= len(toks) {
		s.objs = append(s.objs, ddlObject{text: stmt})

		return nil
	}

	folded, err := s.alterAction(tt, stmt, toks[at:])
	if err != nil {
		return err
	}

	if !folded {
		s.objs = append(s.objs, ddlObject{text: stmt})

		return nil
	}

	s.objs[i].text = tt.String()

	return nil
}

// alterAction folds one ALTER TABLE action into tt. It reports false for an action the schema does not model,
// which the caller keeps verbatim.
func (s *ddlSchema) alterAction(tt *tableText, stmt string, act []ddlTok) (bool, error) {
	if folded, err := s.columnAction(tt, stmt, act); folded || err != nil {
		return folded, err
	}

	return constraintAction(tt, stmt, act)
}

func startsWith(toks []ddlTok, words ...string) bool {
	if len(toks) <= len(words) {
		return false
	}

	for i, w := range words {
		if !toks[i].is(w) {
			return false
		}
	}

	return true
}

func (s *ddlSchema) columnAction(tt *tableText, stmt string, act []ddlTok) (bool, error) {
	switch {
	case startsWith(act, "ADD", "COLUMN"), s.bareAddColumn(act):
		return true, addColumn(tt, stmt, act)
	case startsWith(act, "DROP", "COLUMN"):
		at, _ := skipIf(act, posName, "IF", "EXISTS")
		if at >= len(act) {
			return true, errInvalid("Syntax error: expected a column name.")
		}

		return true, tt.dropColumn(identName(act[at]))
	case !s.postgres && startsWith(act, "ALTER", "COLUMN") && len(act) > posType && act[posName].word &&
		!act[posType].is("SET") && !act[posType].is("DROP"):
		return true, tt.alterColumn(stmt, act)
	}

	return false, nil
}

// bareAddColumn is GoogleSQL's ADD name type without the COLUMN keyword, as opposed to the other ADD actions.
func (s *ddlSchema) bareAddColumn(act []ddlTok) bool {
	return !s.postgres && startsWith(act, "ADD") && len(act) > posName && act[1].word &&
		!isConstraintStart(act[1]) && !act[1].is("ROW") && !act[1].is("SYNONYM")
}

func constraintAction(tt *tableText, stmt string, act []ddlTok) (bool, error) {
	switch {
	case startsWith(act, "ADD") && isConstraintStart(act[1]):
		return true, addConstraint(tt, stmt, act)
	case startsWith(act, "DROP", "CONSTRAINT"):
		return true, tt.dropConstraint(identName(act[posName]))
	case startsWith(act, "SET", "ON", "DELETE") && len(act) > posType:
		return true, tt.setOnDelete(stmt, act)
	}

	return false, nil
}

func isConstraintStart(t ddlTok) bool {
	return t.is("CONSTRAINT") || t.is("FOREIGN") || t.is("CHECK")
}

func addColumn(tt *tableText, stmt string, act []ddlTok) error {
	from := 1
	if act[1].is("COLUMN") {
		from = 2
	}

	from, ifNot := skipIf(act, from, "IF", "NOT", "EXISTS")

	if from >= len(act) || !act[from].word {
		return errInvalid("Syntax error: expected a column definition.")
	}

	def := act[from:]
	n := foldKey(identName(def[0]))

	if tt.column(n) >= 0 {
		if ifNot {
			return nil
		}

		return errExists("Duplicate column name %s.", identName(def[0]))
	}

	tt.items = append(tt.items, stmt[def[0].start:def[len(def)-1].end])
	tt.names = append(tt.names, n)
	tt.toks = append(tt.toks, def)

	return nil
}

func addConstraint(tt *tableText, stmt string, act []ddlTok) error {
	item := act[1:]
	if item[0].is("CONSTRAINT") {
		if len(item) < posName {
			return errInvalid("Syntax error: expected a constraint name.")
		}

		if tt.constraint(identName(item[1])) >= 0 {
			return errExists("Duplicate constraint name %s.", identName(item[1]))
		}
	}

	tt.items = append(tt.items, stmt[item[0].start:item[len(item)-1].end])
	tt.names = append(tt.names, "")
	tt.toks = append(tt.toks, item)

	return nil
}

func (t *tableText) remove(i int) {
	t.items = slices.Delete(t.items, i, i+1)
	t.names = slices.Delete(t.names, i, i+1)
	t.toks = slices.Delete(t.toks, i, i+1)
}

func (t *tableText) dropColumn(name string) error {
	i := t.column(name)
	if i < 0 {
		return errNotFound("Column not found: %s.", name)
	}

	if t.inPrimaryKey(name) {
		return errPrecondition("Cannot drop column %s: it is part of the primary key.", name)
	}

	if len(t.items) == 1 {
		return errPrecondition("Cannot drop the last column of a table.")
	}

	t.remove(i)

	return nil
}

func (t *tableText) dropConstraint(name string) error {
	i := t.constraint(name)
	if i < 0 {
		return errNotFound("Constraint not found: %s.", name)
	}

	t.remove(i)

	return nil
}

func (t *tableText) alterColumn(stmt string, act []ddlTok) error {
	name := identName(act[2])

	i := t.column(name)
	if i < 0 {
		return errNotFound("Column not found: %s.", name)
	}

	def := act[2:]
	t.items[i] = stmt[def[0].start:def[len(def)-1].end]
	t.toks[i] = def

	return nil
}

func (t *tableText) setOnDelete(_ string, act []ddlTok) error {
	var action string

	switch {
	case act[posType].is("CASCADE"):
		action = "CASCADE"
	case act[posType].is("NO") && len(act) > posType+1 && act[posType+1].is("ACTION"):
		action = "NO ACTION"
	default:
		return errInvalid("Syntax error: expected CASCADE or NO ACTION.")
	}

	toks, err := tokenizeDDL(t.tail, t.postgres)
	if err != nil {
		return err
	}

	if !slices.ContainsFunc(toks, func(k ddlTok) bool { return k.is("INTERLEAVE") }) {
		return errPrecondition("Only interleaved tables have an ON DELETE action.")
	}

	for i, k := range toks {
		if k.is("ON") && i+1 < len(toks) && toks[i+1].is("DELETE") {
			end := i + posType
			if i+posName < len(toks) && toks[i+posName].is("NO") {
				end++
			}

			t.tail = t.tail[:k.start] + "ON DELETE " + action + t.tail[toks[min(end, len(toks))-1].end:]

			return nil
		}
	}

	t.tail += " ON DELETE " + action

	return nil
}
