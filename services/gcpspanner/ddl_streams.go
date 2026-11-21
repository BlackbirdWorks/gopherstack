package gcpspanner

import (
	"slices"
	"strings"
)

// alterChangeStream folds SET FOR, DROP FOR ALL and SET OPTIONS of ALTER CHANGE STREAM.
func (s *ddlSchema) alterChangeStream(stmt string, toks []ddlTok) (bool, error) {
	at, ifExists := skipIf(toks, posType, "IF", "EXISTS")

	name, at, err := objectName(toks, at)
	if err != nil || at >= len(toks) {
		return false, nil //nolint:nilerr // an unmodelled form is kept verbatim
	}

	act := toks[at:]

	if hasPrefix(act, "SET", "OPTIONS") {
		return s.alterOptionsObject(stmt, toks, kindChangeStream, posType)
	}

	i := s.find(kindChangeStream, name)
	if i < 0 {
		if ifExists {
			return true, nil
		}

		return true, errNotFound("%s not found: %s.", kindChangeStream, name)
	}

	switch {
	case hasPrefix(act, "SET", "FOR") && len(act) > posName:
		return true, s.setStreamFor(i, stmt, act[posName:])
	case hasPrefix(act, "DROP", "FOR", "ALL") && len(act) == posType:
		return true, s.dropStreamFor(i)
	}

	return false, nil
}

// streamParts splits a stored CREATE CHANGE STREAM into the text before FOR, the FOR list and the text from
// OPTIONS on.
func (s *ddlSchema) streamParts(i int) (string, string, string, error) {
	text := s.objs[i].text

	toks, err := tokenizeDDL(text, s.postgres)
	if err != nil {
		return "", "", "", err
	}

	forAt, optAt := -1, len(text)

	for j, t := range toks {
		switch {
		case t.is("FOR") && forAt < 0 && depthZero(toks, j):
			forAt = j
		case t.is("OPTIONS") && depthZero(toks, j):
			optAt = t.start
		}
	}

	if forAt < 0 {
		return strings.TrimSpace(text[:optAt]), "", text[optAt:], nil
	}

	return strings.TrimSpace(
			text[:toks[forAt].start],
		), strings.TrimSpace(
			text[toks[forAt].end:optAt],
		), text[optAt:], nil
}

func joinStream(head, forList, options string) string {
	out := head
	if forList != "" {
		out += " FOR " + forList
	}

	if options != "" {
		out += " " + strings.TrimSpace(options)
	}

	return out
}

func (s *ddlSchema) setStreamFor(i int, stmt string, toks []ddlTok) error {
	list := stmt[toks[0].start:toks[len(toks)-1].end]

	if len(toks) != 1 || !toks[0].is("ALL") {
		if err := s.checkStreamTables(stmt, toks); err != nil {
			return err
		}
	}

	head, _, options, err := s.streamParts(i)
	if err != nil {
		return err
	}

	s.objs[i].text = joinStream(head, list, options)

	return nil
}

// checkStreamTables checks that the tables of a FOR list exist and, with a column list, hold those columns.
func (s *ddlSchema) checkStreamTables(stmt string, toks []ddlTok) error {
	for _, it := range splitTop(append(append([]ddlTok{{text: "("}}, toks...), ddlTok{text: ")"}), 0, len(toks)+1) {
		name, at, err := objectName(it, 0)
		if err != nil {
			return err
		}

		ti := s.find(kindTable, name)
		if ti < 0 {
			return errNotFound("Table not found: %s.", name)
		}

		if at >= len(it) {
			continue
		}

		if it[at].text != "(" {
			return errInvalid("Syntax error: unexpected %q in the FOR clause.", it[at].text)
		}

		cols := tableColumns(s.objs[ti].text, s.postgres)

		for _, c := range splitTop(it, at, matchParen(it, at)) {
			if len(c) == 1 && cols != nil && !cols[foldKey(identName(c[0]))] {
				return errNotFound("Column not found in table %s: %s.", name, stmt[c[0].start:c[0].end])
			}
		}
	}

	return nil
}

func (s *ddlSchema) dropStreamFor(i int) error {
	head, forList, options, err := s.streamParts(i)
	if err != nil {
		return err
	}

	if !strings.EqualFold(forList, "ALL") {
		return errPrecondition("DROP FOR ALL requires a change stream that watches ALL.")
	}

	s.objs[i].text = joinStream(head, "", options)

	return nil
}

// alterBundle folds ALTER PROTO BUNDLE INSERT/UPDATE/DELETE (types...).
func (s *ddlSchema) alterBundle(stmt string, toks []ddlTok) (bool, error) {
	i := s.find(kindBundle, kindBundle)
	if i < 0 {
		return true, errNotFound("There is no proto bundle to alter.")
	}

	current, err := s.bundleTypes(i)
	if err != nil {
		return true, err
	}

	at := posType

	if at >= len(toks) {
		return false, nil
	}

	for at < len(toks) {
		verb := strings.ToUpper(toks[at].text)
		if !toks[at].word || verb != privInsert && verb != privUpdate && verb != kwDelete ||
			at+1 >= len(toks) || toks[at+1].text != "(" {
			return false, nil
		}

		end := matchParen(toks, at+1)
		if end < 0 {
			return true, errInvalid("Syntax error: unbalanced parentheses.")
		}

		names := make([]string, 0, len(toks))

		for _, it := range splitTop(toks, at+1, end) {
			if len(it) == 0 {
				return true, errInvalid("Syntax error: expected a type name.")
			}

			names = append(names, strings.TrimSpace(stmt[it[0].start:it[len(it)-1].end]))
		}

		if current, err = s.bundleVerb(verb, names, current); err != nil {
			return true, err
		}

		at = end + 1
	}

	s.objs[i].text = "CREATE PROTO BUNDLE (" + strings.Join(current, ", ") + ")"

	return true, nil
}

func (s *ddlSchema) bundleVerb(verb string, names, current []string) ([]string, error) {
	for _, n := range names {
		at := slices.IndexFunc(current, func(c string) bool { return c == n })

		switch {
		case verb == privInsert && at >= 0:
			return nil, errExists("The proto bundle already contains %s.", n)
		case verb == privInsert:
			current = append(current, n)
		case at < 0:
			return nil, errNotFound("The proto bundle does not contain %s.", n)
		case verb == kwDelete:
			if s.typeInUse(n) {
				return nil, errPrecondition("Cannot delete %s from the proto bundle: a column uses it.", n)
			}

			current = slices.Delete(current, at, at+1)
		}
	}

	return current, nil
}

func (s *ddlSchema) typeInUse(name string) bool {
	return slices.ContainsFunc(s.objs, func(o ddlObject) bool {
		return o.kind == kindTable && strings.Contains(o.text, "<"+name+">")
	})
}

func (s *ddlSchema) bundleTypes(i int) ([]string, error) {
	toks, err := tokenizeDDL(s.objs[i].text, s.postgres)
	if err != nil {
		return nil, err
	}

	open := slices.IndexFunc(toks, func(t ddlTok) bool { return t.text == "(" })
	if open < 0 {
		return nil, nil
	}

	var out []string

	for _, it := range splitTop(toks, open, matchParen(toks, open)) {
		if len(it) > 0 {
			out = append(out, strings.TrimSpace(s.objs[i].text[it[0].start:it[len(it)-1].end]))
		}
	}

	return out, nil
}
