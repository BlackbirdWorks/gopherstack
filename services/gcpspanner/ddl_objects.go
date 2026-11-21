package gcpspanner

import (
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	minRetention = time.Hour
	maxRetention = 7 * hoursPerDay * time.Hour
)

var durationUnits = map[byte]time.Duration{ //nolint:gochecknoglobals // immutable
	's': time.Second, 'm': time.Minute, 'h': time.Hour, 'd': hoursPerDay * time.Hour,
}

func unquote(v string) string {
	if len(v) >= 2 && (v[0] == '\'' || v[0] == '"') && v[len(v)-1] == v[0] {
		return v[1 : len(v)-1]
	}

	return v
}

// parseRetention reads '1h', '90m', '3d' and the like.
func parseRetention(v string) (time.Duration, bool) {
	s := unquote(v)
	if len(s) < posName {
		return 0, false
	}

	unit, ok := durationUnits[s[len(s)-1]]

	n, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
	if !ok || err != nil || n <= 0 || n > int64(maxRetention/unit) {
		return 0, false
	}

	return time.Duration(n) * unit, true
}

func retentionOption(p optPair) error {
	d, ok := parseRetention(p.val)
	if !ok || d < minRetention || d > maxRetention {
		return errInvalid("version_retention_period must be a duration between 1h and 7d.")
	}

	return nil
}

func optimizerVersion(p optPair) error {
	if n, ok := atoiBounded(p.val, 1, 9); !ok || n == 0 { //nolint:mnd // documented range
		return errInvalid("optimizer_version must be an integer between 1 and 9 or null.")
	}

	return nil
}

func sequenceKind(p optPair) error {
	if unquote(p.val) != "bit_reversed_positive" {
		return errInvalid("default_sequence_kind must be 'bit_reversed_positive' or null.")
	}

	return nil
}

func zoneOption(p optPair) error {
	if err := quotedString(p); err != nil {
		return err
	}

	if _, err := time.LoadLocation("UTC"); err == nil {
		if _, err = time.LoadLocation(unquote(p.val)); err != nil {
			return errInvalid("default_time_zone %s is not an IANA time zone name.", p.val)
		}
	}

	return nil
}

func databaseOptionChecks() map[string]func(optPair) error {
	return map[string]func(optPair) error{
		optDefaultLeader: quotedString, "optimizer_version": optimizerVersion,
		"optimizer_statistics_package": quotedString, "version_retention_period": retentionOption,
		"default_sequence_kind": sequenceKind, "default_time_zone": zoneOption, "read_lease_regions": quotedString,
		optColumnarPolicy: quotedString, "use_unenforced_foreign_key_for_query_optimization": boolValue,
	}
}

func (s *ddlSchema) databaseOptions() int {
	return slices.IndexFunc(s.objs, func(o ddlObject) bool { return o.kind == kindDBOptions })
}

// alterDatabase folds ALTER DATABASE name SET OPTIONS (...) into one statement holding the options in force.
func (s *ddlSchema) alterDatabase(stmt string, toks []ddlTok) error {
	if len(toks) <= posType || !toks[posName].word || !hasPrefix(toks[posType:], "SET", "OPTIONS") {
		s.objs = append(s.objs, ddlObject{text: stmt})

		return nil
	}

	pairs, end, err := pairsAfter(stmt, toks, posType+posName)
	if err != nil {
		return err
	}

	if end != len(toks) {
		return errInvalid("Syntax error: unexpected %q.", toks[end].text)
	}

	if err = checkOptionKeys(pairs, databaseOptionChecks(), "database"); err != nil {
		return err
	}

	if err = s.checkDatabasePairs(pairs); err != nil {
		return err
	}

	name := stmt[toks[posName].start:toks[posName].end]
	i := s.databaseOptions()

	var have []optPair

	if i >= 0 {
		have = s.databaseOptionPairs(i)
		name = s.objs[i].name
	}

	merged := mergePairs(have, pairs)
	text := "ALTER DATABASE " + name + " SET " + renderPairs("OPTIONS", merged)

	switch {
	case i >= 0 && len(merged) == 0:
		s.objs = slices.Delete(s.objs, i, i+1)
	case i >= 0:
		s.objs[i].text = text
	case len(merged) > 0:
		s.objs = append(s.objs, ddlObject{kind: kindDBOptions, key: "database", name: name, text: text})
	}

	return nil
}

func (s *ddlSchema) checkDatabasePairs(pairs []optPair) error {
	for _, p := range pairs {
		switch {
		case p.key == "default_time_zone" && !p.null() && s.hasTables():
			return errPrecondition("default_time_zone can only be set on a database without tables.")
		case p.key == optDefaultLeader && !p.null() && s.leaders != nil && !slices.Contains(s.leaders, unquote(p.val)):
			return errInvalid("default_leader %s is not a leader option of the instance configuration.", p.val)
		case p.key == optDefaultLeader && !p.null() && s.leaders == nil && s.leadersKnown:
			return errInvalid("default_leader requires a multi-region instance configuration.")
		}
	}

	return nil
}

func (s *ddlSchema) hasTables() bool {
	return slices.ContainsFunc(s.objs, func(o ddlObject) bool { return o.kind == kindTable })
}

func (s *ddlSchema) databaseOptionPairs(i int) []optPair {
	toks, err := tokenizeDDL(s.objs[i].text, s.postgres)
	if err != nil {
		return nil
	}

	_, open, end, ok := topLevelClause(toks, "OPTIONS")
	if !ok {
		return nil
	}

	pairs, _ := parsePairs(s.objs[i].text, toks, open, end)

	return pairs
}

// ddlDatabaseOptions reads the ALTER DATABASE options out of a stored DDL.
func ddlDatabaseOptions(ddl []string, postgres bool) map[string]string {
	s := loadSchema(ddl, postgres)
	out := map[string]string{}

	if i := s.databaseOptions(); i >= 0 {
		for _, p := range s.databaseOptionPairs(i) {
			out[p.key] = unquote(p.val)
		}
	}

	return out
}

// alterIndex folds ALTER INDEX name ADD|DROP STORED COLUMN column.
func (s *ddlSchema) alterIndex(stmt string, toks []ddlTok) error {
	at, ifExists := skipIf(toks, posName, "IF", "EXISTS")

	name, at, err := objectName(toks, at)
	if err != nil || at >= len(toks) {
		s.objs = append(s.objs, ddlObject{text: stmt})

		return nil //nolint:nilerr // an unmodelled form is kept verbatim
	}

	i := s.find(kindIndex, name)
	if i < 0 {
		if ifExists {
			return nil
		}

		return errNotFound("Index not found: %s.", name)
	}

	act := toks[at:]

	word, clause := "STORED", "STORING"
	if s.postgres {
		word, clause = "INCLUDE", "INCLUDE"
	}

	switch {
	case hasPrefix(act, "ADD", word, "COLUMN") && len(act) == posType+1:
		return s.storedColumn(i, stmt, act[posType], true, clause)
	case hasPrefix(act, "DROP", word, "COLUMN") && len(act) == posType+1:
		return s.storedColumn(i, stmt, act[posType], false, clause)
	case !s.postgres && hasPrefix(act, "SET", "OPTIONS"):
		return s.indexOptions(i, stmt, act)
	}

	s.objs = append(s.objs, ddlObject{text: stmt})

	return nil
}

func (s *ddlSchema) indexOptions(i int, stmt string, act []ddlTok) error {
	pairs, _, err := pairsAfter(stmt, act, posName)
	if err != nil {
		return err
	}

	allowed := map[string]func(optPair) error{optLocalityGroup: quotedString, optColumnarPolicy: quotedString}
	if err = checkOptionKeys(pairs, allowed, "index"); err != nil {
		return err
	}

	text, err := applyOptions(s.objs[i].text, s.postgres, pairs, " ")
	if err != nil {
		return err
	}

	s.objs[i].text = text

	return nil
}

func (s *ddlSchema) storedColumn(i int, stmt string, col ddlTok, add bool, clause string) error {
	if !col.word {
		return errInvalid("Syntax error: expected a column name.")
	}

	name := identName(col)
	text := s.objs[i].text

	toks, err := tokenizeDDL(text, s.postgres)
	if err != nil {
		return err
	}

	on := slices.IndexFunc(toks, func(t ddlTok) bool { return t.is("ON") })
	if on < 0 {
		return errInvalid("Syntax error: malformed stored index definition.")
	}

	table, keyOpen, err := objectName(toks, on+1)
	if err != nil || keyOpen >= len(toks) || toks[keyOpen].text != "(" {
		return errInvalid("Syntax error: malformed stored index definition.")
	}

	keyEnd := matchParen(toks, keyOpen)

	kw, open, end, has := topLevelClause(toks[keyEnd+1:], clause)
	if has {
		kw, open, end = kw+keyEnd+1, open+keyEnd+1, end+keyEnd+1
	}

	var stored []string

	if has {
		for _, it := range splitTop(toks, open, end) {
			if len(it) > 0 {
				stored = append(stored, text[it[0].start:it[len(it)-1].end])
			}
		}
	}

	pos := slices.IndexFunc(stored, func(c string) bool { return foldKey(identNameText(c)) == foldKey(name) })
	raw := stmt[col.start:col.end]

	switch {
	case add:
		return s.addStored(i, table, name, raw, toks, [4]int{keyEnd, kw, open, end}, stored, pos, clause)
	case pos < 0:
		return errNotFound("Column %s is not a stored column of the index.", name)
	}

	stored = slices.Delete(stored, pos, pos+1)

	s.objs[i].text = replaceStoring(text, toks, [3]int{keyEnd, kw, end}, has, stored, clause)

	return nil
}

func (s *ddlSchema) addStored(
	i int, table, name, raw string, toks []ddlTok, at [4]int, stored []string, pos int, word string,
) error {
	text := s.objs[i].text

	cols := tableColumns(s.objs[s.find(kindTable, table)].text, s.postgres)
	if cols != nil && !cols[foldKey(name)] {
		return errNotFound("Column not found in table %s: %s.", table, name)
	}

	if pos >= 0 {
		return errExists("Column %s is already stored in the index.", name)
	}

	for _, it := range splitTop(toks, slices.IndexFunc(toks, func(t ddlTok) bool { return t.text == "(" }), at[0]) {
		if len(it) > 0 && foldKey(identName(it[0])) == foldKey(name) {
			return errExists("Column %s is already a key column of the index.", name)
		}
	}

	has := at[1] > at[0]
	s.objs[i].text = replaceStoring(text, toks, [3]int{at[0], at[1], at[3]}, has, append(stored, raw), word)

	return nil
}

func replaceStoring(text string, toks []ddlTok, at [3]int, has bool, stored []string, word string) string {
	keyEnd, kw, end := at[0], at[1], at[2]

	clause := ""
	if len(stored) > 0 {
		clause = word + " (" + strings.Join(stored, ", ") + ")"
	}

	switch {
	case has:
		left := strings.TrimRight(text[:toks[kw].start], " \n\t")
		if clause == "" {
			return left + text[toks[end].end:]
		}

		return left + " " + clause + text[toks[end].end:]
	default:
		return text[:toks[keyEnd].end] + " " + clause + text[toks[keyEnd].end:]
	}
}

// alterOptionsObject folds ALTER <kind> name SET OPTIONS (...) into the object's own OPTIONS clause.
func (s *ddlSchema) alterOptionsObject(stmt string, toks []ddlTok, kind string, at int) (bool, error) {
	at, ifExists := skipIf(toks, at, "IF", "EXISTS")

	name, at, err := objectName(toks, at)
	if err != nil || !hasPrefix(toks[at:], "SET", "OPTIONS") {
		return false, nil //nolint:nilerr // an unmodelled form is kept verbatim
	}

	i := s.find(lookupKind(kind), name)
	if i < 0 || s.objs[i].kind != kind {
		if ifExists {
			return true, nil
		}

		return true, errNotFound("%s not found: %s.", kind, name)
	}

	pairs, end, err := pairsAfter(stmt, toks, at+posName)
	if err != nil {
		return true, err
	}

	if end != len(toks) {
		return true, errInvalid("Syntax error: unexpected %q.", toks[end].text)
	}

	if err = checkOptionKeys(pairs, optionChecks(kind), strings.ToLower(kind)); err != nil {
		return true, err
	}

	text, err := applyOptions(s.objs[i].text, s.postgres, pairs, " ")
	if err != nil {
		return true, err
	}

	s.objs[i].text = text

	return true, s.checkSequenceRange(kind, i)
}

// alterPGSequence folds the PostgreSQL ALTER SEQUENCE options: SKIP RANGE min max is kept in the statement, the
// others are accepted and leave the schema unchanged.
func (s *ddlSchema) alterPGSequence(stmt string, toks []ddlTok) (bool, error) {
	at, ifExists := skipIf(toks, posName, "IF", "EXISTS")

	name, at, err := objectName(toks, at)
	if err != nil || at >= len(toks) {
		return false, nil //nolint:nilerr // an unmodelled form is kept verbatim
	}

	i := s.find(kindSequence, name)
	if i < 0 {
		if ifExists {
			return true, nil
		}

		return true, errNotFound("%s not found: %s.", kindSequence, name)
	}

	return s.pgSequenceOptions(i, stmt, toks[at:])
}

func (s *ddlSchema) pgSequenceOptions(i int, stmt string, act []ddlTok) (bool, error) {
	text := s.objs[i].text

	for len(act) > 0 {
		switch {
		case hasPrefix(act, "SKIP", "RANGE") && len(act) >= posType+1:
			lo, hi := act[posName], act[posType]
			if _, perr := strconv.ParseInt(lo.text, 10, 64); perr != nil {
				return true, errInvalid("SKIP RANGE needs integer bounds.")
			}

			if _, perr := strconv.ParseInt(hi.text, 10, 64); perr != nil {
				return true, errInvalid("SKIP RANGE needs integer bounds.")
			}

			text = replaceClauseWords(text, s.postgres, "SKIP", "SKIP RANGE "+stmt[lo.start:hi.end])
			act = act[posType+1:]
		case hasPrefix(act, "NO", "MINVALUE"), hasPrefix(act, "NO", "MAXVALUE"), hasPrefix(act, "NO", "CYCLE"):
			act = act[posName:]
		case hasPrefix(act, "RESTART", "COUNTER") && len(act) > posName:
			act = act[posName:]
			if act[0].is("WITH") && len(act) > 1 {
				act = act[1:]
			}

			act = act[1:]
		default:
			return false, nil
		}
	}

	s.objs[i].text = text

	return true, nil
}

// replaceClauseWords replaces the clause that starts with word, up to the end of its two integer arguments, or
// appends the replacement.
func replaceClauseWords(text string, postgres bool, word, repl string) string {
	toks, err := tokenizeDDL(text, postgres)
	if err != nil {
		return text
	}

	for i, t := range toks {
		if t.is(word) && i+posType < len(toks) {
			return text[:t.start] + repl + text[toks[i+posType].end:]
		}
	}

	return strings.TrimRight(text, " ") + " " + repl
}

func (s *ddlSchema) checkSequenceRange(kind string, i int) error {
	if kind != kindSequence {
		return nil
	}

	toks, err := tokenizeDDL(s.objs[i].text, s.postgres)
	if err != nil {
		return err
	}

	_, open, end, ok := topLevelClause(toks, "OPTIONS")
	if !ok {
		return nil
	}

	pairs, _ := parsePairs(s.objs[i].text, toks, open, end)

	var lo, hi *int64

	for _, p := range pairs {
		n, perr := strconv.ParseInt(p.val, 10, 64)
		if perr != nil {
			continue
		}

		switch p.key {
		case "skip_range_min":
			lo = &n
		case "skip_range_max":
			hi = &n
		}
	}

	if lo != nil != (hi != nil) {
		return errInvalid("skip_range_min and skip_range_max must be set together.")
	}

	if lo != nil && *lo > *hi {
		return errInvalid("skip_range_min must not exceed skip_range_max.")
	}

	return nil
}

func intOption(p optPair) error {
	if _, err := strconv.ParseInt(p.val, 10, 64); err != nil {
		return errInvalid("Option %s must be an integer.", p.key)
	}

	return nil
}

func optionChecks(kind string) map[string]func(optPair) error {
	switch kind {
	case kindSequence:
		return map[string]func(optPair) error{
			"sequence_kind": quotedString, "skip_range_min": intOption, "skip_range_max": intOption,
			"start_with_counter": intOption,
		}
	case kindLocality:
		return map[string]func(optPair) error{"storage": quotedString, "ssd_to_hdd_spill_timespan": quotedString}
	case kindChangeStream:
		return changeStreamChecks()
	default:
		return nil
	}
}

func changeStreamChecks() map[string]func(optPair) error {
	out := map[string]func(optPair) error{
		"retention_period": quotedString, "value_capture_type": quotedString,
	}

	for _, k := range []string{
		"exclude_ttl_deletes", "exclude_insert", "exclude_update", "exclude_delete", "allow_txn_exclusion",
	} {
		out[k] = boolValue
	}

	return out
}
