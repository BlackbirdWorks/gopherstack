package gcpspanner

import (
	"slices"
	"strings"
)

const (
	grantKindRole      = "ROLE GRANT"
	grantKindAllTables = "ALL TABLES IN SCHEMA"
	grantKindAllViews  = "ALL VIEWS IN SCHEMA"
	grantKindAllStream = "ALL CHANGE STREAMS IN SCHEMA"
	grantKindAllSeqs   = "ALL SEQUENCES IN SCHEMA"

	privSelect  = "SELECT"
	privInsert  = "INSERT"
	privUpdate  = "UPDATE"
	privExecute = "EXECUTE"
	privUsage   = "USAGE"
)

func privOrder() []string {
	return []string{privSelect, privInsert, privUpdate, kwDelete, privExecute, privUsage}
}

// grantPrivs lists the privileges each object kind accepts.
var grantPrivs = map[string][]string{ //nolint:gochecknoglobals // immutable catalog
	kindTable:          {privSelect, privInsert, privUpdate, kwDelete},
	"VIEW":             {privSelect},
	kindChangeStream:   {privSelect},
	kindTableFunc:      {privExecute},
	kindSequence:       {privSelect, privUpdate},
	kindModel:          {privExecute},
	kindSchema:         {privUsage},
	grantKindAllTables: {privSelect, privInsert, privUpdate, kwDelete},
	grantKindAllViews:  {privSelect},
	grantKindAllStream: {privSelect},
	grantKindAllSeqs:   {privSelect, privUpdate},
}

// bulkGrantKinds are the ALL <objects> IN SCHEMA forms of the DDL references.
func bulkGrantKinds() []string {
	return []string{grantKindAllTables, grantKindAllViews, grantKindAllStream, grantKindAllSeqs}
}

func bulkGrantKind(toks []ddlTok) (string, bool) {
	for _, k := range bulkGrantKinds() {
		if hasPrefix(toks, strings.Fields(k)...) {
			return k, true
		}
	}

	return "", false
}

// grantObjectKinds maps the words after ON to the schema kind that must hold the object.
var grantObjectKinds = map[string]string{ //nolint:gochecknoglobals // immutable catalog
	kindTable: kindTable, "VIEW": kindView, kindChangeStream: kindChangeStream, kindSequence: kindSequence,
	kindModel: kindModel, kindSchema: kindSchema,
}

type grantPriv struct {
	name string
	cols []string
}

func (p grantPriv) columnLevel() bool { return len(p.cols) > 0 }

// grantRec is one canonical GRANT: the privileges of a role on an object, or the roles granted to a role.
type grantRec struct {
	kind    string
	object  string
	role    string
	privs   []grantPriv
	granted []string
}

func (g grantRec) key(s *ddlSchema) string {
	if g.kind == grantKindRole {
		return foldKey(g.role) + "|" + grantKindRole
	}

	return foldKey(g.role) + "|" + g.kind + "|" + s.key(g.object)
}

func (g grantRec) text(postgres bool) string {
	to := " TO ROLE "
	if postgres {
		to = " TO "
	}

	if g.kind == grantKindRole {
		prefix := "GRANT ROLE "
		if postgres {
			prefix = "GRANT "
		}

		return prefix + strings.Join(g.granted, ", ") + to + g.role
	}

	parts := make([]string, 0, len(g.privs))

	for _, p := range g.privs {
		if p.columnLevel() {
			parts = append(parts, p.name+"("+strings.Join(p.cols, ", ")+")")
		} else {
			parts = append(parts, p.name)
		}
	}

	return "GRANT " + strings.Join(parts, ", ") + " ON " + g.kind + " " + g.object + to + g.role
}

type grantStmt struct {
	kind       string
	privs      []grantPriv
	objects    []string
	roles      []string
	roleGrants []string
	revoke     bool
}

func isPrivilege(t ddlTok) bool {
	return t.word && slices.Contains(privOrder(), strings.ToUpper(t.text)) && !strings.ContainsAny(t.text[:1], "`\"")
}

// nameList reads comma separated object names from toks[at:]; it returns the raw texts and the next index.
func nameList(stmt string, toks []ddlTok, at int) ([]string, int, error) {
	var out []string

	for {
		_, next, err := objectName(toks, at)
		if err != nil {
			return nil, 0, err
		}

		out = append(out, stmt[toks[at].start:toks[next-1].end])

		at = next
		if at < len(toks) && toks[at].text == "," {
			at++

			continue
		}

		return out, at, nil
	}
}

func parseGrantStmt(stmt string, toks []ddlTok, postgres bool) (grantStmt, error) {
	g := grantStmt{revoke: toks[0].is("REVOKE")}
	sep := "TO"

	if g.revoke {
		sep = "FROM"
	}

	if len(toks) < posName {
		return g, errInvalid("Syntax error: incomplete %s statement.", toks[0].text)
	}

	at := 1

	if toks[1].is("ROLE") && !postgres || postgres && !isPrivilege(toks[1]) && !toks[1].is("ALL") {
		if !postgres {
			at++
		}

		var err error
		if g.roleGrants, at, err = nameList(stmt, toks, at); err != nil {
			return g, err
		}

		g.kind = grantKindRole

		return g, grantTarget(&g, stmt, toks, at, sep, !postgres)
	}

	privEnd := slices.IndexFunc(toks, func(t ddlTok) bool { return t.is("ON") })
	if privEnd < 0 {
		return g, errInvalid("Syntax error: expected ON.")
	}

	privs, err := parsePrivs(stmt, toks[1:privEnd])
	if err != nil {
		return g, err
	}

	g.privs = privs
	at = privEnd + 1

	if at, err = parseGrantObjects(stmt, toks, at, postgres, &g); err != nil {
		return g, err
	}

	return g, grantTarget(&g, stmt, toks, at, sep, !postgres)
}

func parsePrivs(stmt string, toks []ddlTok) ([]grantPriv, error) {
	if len(toks) == 0 {
		return nil, errInvalid("Syntax error: expected privileges.")
	}

	var out []grantPriv

	for _, it := range splitTop(append(append([]ddlTok{{text: "("}}, toks...), ddlTok{text: ")"}), 0, len(toks)+1) {
		if len(it) == 0 || !isPrivilege(it[0]) {
			return nil, errInvalid("Syntax error: expected SELECT, INSERT, UPDATE, DELETE, EXECUTE or USAGE.")
		}

		p := grantPriv{name: strings.ToUpper(it[0].text)}

		if len(it) > 1 {
			if it[1].text != "(" || it[len(it)-1].text != ")" {
				return nil, errInvalid("Syntax error: expected a column list.")
			}

			for _, c := range splitTop(it[1:], 0, len(it)-1-1) {
				if len(c) != 1 || !c[0].word {
					return nil, errInvalid("Syntax error: expected a column name.")
				}

				p.cols = append(p.cols, stmt[c[0].start:c[0].end])
			}
		}

		out = append(out, p)
	}

	return out, nil
}

func isBulkGrant(toks []ddlTok) bool {
	_, ok := bulkGrantKind(toks)

	return ok
}

func parseGrantObjects(stmt string, toks []ddlTok, at int, postgres bool, g *grantStmt) (int, error) {
	switch {
	case isBulkGrant(toks[at:]):
		g.kind, _ = bulkGrantKind(toks[at:])
		at += len(strings.Fields(g.kind))
	case hasPrefix(toks[at:], "CHANGE", "STREAM"):
		g.kind, at = kindChangeStream, at+posName
	case hasPrefix(toks[at:], "TABLE", "FUNCTION"):
		g.kind, at = kindTableFunc, at+posName
	case at < len(toks) && toks[at].word && grantObjectKinds[strings.ToUpper(toks[at].text)] != "":
		g.kind, at = strings.ToUpper(toks[at].text), at+1
	case postgres:
		g.kind = kindTable
	default:
		return 0, errInvalid("Syntax error: expected TABLE, VIEW, CHANGE STREAM, TABLE FUNCTION, SEQUENCE or MODEL.")
	}

	if at >= len(toks) {
		return 0, errInvalid("Syntax error: expected an object name.")
	}

	var err error

	g.objects, at, err = nameList(stmt, toks, at)

	return at, err
}

func grantTarget(g *grantStmt, stmt string, toks []ddlTok, at int, sep string, roleKeyword bool) error {
	if at >= len(toks) || !toks[at].is(sep) {
		return errInvalid("Syntax error: expected %s.", sep)
	}

	at++

	if roleKeyword {
		if at >= len(toks) || !toks[at].is("ROLE") {
			return errInvalid("Syntax error: expected ROLE.")
		}

		at++
	}

	roles, end, err := nameList(stmt, toks, at)
	if err != nil {
		return err
	}

	if end != len(toks) {
		return errInvalid("Syntax error: unexpected %q.", toks[end].text)
	}

	g.roles = roles

	return nil
}

func (s *ddlSchema) grantIndex(key string) int {
	return slices.IndexFunc(s.objs, func(o ddlObject) bool { return o.kind == kindGrant && o.key == key })
}

func (s *ddlSchema) loadGrantRec(text string, toks []ddlTok) (grantRec, bool) {
	g, err := parseGrantStmt(text, toks, s.postgres)
	if err != nil || len(g.roles) != 1 || g.revoke {
		return grantRec{}, false
	}

	rec := grantRec{kind: g.kind, role: g.roles[0], privs: g.privs, granted: g.roleGrants}
	if len(g.objects) == 1 {
		rec.object = g.objects[0]
	}

	return rec, rec.kind == grantKindRole && len(rec.granted) > 0 || rec.object != ""
}

func (s *ddlSchema) checkRole(name string) error {
	if s.find(kindRole, name) < 0 {
		return errNotFound("Role not found: %s.", name)
	}

	return nil
}

// isDefaultSchema reports the schema every database has: public in PostgreSQL, _default in GoogleSQL.
func (s *ddlSchema) isDefaultSchema(name string) bool {
	k := s.key(name)

	return k == "public" || !s.postgres && (k == "_default" || k == "default")
}

func (s *ddlSchema) checkGrantObject(g *grantStmt, obj string) error {
	bare := identNameText(obj)

	if slices.Contains(bulkGrantKinds(), g.kind) {
		if s.find(kindSchema, bare) < 0 && !s.isDefaultSchema(bare) {
			return errNotFound("Schema not found: %s.", obj)
		}

		return nil
	}

	if g.kind == kindTableFunc {
		stream, ok := strings.CutPrefix(foldKey(bare), "read_")
		if !ok || s.find(kindChangeStream, stream) < 0 {
			return errNotFound("Table function not found: %s.", obj)
		}

		return nil
	}

	kind := grantObjectKinds[g.kind]

	if kind == kindSchema && s.isDefaultSchema(bare) {
		return nil
	}

	i := s.find(kind, bare)
	if i < 0 {
		return errNotFound("%s not found: %s.", g.kind, obj)
	}

	for _, p := range g.privs {
		if err := s.checkGrantColumns(p, g.kind, s.objs[i].text, obj); err != nil {
			return err
		}
	}

	return nil
}

func (s *ddlSchema) checkGrantColumns(p grantPriv, kind, text, obj string) error {
	if !p.columnLevel() {
		return nil
	}

	if kind != kindTable || p.name == kwDelete {
		return errInvalid("Column-level %s privileges are not supported on %s.", p.name, kind)
	}

	cols := tableColumns(text, s.postgres)

	for _, c := range p.cols {
		if cols != nil && !cols[foldKey(identNameText(c))] {
			return errNotFound("Column not found in table %s: %s.", obj, c)
		}
	}

	return nil
}

func identNameText(raw string) string {
	if len(raw) >= 2 && (raw[0] == '`' || raw[0] == '"') {
		return strings.ReplaceAll(raw[1:len(raw)-1], raw[:1]+raw[:1], raw[:1])
	}

	return raw
}

func (s *ddlSchema) validateGrant(g *grantStmt) error {
	if g.kind == grantKindRole {
		for _, r := range g.roleGrants {
			if err := s.checkRole(identNameText(r)); err != nil {
				return err
			}
		}

		return nil
	}

	for _, p := range g.privs {
		if !slices.Contains(grantPrivs[g.kind], p.name) {
			return errInvalid("%s is not a privilege of %s.", p.name, g.kind)
		}
	}

	for _, o := range g.objects {
		if err := s.checkGrantObject(g, o); err != nil {
			return err
		}
	}

	return nil
}

func (s *ddlSchema) grantStatement(stmt string, toks []ddlTok) error {
	g, err := parseGrantStmt(stmt, toks, s.postgres)
	if err != nil {
		return err
	}

	if err = s.validateGrant(&g); err != nil {
		return err
	}

	for _, r := range g.roles {
		if err = s.checkRole(identNameText(r)); err != nil {
			return err
		}
	}

	for _, role := range g.roles {
		if g.kind == grantKindRole {
			s.applyRoleGrant(g, role)

			continue
		}

		for _, obj := range g.objects {
			s.applyObjectGrant(g, role, obj)
		}
	}

	return nil
}

func (s *ddlSchema) apply2(rec grantRec, keep bool) {
	key := rec.key(s)
	i := s.grantIndex(key)

	switch {
	case i >= 0 && keep:
		s.objs[i].text = rec.text(s.postgres)
	case i >= 0:
		s.objs = slices.Delete(s.objs, i, i+1)
	case keep:
		s.objs = append(s.objs, ddlObject{kind: kindGrant, key: key, name: key, text: rec.text(s.postgres)})
	}
}

func (s *ddlSchema) current(rec grantRec) grantRec {
	if i := s.grantIndex(rec.key(s)); i >= 0 {
		toks, err := tokenizeDDL(s.objs[i].text, s.postgres)
		if err == nil {
			if have, ok := s.loadGrantRec(s.objs[i].text, toks); ok {
				return have
			}
		}
	}

	return grantRec{kind: rec.kind, object: rec.object, role: rec.role}
}

func (s *ddlSchema) applyObjectGrant(g grantStmt, role, obj string) {
	rec := s.current(grantRec{kind: g.kind, object: obj, role: role})

	if g.revoke {
		rec.privs = revokePrivs(rec.privs, g.privs)
	} else {
		rec.privs = grantMerge(rec.privs, g.privs)
	}

	s.apply2(rec, len(rec.privs) > 0)
}

func (s *ddlSchema) applyRoleGrant(g grantStmt, role string) {
	rec := s.current(grantRec{kind: grantKindRole, role: role})

	for _, r := range g.roleGrants {
		i := slices.IndexFunc(
			rec.granted,
			func(x string) bool { return foldKey(identNameText(x)) == foldKey(identNameText(r)) },
		)

		switch {
		case g.revoke && i >= 0:
			rec.granted = slices.Delete(rec.granted, i, i+1)
		case !g.revoke && i < 0:
			rec.granted = append(rec.granted, r)
		}
	}

	s.apply2(rec, len(rec.granted) > 0)
}

func grantMerge(have, add []grantPriv) []grantPriv {
	out := slices.Clone(have)

	for _, p := range add {
		i := slices.IndexFunc(out, func(x grantPriv) bool { return x.name == p.name })

		switch {
		case i < 0:
			out = append(out, grantPriv{name: p.name, cols: slices.Clone(p.cols)})
		case !p.columnLevel():
			out[i].cols = nil
		case out[i].columnLevel():
			for _, c := range p.cols {
				if !slices.ContainsFunc(
					out[i].cols,
					func(x string) bool { return foldKey(identNameText(x)) == foldKey(identNameText(c)) },
				) {
					out[i].cols = append(out[i].cols, c)
				}
			}
		}
	}

	slices.SortStableFunc(out, func(a, b grantPriv) int {
		return slices.Index(privOrder(), a.name) - slices.Index(privOrder(), b.name)
	})

	return out
}

func revokePrivs(have, drop []grantPriv) []grantPriv {
	out := slices.Clone(have)

	for _, p := range drop {
		i := slices.IndexFunc(out, func(x grantPriv) bool { return x.name == p.name })
		if i < 0 {
			continue
		}

		if !p.columnLevel() {
			out = slices.Delete(out, i, i+1)

			continue
		}

		if !out[i].columnLevel() {
			continue
		}

		out[i].cols = slices.DeleteFunc(slices.Clone(out[i].cols), func(c string) bool {
			return slices.ContainsFunc(
				p.cols,
				func(x string) bool { return foldKey(identNameText(x)) == foldKey(identNameText(c)) },
			)
		})

		if len(out[i].cols) == 0 {
			out = slices.Delete(out, i, i+1)
		}
	}

	return out
}

// dropGrants removes the grants that refer to a dropped object, or to a dropped role.
func (s *ddlSchema) dropGrants(kind, name string) {
	k := s.key(name)

	s.objs = slices.DeleteFunc(s.objs, func(o ddlObject) bool {
		if o.kind != kindGrant {
			return false
		}

		toks, err := tokenizeDDL(o.text, s.postgres)
		if err != nil {
			return false
		}

		g, ok := s.loadGrantRec(o.text, toks)
		if !ok {
			return false
		}

		if kind == kindRole {
			return foldKey(identNameText(g.role)) == foldKey(name) ||
				slices.ContainsFunc(g.granted, func(x string) bool {
					return foldKey(identNameText(x)) == foldKey(name)
				})
		}

		streamFunc := kind == kindChangeStream && g.kind == kindTableFunc && foldKey(g.object) == "read_"+k

		return streamFunc || g.kind != grantKindRole && grantObjectKinds[g.kind] == kind && s.key(g.object) == k
	})
}

// renameGrantTable follows a table rename in the grants on it.
func (s *ddlSchema) renameGrantTable(old, raw string) {
	k := s.key(old)

	for i := range s.objs {
		o := &s.objs[i]
		if o.kind != kindGrant {
			continue
		}

		toks, err := tokenizeDDL(o.text, s.postgres)
		if err != nil {
			continue
		}

		g, ok := s.loadGrantRec(o.text, toks)
		if !ok || g.kind != kindTable || s.key(g.object) != k {
			continue
		}

		g.object = raw
		o.key, o.name, o.text = g.key(s), g.key(s), g.text(s.postgres)
	}
}
