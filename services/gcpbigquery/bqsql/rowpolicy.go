package bqsql

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// DefaultUser is the identity SESSION_USER() reports when the request carries none.
const DefaultUser = "gopherstack@localhost"

const (
	granteeAllUsers   = "allUsers"
	granteeAllAuthed  = "allAuthenticatedUsers"
	granteeUser       = "user:"
	granteeServiceAcc = "serviceAccount:"
	granteeGroup      = "group:"
	granteeDomain     = "domain:"
	granteePrincipal  = "principal://"
	principalSubject  = "/subject/"
	maxPolicyDepth    = 1
)

// RowPolicyData is a row access policy as the engine sees it.
type RowPolicyData struct {
	Name     string
	Filter   string
	Grantees []string
}

// ValidGrantee reports whether g is one of the IAM member forms row access policies accept.
func ValidGrantee(g string) bool {
	switch {
	case g == granteeAllUsers, g == granteeAllAuthed:
		return true
	case strings.HasPrefix(g, granteeUser), strings.HasPrefix(g, granteeServiceAcc),
		strings.HasPrefix(g, granteeGroup), strings.HasPrefix(g, granteeDomain):
		_, v, _ := strings.Cut(g, ":")

		return strings.TrimSpace(v) != ""
	case strings.HasPrefix(g, granteePrincipal):
		return strings.Contains(g, principalSubject)
	}

	return false
}

// GranteeMatches reports whether the caller identity user is covered by grantee. Group membership is not known to
// the emulator, so group grantees never match.
func GranteeMatches(grantee, user string) bool {
	switch {
	case grantee == granteeAllUsers, grantee == granteeAllAuthed:
		return true
	case strings.HasPrefix(grantee, granteeUser):
		return strs.Equal(strings.TrimPrefix(grantee, granteeUser), user)
	case strings.HasPrefix(grantee, granteeServiceAcc):
		return strs.Equal(strings.TrimPrefix(grantee, granteeServiceAcc), user)
	case strings.HasPrefix(grantee, granteeDomain):
		_, dom, ok := strings.Cut(user, "@")

		return ok && strs.Equal(strings.TrimPrefix(grantee, granteeDomain), dom)
	case strings.HasPrefix(grantee, granteePrincipal):
		_, id, _ := strings.Cut(grantee, principalSubject)

		return strs.Equal(id, user)
	}

	return false
}

// FilterReferences reports whether the policy filter mentions column as an identifier.
func FilterReferences(filter, column string) bool {
	toks, err := lex(filter)
	if err != nil {
		return false
	}

	for _, t := range toks {
		if (t.kind == tkIdent || t.kind == tkQuoted) && strs.Equal(t.text, column) {
			return true
		}
	}

	return false
}

func (p *parser) isRowPolicy() bool { return p.kw("ROW") && p.peekNext().isKw("ACCESS") }

func (p *parser) createRowPolicy(st *Statement) (*Statement, error) {
	p.i += 2

	if err := p.expectKw("POLICY"); err != nil {
		return nil, err
	}

	st.Kind = StmtCreateRowPolicy

	if p.kw("IF") {
		p.i++

		if err := p.expectKw(litNot); err != nil {
			return nil, err
		}

		if err := p.expectKw("EXISTS"); err != nil {
			return nil, err
		}

		st.ifNot = true
	}

	name, err := p.anyIdent()
	if err != nil {
		return nil, err
	}

	st.policyName = name

	if err = p.onTable(st); err != nil {
		return nil, err
	}

	if err = p.expectKw("GRANT"); err != nil {
		return nil, err
	}

	if err = p.expectKw("TO"); err != nil {
		return nil, err
	}

	if err = p.granteeList(st); err != nil {
		return nil, err
	}

	if err = p.expectKw("FILTER"); err != nil {
		return nil, err
	}

	if err = p.expectKw("USING"); err != nil {
		return nil, err
	}

	return st, p.filterClause(st)
}

func (p *parser) onTable(st *Statement) error {
	if err := p.expectKw("ON"); err != nil {
		return err
	}

	path, err := p.tablePath()
	if err != nil {
		return err
	}

	st.target = path

	return nil
}

func (p *parser) granteeList(st *Statement) error {
	if err := p.expectPunct("("); err != nil {
		return err
	}

	for {
		t := p.peek()
		if t.kind != tkString {
			return p.unexpected("expected a quoted grantee")
		}

		p.i++
		st.grantees = append(st.grantees, t.text)

		if p.acceptPunct(")") {
			return nil
		}

		if err := p.expectPunct(","); err != nil {
			return err
		}
	}
}

func (p *parser) filterClause(st *Statement) error {
	if err := p.expectPunct("("); err != nil {
		return err
	}

	start := p.peek().pos

	if _, err := p.expr(); err != nil {
		return err
	}

	end := p.peek().pos
	if err := p.expectPunct(")"); err != nil {
		return err
	}

	st.filterSQL = strings.TrimSpace(p.src[start:end])

	return nil
}

func (p *parser) dropRowPolicy(st *Statement, all bool) (*Statement, error) {
	st.Kind = StmtDropRowPolicy
	st.dropAll = all

	if all {
		p.i += 2

		if err := p.expectKw("POLICIES"); err != nil {
			return nil, err
		}

		return st, p.onTable(st)
	}

	p.i += 2

	if err := p.expectKw("POLICY"); err != nil {
		return nil, err
	}

	if p.kw("IF") {
		p.i++

		if err := p.expectKw("EXISTS"); err != nil {
			return nil, err
		}

		st.ifExists = true
	}

	name, err := p.anyIdent()
	if err != nil {
		return nil, err
	}

	st.policyName = name

	return st, p.onTable(st)
}

func (c *compiler) policyTable(st *Statement) (TableRef, *TableData, error) {
	ref, err := c.resolveRef(st.target)
	if err != nil {
		return ref, nil, err
	}

	td, ok := c.in.Catalog.Table(ref)
	if !ok {
		return ref, nil, notFound("Not found: Table %s", ref)
	}

	if td.ViewQuery != "" {
		return ref, nil, invalid("Row access policies can only be created on tables, not on views: %s", ref)
	}

	return ref, td, nil
}

func policyIndex(td *TableData, name string) int {
	for i, p := range td.RowPolicies {
		if strs.Equal(p.Name, name) {
			return i
		}
	}

	return -1
}

// CheckRowPolicy validates a policy against the table schema: grantee forms, a boolean filter, no JSON columns.
func (c *compiler) checkRowPolicy(td *TableData, grantees []string, filter string) error {
	for _, g := range grantees {
		if !ValidGrantee(g) {
			return invalid("Invalid grantee %q; use user:, serviceAccount:, group:, domain:, allUsers or "+
				"allAuthenticatedUsers", g)
		}
	}

	for _, f := range td.Fields {
		if f.SQLType().Kind == KindJSON {
			return invalid("Row access policies cannot be applied to tables with JSON columns: %s", td.Ref)
		}
	}

	_, err := c.compilePolicyFilter(td, "t", filter)

	return err
}

func (c *compiler) compilePolicyFilter(td *TableData, alias, filter string) (*gx, error) {
	n, err := parseExprText(filter)
	if err != nil {
		return nil, err
	}

	rel := &relation{alias: alias}

	for _, fl := range td.Fields {
		rel.cols = append(rel.cols, &colInfo{
			name: fl.Name, typ: fl.SQLType(), sql: quoteIdent(alias) + "." + quoteIdent(fl.Name),
		})
	}

	if c.polDepth >= maxPolicyDepth {
		return nil, invalid("Row access policy filters cannot reference tables that have row access policies")
	}

	c.polDepth++
	defer func() { c.polDepth-- }()

	g, err := c.expr(n, &scope{rels: []*relation{rel}})
	if err != nil {
		return nil, err
	}

	if g.typ.Kind != KindBool && g.typ.Kind != KindUnknown {
		return nil, invalid("Row access policy filter must be a BOOL expression but is %s", g.typ.Kind.SQLName())
	}

	return g, nil
}

func (c *compiler) ddlCreateRowPolicy(st *Statement) ([]Effect, error) {
	ref, td, err := c.policyTable(st)
	if err != nil {
		return nil, err
	}

	if len(st.grantees) == 0 {
		return nil, invalid("A row access policy needs at least one grantee")
	}

	if i := policyIndex(td, st.policyName); i >= 0 {
		switch {
		case st.ifNot:
			return nil, nil
		case !st.orReplace:
			return nil, exists("Already Exists: Row access policy %s on table %s", st.policyName, ref)
		}
	}

	if err = c.checkRowPolicy(td, st.grantees, st.filterSQL); err != nil {
		return nil, err
	}

	pd := &RowPolicyData{Name: st.policyName, Filter: st.filterSQL, Grantees: st.grantees}

	return []Effect{{Op: OpCreateRowPolicy, Ref: ref, Policy: pd, OrReplace: st.orReplace}}, nil
}

func (c *compiler) ddlDropRowPolicy(st *Statement) ([]Effect, error) {
	ref, td, err := c.policyTable(st)
	if err != nil {
		return nil, err
	}

	if st.dropAll {
		return []Effect{{Op: OpDropRowPolicy, Ref: ref, DropAll: true}}, nil
	}

	if policyIndex(td, st.policyName) < 0 {
		if st.ifExists {
			return nil, nil
		}

		return nil, notFound("Not found: Row access policy %s on table %s", st.policyName, ref)
	}

	if len(td.RowPolicies) == 1 {
		return nil, invalid(
			"Cannot drop the last row access policy %s on table %s: removing it would widen access; "+
				"use DROP ALL ROW ACCESS POLICIES", st.policyName, ref)
	}

	return []Effect{{Op: OpDropRowPolicy, Ref: ref, PolicyName: st.policyName}}, nil
}

// policySource wraps a loaded table in the union of the filters of every policy that grants user access.
func (c *compiler) policySource(td *TableData, lt *loadedTable, alias string) (string, error) {
	var conds []string

	for _, p := range td.RowPolicies {
		if !c.policyGrants(p) {
			continue
		}

		g, err := c.compilePolicyFilter(td, alias, p.Filter)
		if err != nil {
			return "", err
		}

		conds = append(conds, "COALESCE(("+g.sql+"), 0)")
	}

	where := "0"
	if len(conds) > 0 {
		where = strings.Join(conds, " OR ")
	}

	return "(SELECT * FROM " + quoteIdent(lt.name) + " AS " + quoteIdent(alias) + " WHERE " + where + ") AS " +
		quoteIdent(alias), nil
}

func (c *compiler) policyGrants(p RowPolicyData) bool {
	user := c.sessionUser()

	for _, g := range p.Grantees {
		if GranteeMatches(g, user) {
			return true
		}
	}

	return false
}

func (c *compiler) sessionUser() string {
	if c.in.User != "" {
		return c.in.User
	}

	return DefaultUser
}

func genSessionUser(c *compiler, _ *node, _ *scope) (*gx, error) {
	return &gx{sql: c.bind(&Param{Value: c.sessionUser(), Field: Field{Type: litString}}), typ: scalar(KindString)}, nil
}

// CheckRowPolicy validates a policy for the table ref against the catalog, as CREATE ROW ACCESS POLICY does.
func CheckRowPolicy(in *Input, ref TableRef, grantees []string, filter string) error {
	c := newCompiler(in)

	td, ok := in.Catalog.Table(ref)
	if !ok {
		return notFound("Not found: Table %s", ref)
	}

	if td.ViewQuery != "" {
		return invalid("Row access policies can only be created on tables, not on views: %s", ref)
	}

	return c.checkRowPolicy(td, grantees, filter)
}

// requirePartitionFilters enforces require_hive_partition_filter: some top-level AND term of WHERE must read only
// the table's partition columns.
func requirePartitionFilters(s *selectStmt, sc *scope) error {
	for _, rel := range sc.rels {
		if len(rel.needFilter) == 0 {
			continue
		}

		ok := false

		for _, term := range andTerms(s.where) {
			if onlyReads(term, rel) {
				ok = true

				break
			}
		}

		if !ok {
			return invalid("Cannot query over table %s without a filter over column(s) '%s' that can be used for "+
				"partition elimination", rel.alias, strings.Join(rel.needFilter, "', '"))
		}
	}

	return nil
}

func andTerms(n *node) []*node {
	if n == nil {
		return nil
	}

	if n.kind == nBinary && strings.EqualFold(n.name, "AND") && len(n.args) == 2 {
		return append(andTerms(n.args[0]), andTerms(n.args[1])...)
	}

	return []*node{n}
}

// onlyReads reports whether n references at least one column and every column is a partition column of rel.
func onlyReads(n *node, rel *relation) bool {
	refs := 0
	ok := true

	var walk func(*node)

	walk = func(x *node) {
		if x == nil || !ok {
			return
		}

		if x.query != nil {
			ok = false

			return
		}

		if x.kind == nPath {
			refs++

			if !isPartitionRef(x.parts, rel) {
				ok = false
			}
		}

		for _, a := range x.args {
			walk(a)
		}
	}

	walk(n)

	return ok && refs > 0
}

func isPartitionRef(parts []string, rel *relation) bool {
	name := parts[len(parts)-1]
	if len(parts) > 1 && !strs.Equal(parts[0], rel.alias) {
		return false
	}

	return strs.ContainsFold(rel.needFilter, name)
}
