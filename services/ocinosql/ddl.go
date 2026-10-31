package ocinosql

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	hoursPerDay = 24
	maxNameLen  = 256
	maxFieldLen = 64
	maxColumns  = 50
)

var identRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

// Column is one table column.
type Column struct {
	Name    string          `json:"name"`
	Type    string          `json:"type"`
	Default json.RawMessage `json:"default,omitempty"`
	NotNull bool            `json:"notNull,omitempty"`
	AsUUID  bool            `json:"asUuid,omitempty"`
	Gen     bool            `json:"generated,omitempty"`
}

// Identity is the table's identity column spec and its counter.
type Identity struct {
	Column    string `json:"column"`
	Always    bool   `json:"always"`
	OnNull    bool   `json:"onNull,omitempty"`
	Next      int64  `json:"next"`
	Increment int64  `json:"increment"`
}

type createSpec struct {
	ttlHours    *int
	identity    *Identity
	name        string
	cols        []Column
	pk          []string
	shard       []string
	ifNotExists bool
}

type alterSpec struct {
	ttlHours *int
	identity *Identity
	name     string
	add      []Column
	drop     []string
}

func validIdent(kind, name string) *APIError {
	limit := maxFieldLen
	if kind == "table" {
		limit = maxNameLen
	}

	if len(name) > limit || !identRe.MatchString(name) {
		return errParam(kind + " name '" + name + "' is invalid: it must start with a letter and contain only " +
			"letters, digits and underscores, at most " + strconv.Itoa(limit) + " characters")
	}

	return nil
}

func parseCreate(sql string) (*createSpec, *APIError) {
	p, err := newParser(sql)
	if err != nil {
		return nil, err
	}

	if !p.acceptKw("CREATE") || !p.acceptKw("TABLE") {
		return nil, errParam("ddlStatement must be a CREATE TABLE statement")
	}

	spec := &createSpec{}

	if p.acceptKw("IF") {
		if e := firstErr(p.expectKw("NOT"), p.expectKw("EXISTS")); e != nil {
			return nil, e
		}

		spec.ifNotExists = true
	}

	name, e := p.ident("a table name")
	if e != nil {
		return nil, e
	}

	if p.isPunct(".") {
		return nil, errParam("child tables (parent.child names) are not supported")
	}

	spec.name = name

	if e = p.parseCreateBody(spec); e != nil {
		return nil, e
	}

	if spec.ttlHours, e = p.parseTTL(); e != nil {
		return nil, e
	}

	if !p.atEOF() {
		return nil, p.fail("end of statement")
	}

	return spec, validateSchema(spec)
}

func firstErr(errs ...*APIError) *APIError {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

func (p *parser) parseCreateBody(spec *createSpec) *APIError {
	if e := p.expectPunct("("); e != nil {
		return e
	}

	for {
		var e *APIError

		if p.isKw("PRIMARY") {
			e = p.parsePrimaryKey(spec)
		} else {
			e = p.parseColumnInto(spec)
		}

		if e != nil {
			return e
		}

		if !p.acceptPunct(",") {
			break
		}
	}

	return p.expectPunct(")")
}

func (p *parser) parseColumnInto(spec *createSpec) *APIError {
	col, id, e := p.parseColumn()
	if e != nil {
		return e
	}

	if id != nil {
		if spec.identity != nil {
			return errParam("a table can have only one identity column")
		}

		spec.identity = id
	}

	spec.cols = append(spec.cols, col)

	return nil
}

func (p *parser) parsePrimaryKey(spec *createSpec) *APIError {
	p.pos++

	if e := firstErr(p.expectKw("KEY"), p.expectPunct("(")); e != nil {
		return e
	}

	if len(spec.pk) > 0 {
		return errParam("PRIMARY KEY specified more than once")
	}

	if p.acceptKw("SHARD") {
		if e := p.expectPunct("("); e != nil {
			return e
		}

		names, e := p.identList()
		if e != nil {
			return e
		}

		spec.shard = names

		if e = p.expectPunct(")"); e != nil {
			return e
		}

		p.acceptPunct(",")
	}

	names, e := p.identList()
	if e != nil {
		return e
	}

	spec.pk = append(append([]string{}, spec.shard...), names...)

	return p.expectPunct(")")
}

func (p *parser) identList() ([]string, *APIError) {
	var out []string

	for {
		n, e := p.ident("a column name")
		if e != nil {
			return nil, e
		}

		out = append(out, n)

		if !p.isPunct(",") || p.toks[p.pos+1].kind != tkIdent || p.toks[p.pos+1].up == "SHARD" {
			return out, nil
		}

		p.pos++
	}
}

func (p *parser) parseTTL() (*int, *APIError) {
	if !p.acceptKw("USING") {
		return nil, nil
	}

	if e := p.expectKw("TTL"); e != nil {
		return nil, e
	}

	n, err := strconv.Atoi(p.next().text)
	if err != nil || n < 0 {
		return nil, errParam("TTL must be a non-negative integer")
	}

	hours := n

	switch {
	case p.acceptKw("DAYS"), p.acceptKw("DAY"):
		hours = n * hoursPerDay
	case p.acceptKw("HOURS"), p.acceptKw("HOUR"):
	default:
		return nil, p.fail("DAYS or HOURS")
	}

	return &hours, nil
}

func (p *parser) parseColumn() (Column, *Identity, *APIError) {
	name, e := p.ident("a column name")
	if e != nil {
		return Column{}, nil, e
	}

	if e = validIdent("column", name); e != nil {
		return Column{}, nil, e
	}

	t, e := p.parseType()
	if e != nil {
		return Column{}, nil, e
	}

	col := Column{Name: name, Type: t.String()}

	var id *Identity

	for {
		var done bool

		done, id, e = p.parseColumnOption(&col, t, id)
		if e != nil {
			return Column{}, nil, e
		}

		if done {
			break
		}
	}

	if id != nil {
		id.Column = name
	}

	return col, id, nil
}

func (p *parser) parseColumnOption(col *Column, t *dtype, id *Identity) (bool, *Identity, *APIError) {
	switch {
	case p.acceptKw("NOT"):
		col.NotNull = true

		return false, id, p.expectKw(kwNull)
	case p.acceptKw("DEFAULT"):
		lit, e := p.defaultLiteral()
		col.Default = lit

		return false, id, e
	case p.isKw("AS") && p.toks[p.pos+1].up == "UUID":
		p.pos += 2
		col.AsUUID = true

		return false, id, nil
	case p.acceptKw("GENERATED"):
		return p.parseGenerated(col, t, id)
	}

	return true, id, nil
}

func (p *parser) defaultLiteral() (json.RawMessage, *APIError) {
	t := p.next()
	switch {
	case t.kind == tkNumber:
		return json.RawMessage(t.text), nil
	case t.kind == tkString:
		b, _ := json.Marshal(t.text)

		return b, nil
	case t.up == kwTrue || t.up == kwFalse:
		return json.RawMessage(strings.ToLower(t.text)), nil
	case t.up == kwNull:
		return json.RawMessage("null"), nil
	case t.text == "-" && p.peek().kind == tkNumber:
		return json.RawMessage("-" + p.next().text), nil
	}

	return nil, errParam("syntax error: DEFAULT must be a number, string, boolean or NULL literal")
}

func (p *parser) parseGenerated(col *Column, t *dtype, id *Identity) (bool, *Identity, *APIError) {
	always := p.acceptKw("ALWAYS")
	if !always {
		if e := firstErr(p.expectKw("BY"), p.expectKw("DEFAULT")); e != nil {
			return false, id, e
		}
	}

	if col.AsUUID && !p.isKw("AS") {
		col.Gen = true

		return false, id, nil
	}

	onNull := false
	if !always && p.acceptKw("ON") {
		if e := p.expectKw(kwNull); e != nil {
			return false, id, e
		}

		onNull = true
	}

	if e := p.expectKw("AS"); e != nil {
		return false, id, e
	}

	if p.acceptKw("UUID") {
		col.AsUUID, col.Gen = true, true

		return false, id, nil
	}

	if e := p.expectKw("IDENTITY"); e != nil {
		return false, id, e
	}

	if t.kind != kInteger && t.kind != kLong {
		return false, id, errParam("identity columns must be INTEGER or LONG")
	}

	if id != nil {
		return false, id, errParam("a column can have only one identity specification")
	}

	col.Gen = true

	ident := &Identity{Always: always, OnNull: onNull, Next: 1, Increment: 1}

	return false, ident, p.parseIdentityOptions(ident)
}

func (p *parser) parseIdentityOptions(id *Identity) *APIError {
	if !p.acceptPunct("(") {
		return nil
	}

	for !p.acceptPunct(")") {
		switch {
		case p.acceptKw("START"):
			v, e := p.optInt("WITH")
			if e != nil {
				return e
			}

			id.Next = v
		case p.acceptKw("INCREMENT"):
			v, e := p.optInt("BY")
			if e != nil {
				return e
			}

			if v == 0 {
				return errParam("identity INCREMENT BY must not be zero")
			}

			id.Increment = v
		default:
			return errParam("syntax error: unsupported identity option '" + p.peek().text + "'")
		}
	}

	return nil
}

func (p *parser) optInt(kw string) (int64, *APIError) {
	if e := p.expectKw(kw); e != nil {
		return 0, e
	}

	neg := p.acceptPunct("-")

	v, err := strconv.ParseInt(p.next().text, 10, 64)
	if err != nil {
		return 0, errParam("syntax error: expected an integer")
	}

	if neg {
		v = -v
	}

	return v, nil
}

func parseAlter(sql string) (*alterSpec, *APIError) {
	p, err := newParser(sql)
	if err != nil {
		return nil, err
	}

	if !p.acceptKw("ALTER") || !p.acceptKw("TABLE") {
		return nil, errParam("ddlStatement must be an ALTER TABLE statement")
	}

	name, e := p.ident("a table name")
	if e != nil {
		return nil, e
	}

	spec := &alterSpec{name: name}

	if p.isPunct("(") {
		if e = p.parseAlterActions(spec); e != nil {
			return nil, e
		}
	}

	if spec.ttlHours, e = p.parseTTL(); e != nil {
		return nil, e
	}

	if !p.atEOF() {
		return nil, p.fail("end of statement")
	}

	if len(spec.add) == 0 && len(spec.drop) == 0 && spec.ttlHours == nil {
		return nil, errParam("ALTER TABLE must add a column, drop a column or set USING TTL")
	}

	return spec, nil
}

func (p *parser) parseAlterActions(spec *alterSpec) *APIError {
	p.pos++

	for {
		switch {
		case p.acceptKw("ADD"):
			col, id, e := p.parseColumn()
			if e != nil {
				return e
			}

			if id != nil {
				if spec.identity != nil {
					return errParam("a table can have only one identity column")
				}

				spec.identity = id
			}

			spec.add = append(spec.add, col)
		case p.acceptKw("DROP"):
			n, e := p.ident("a column name")
			if e != nil {
				return e
			}

			spec.drop = append(spec.drop, n)
		default:
			return p.fail("ADD or DROP")
		}

		if !p.acceptPunct(",") {
			break
		}
	}

	return p.expectPunct(")")
}

func validateSchema(s *createSpec) *APIError {
	if e := validIdent("table", s.name); e != nil {
		return e
	}

	if len(s.cols) == 0 {
		return errParam("a table needs at least one column")
	}

	if len(s.cols) > maxColumns {
		return errLimit("a table can have at most " + strconv.Itoa(maxColumns) + " columns")
	}

	byName := map[string]*dtype{}

	for i := range s.cols {
		c := &s.cols[i]

		t, e := parseTypeString(c.Type)
		if e != nil {
			return e
		}

		if len(c.Default) > 0 {
			if _, msg := coerceDefault(t, c.Default); msg != "" {
				return errParam("default of column '" + c.Name + "' " + msg)
			}
		}

		if _, dup := byName[strs.Fold(c.Name)]; dup {
			return errParam("duplicate column '" + c.Name + "'")
		}

		byName[strs.Fold(c.Name)] = t
	}

	if len(s.pk) == 0 {
		return errParam("a primary key is required: add PRIMARY KEY(...)")
	}

	return validateKey(s, byName)
}

func validateKey(s *createSpec, byName map[string]*dtype) *APIError {
	seen := map[string]bool{}

	for i, n := range s.pk {
		t, ok := byName[strs.Fold(n)]
		if !ok {
			return errParam("primary key column '" + n + "' is not a column of the table")
		}

		if !t.keyable() {
			return errParam("primary key column '" + n + "' has type " + t.kind + " which cannot be part of a key")
		}

		if seen[strs.Fold(n)] {
			return errParam("duplicate primary key column '" + n + "'")
		}

		seen[strs.Fold(n)] = true
		s.pk[i] = canonicalName(s.cols, n)

		for j := range s.cols {
			if strs.Equal(s.cols[j].Name, n) {
				s.cols[j].NotNull = true
			}
		}
	}

	if len(s.shard) > 0 && !strs.Equal(strings.Join(s.shard, ","), strings.Join(s.pk[:len(s.shard)], ",")) {
		return errParam("SHARD columns must be the leading columns of the primary key")
	}

	return nil
}

func canonicalName(cols []Column, n string) string {
	for _, c := range cols {
		if strs.Equal(c.Name, n) {
			return c.Name
		}
	}

	return n
}
