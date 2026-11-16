package bqsql

import (
	"strings"
)

// Routine kinds, spelled as in the REST routineType.
const (
	RoutineScalar    = "SCALAR_FUNCTION"
	RoutineTable     = "TABLE_VALUED_FUNCTION"
	RoutineProcedure = "PROCEDURE"
)

// Routine argument modes.
const (
	ModeIn    = "IN"
	ModeOut   = "OUT"
	ModeInOut = "INOUT"
)

// RoutineArg is one routine parameter.
type RoutineArg struct {
	Name     string
	Mode     string
	Table    []Column
	Type     Type
	AnyType  bool
	AnyTable bool
	IsTable  bool
}

// RoutineData is a persistent or temporary function, table function or procedure.
type RoutineData struct {
	Returns     *Type
	Ref         TableRef
	Kind        string
	Language    string
	Body        string
	Description string
	Args        []RoutineArg
	ReturnTable []Column
	Temp        bool
}

// RoutineCatalog is implemented by catalogs that store routines.
type RoutineCatalog interface {
	Routine(ref TableRef) (*RoutineData, bool)
}

func (p *parser) createRoutine(st *Statement, kind string) (*Statement, error) {
	st.Kind = StmtCreateRoutine

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

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	st.target = path
	st.routine = &RoutineData{Kind: kind, Language: langSQL, Temp: st.temp}

	if err = p.expectPunct("("); err != nil {
		return nil, err
	}

	if st.routine.Args, err = p.routineArgs(kind); err != nil {
		return nil, err
	}

	if err = p.routineClauses(st, kind); err != nil {
		return nil, err
	}

	return st, nil
}

func (p *parser) routineClauses(st *Statement, kind string) error {
	for {
		done, err := p.routineClause(st, kind)
		if err != nil || done {
			return err
		}
	}
}

// routineClause consumes one clause of a CREATE FUNCTION header; done is true after the body.
func (p *parser) routineClause(st *Statement, kind string) (bool, error) {
	r := st.routine

	switch {
	case p.acceptKw("RETURNS"):
		return false, p.routineReturns(r, kind)
	case p.kw("DETERMINISTIC") || p.kw(litNot) && p.peekNext().isKw("DETERMINISTIC"):
		p.i++
		p.acceptKw("DETERMINISTIC")

		return false, nil
	case p.acceptKw("LANGUAGE"):
		lang, err := p.anyIdent()
		r.Language = strings.ToUpper(lang)

		if r.Language == "JS" {
			r.Language = "JAVASCRIPT"
		}

		return false, err
	case p.kw("OPTIONS"):
		opts, err := p.optionsClause()
		st.options = opts
		r.Description = opts["description"]

		return false, err
	case kind == RoutineProcedure && p.kw("BEGIN"):
		return true, p.procedureBody(r)
	case p.acceptKw("AS"):
		return true, p.routineAs(st, kind)
	}

	return false, p.unexpected("expected AS")
}

func (p *parser) routineReturns(r *RoutineData, kind string) error {
	if kind == RoutineTable {
		if err := p.expectKw("TABLE"); err != nil {
			return err
		}

		t, err := p.structType()
		if err != nil {
			return err
		}

		r.ReturnTable = t.Fields

		return nil
	}

	t, err := p.typeSpec()
	if err != nil {
		return err
	}

	r.Returns = &t

	return nil
}

func (p *parser) routineAs(st *Statement, kind string) error {
	r := st.routine

	if kind == RoutineScalar && p.peek().kind == tkString {
		r.Body = p.advance().text
		p.acceptPunct(";")

		return p.routineTailOptions(st)
	}

	start := p.peek().pos

	if kind == RoutineScalar {
		if err := p.expectPunct("("); err != nil {
			return err
		}

		bodyStart := p.peek().pos

		if _, err := p.expr(); err != nil {
			return err
		}

		end := p.peek().pos
		if err := p.expectPunct(")"); err != nil {
			return err
		}

		r.Body = strings.TrimSpace(p.src[bodyStart:end])

		return p.routineTailOptions(st)
	}

	if _, err := p.query(); err != nil {
		return err
	}

	end := p.peek().pos
	if p.peek().kind == tkEOF {
		end = len(p.src)
	}

	r.Body = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(p.src[start:end]), ";"))

	return nil
}

func (p *parser) routineTailOptions(st *Statement) error {
	if p.kw("OPTIONS") {
		opts, err := p.optionsClause()
		if err != nil {
			return err
		}

		st.options = opts
		st.routine.Description = opts["description"]
	}

	return nil
}

func (p *parser) routineArgs(kind string) ([]RoutineArg, error) {
	var out []RoutineArg

	if p.acceptPunct(")") {
		return out, nil
	}

	for {
		a, err := p.routineArg(kind)
		if err != nil {
			return nil, err
		}

		out = append(out, a)

		if p.acceptPunct(")") {
			return out, nil
		}

		if err = p.expectPunct(","); err != nil {
			return nil, err
		}
	}
}

func (p *parser) routineArg(kind string) (RoutineArg, error) {
	var a RoutineArg

	if kind == RoutineProcedure {
		a.Mode = p.procedureMode()
	}

	name, err := p.identName()
	if err != nil {
		return a, err
	}

	a.Name = name

	if err = p.routineArgType(&a); err != nil {
		return a, err
	}

	if p.kw(litNot) && p.peekNext().isKw("AGGREGATE") {
		p.i += 2
	}

	return a, nil
}

func (p *parser) procedureMode() string {
	for _, m := range []string{ModeIn, ModeOut, ModeInOut} {
		if p.kw(m) && (p.peekNext().kind == tkIdent || p.peekNext().kind == tkQuoted) && !p.argNameThenEnd() {
			p.i++

			return m
		}
	}

	return ""
}

func (p *parser) routineArgType(a *RoutineArg) error {
	var err error

	switch {
	case p.kw("ANY") && p.peekNext().isKw("TYPE"):
		p.i += 2
		a.AnyType = true
	case p.kw("ANY") && p.peekNext().isKw("TABLE"):
		p.i += 2
		a.AnyTable, a.IsTable = true, true
	case p.kw("TABLE") && p.peekNext().isPunct("<"):
		p.i++

		var t Type

		if t, err = p.structType(); err == nil {
			a.Table, a.IsTable = t.Fields, true
		}
	default:
		a.Type, err = p.typeSpec()
	}

	return err
}

// argNameThenEnd reports whether the current token is an argument named like a mode keyword.
func (p *parser) argNameThenEnd() bool {
	if p.i+2 >= len(p.toks) {
		return false
	}

	n := p.toks[p.i+2]

	return n.isPunct(",") || n.isPunct(")")
}

func (p *parser) dropRoutine(st *Statement, kind string) (*Statement, error) {
	st.Kind = StmtDropRoutine
	st.dropKind = kind

	if p.kw("IF") {
		p.i++

		if err := p.expectKw("EXISTS"); err != nil {
			return nil, err
		}

		st.ifExists = true
	}

	path, err := p.tablePath()
	if err != nil {
		return nil, err
	}

	st.target = path

	return st, nil
}

func (p *parser) tvfArgs() ([]*node, error) {
	p.i++

	var out []*node

	if p.acceptPunct(")") {
		return out, nil
	}

	for {
		if p.kw("TABLE") && (p.peekNext().kind == tkIdent || p.peekNext().kind == tkQuoted) {
			pos := p.peek().pos
			p.i++

			path, err := p.tablePath()
			if err != nil {
				return nil, err
			}

			out = append(out, &node{kind: nTableArg, parts: path, pos: pos})
		} else {
			e, err := p.expr()
			if err != nil {
				return nil, err
			}

			out = append(out, e)
		}

		if p.acceptPunct(")") {
			return out, nil
		}

		if err := p.expectPunct(","); err != nil {
			return nil, err
		}
	}
}
