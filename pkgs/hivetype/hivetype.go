// Package hivetype parses Hive/Athena column type strings into a type tree.
package hivetype

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Kind is a Hive type category.
type Kind int

// Type kinds.
const (
	Boolean Kind = iota
	Tinyint
	Smallint
	Int
	Bigint
	Float
	Double
	String
	Varchar
	Char
	Binary
	Date
	Timestamp
	Decimal
	Array
	Map
	Struct
)

// ErrSyntax is returned for a type string that does not parse.
var ErrSyntax = errors.New("invalid hive type")

const (
	defaultPrecision = 10
	maxPrecision     = 38
)

// Field is one member of a struct type.
type Field struct {
	Type *Type
	Name string
}

// Type is a parsed column type.
type Type struct {
	Elem      *Type
	Key       *Type
	Value     *Type
	Fields    []Field
	Len       int
	Precision int
	Scale     int
	Kind      Kind
}

// Nested reports whether the type is array, map or struct.
func (t *Type) Nested() bool { return t.Kind == Array || t.Kind == Map || t.Kind == Struct }

// IsNested reports whether the type string names an array, map or struct.
func IsNested(s string) bool {
	t, err := Parse(s)

	return err == nil && t.Nested()
}

func scalarKind(name string) (Kind, bool) {
	kinds := map[string]Kind{
		"boolean": Boolean, "tinyint": Tinyint, "smallint": Smallint, "int": Int, "integer": Int,
		"bigint": Bigint, "float": Float, "real": Float, "double": Double, "string": String,
		"varchar": Varchar, "char": Char, "binary": Binary, "varbinary": Binary, "date": Date,
		"timestamp": Timestamp, "decimal": Decimal, "numeric": Decimal,
	}
	k, ok := kinds[name]

	return k, ok
}

type parser struct {
	s   string
	pos int
}

// Parse reads a type such as array<struct<a:int,b:map<string,double>>>; Trino spellings
// (row(a int), array(int), map(k, v)) are accepted too.
func Parse(s string) (*Type, error) {
	p := &parser{s: strings.TrimSpace(s)}

	t, err := p.parseType()
	if err != nil {
		return nil, err
	}

	if p.pos != len(p.s) {
		return nil, fmt.Errorf("%w: trailing %q in %q", ErrSyntax, p.s[p.pos:], s)
	}

	return t, nil
}

func (p *parser) skipSpace() {
	for p.pos < len(p.s) && p.s[p.pos] == ' ' {
		p.pos++
	}
}

func (p *parser) word() string {
	p.skipSpace()

	start := p.pos
	for p.pos < len(p.s) && (isWordByte(p.s[p.pos])) {
		p.pos++
	}

	return p.s[start:p.pos]
}

func isWordByte(c byte) bool {
	return c == '_' || c == '-' || c == '.' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func (p *parser) accept(c byte) bool {
	p.skipSpace()

	if p.pos < len(p.s) && p.s[p.pos] == c {
		p.pos++

		return true
	}

	return false
}

func (p *parser) expectClose(open byte) error {
	want := byte('>')
	if open == '(' {
		want = ')'
	}

	if !p.accept(want) {
		return fmt.Errorf("%w: expected %q at %d of %q", ErrSyntax, want, p.pos, p.s)
	}

	return nil
}

func (p *parser) open() (byte, error) {
	for _, c := range []byte{'<', '('} {
		if p.accept(c) {
			return c, nil
		}
	}

	return 0, fmt.Errorf("%w: expected type arguments at %d of %q", ErrSyntax, p.pos, p.s)
}

func (p *parser) parseType() (*Type, error) {
	name := strings.ToLower(p.word())

	switch name {
	case "array":
		return p.parseArray()
	case "map":
		return p.parseMap()
	case "struct", "row":
		return p.parseStruct()
	}

	k, ok := scalarKind(name)
	if !ok {
		return nil, fmt.Errorf("%w: unknown type %q in %q", ErrSyntax, name, p.s)
	}

	t := &Type{Kind: k}

	if err := p.scalarArgs(t); err != nil {
		return nil, err
	}

	return t, nil
}

func (p *parser) scalarArgs(t *Type) error {
	if t.Kind == Decimal {
		t.Precision = defaultPrecision
	}

	if !p.accept('(') {
		return nil
	}

	nums := []int{}

	for {
		w := p.word()

		n, err := strconv.Atoi(w)
		if err != nil {
			return fmt.Errorf("%w: bad type argument %q in %q", ErrSyntax, w, p.s)
		}

		nums = append(nums, n)

		if !p.accept(',') {
			break
		}
	}

	if err := p.expectClose('('); err != nil {
		return err
	}

	switch t.Kind {
	case Decimal:
		t.Precision = min(nums[0], maxPrecision)
		if len(nums) > 1 {
			t.Scale = nums[1]
		}
	case Varchar, Char:
		t.Len = nums[0]
	default:
	}

	return nil
}

func (p *parser) parseArray() (*Type, error) {
	o, err := p.open()
	if err != nil {
		return nil, err
	}

	elem, err := p.parseType()
	if err != nil {
		return nil, err
	}

	return &Type{Kind: Array, Elem: elem}, p.expectClose(o)
}

func (p *parser) parseMap() (*Type, error) {
	o, err := p.open()
	if err != nil {
		return nil, err
	}

	key, err := p.parseType()
	if err != nil {
		return nil, err
	}

	if !p.accept(',') {
		return nil, fmt.Errorf("%w: map needs a value type in %q", ErrSyntax, p.s)
	}

	val, err := p.parseType()
	if err != nil {
		return nil, err
	}

	return &Type{Kind: Map, Key: key, Value: val}, p.expectClose(o)
}

func (p *parser) parseStruct() (*Type, error) {
	o, err := p.open()
	if err != nil {
		return nil, err
	}

	t := &Type{Kind: Struct}

	for {
		name := p.word()
		if name == "" {
			return nil, fmt.Errorf("%w: struct field name expected at %d of %q", ErrSyntax, p.pos, p.s)
		}

		p.accept(':')

		ft, ferr := p.parseType()
		if ferr != nil {
			return nil, ferr
		}

		t.Fields = append(t.Fields, Field{Name: name, Type: ft})

		if !p.accept(',') {
			break
		}
	}

	return t, p.expectClose(o)
}
