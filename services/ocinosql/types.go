package ocinosql

import (
	"strconv"
	"strings"
)

// Column type kinds.
const (
	kInteger   = "INTEGER"
	kLong      = "LONG"
	kFloat     = "FLOAT"
	kDouble    = "DOUBLE"
	kNumber    = "NUMBER"
	kString    = "STRING"
	kBoolean   = "BOOLEAN"
	kBinary    = "BINARY"
	kTimestamp = "TIMESTAMP"
	kJSON      = "JSON"
	kEnum      = "ENUM"
	kArray     = "ARRAY"
	kMap       = "MAP"
	kRecord    = "RECORD"

	defaultTimestampPrecision = 9
	maxTimestampPrecision     = 9
)

type dtype struct {
	elem   *dtype
	kind   string
	fields []dfield
	enum   []string
	prec   int
}

type dfield struct {
	typ  *dtype
	name string
}

func (t *dtype) String() string {
	switch t.kind {
	case kTimestamp:
		return kTimestamp + "(" + strconv.Itoa(t.prec) + ")"
	case kEnum:
		return kEnum + "(" + strings.Join(t.enum, ",") + ")"
	case kArray, kMap:
		return t.kind + "(" + t.elem.String() + ")"
	case kRecord:
		parts := make([]string, len(t.fields))
		for i, f := range t.fields {
			parts[i] = f.name + " " + f.typ.String()
		}

		return kRecord + "(" + strings.Join(parts, ", ") + ")"
	}

	return t.kind
}

func (t *dtype) isNumeric() bool {
	switch t.kind {
	case kInteger, kLong, kFloat, kDouble, kNumber:
		return true
	}

	return false
}

// keyable reports whether a column of this type may be part of a primary key.
func (t *dtype) keyable() bool {
	switch t.kind {
	case kInteger, kLong, kNumber, kString, kTimestamp, kEnum:
		return true
	}

	return false
}

var simpleKinds = map[string]string{ //nolint:gochecknoglobals // immutable lookup
	"INTEGER": kInteger, "INT": kInteger, "LONG": kLong, "FLOAT": kFloat, "DOUBLE": kDouble, "NUMBER": kNumber,
	"STRING": kString, "BOOLEAN": kBoolean, "BINARY": kBinary, "JSON": kJSON,
}

func (p *parser) parseType() (*dtype, *APIError) {
	t := p.peek()
	if t.kind != tkIdent {
		return nil, p.fail("a data type")
	}

	p.pos++

	if k, ok := simpleKinds[t.up]; ok {
		return &dtype{kind: k}, nil
	}

	switch t.up {
	case kTimestamp:
		return p.parseTimestampType()
	case kEnum:
		return p.parseEnumType()
	case kArray, kMap:
		return p.parseContainerType(t.up)
	case kRecord:
		return p.parseRecordType()
	}

	return nil, errParam("syntax error: unsupported data type '" + t.text + "'")
}

func (p *parser) parseTimestampType() (*dtype, *APIError) {
	prec := defaultTimestampPrecision

	if p.acceptPunct("(") {
		n := p.next()

		v, err := strconv.Atoi(n.text)
		if err != nil || v < 0 || v > maxTimestampPrecision {
			return nil, errParam("TIMESTAMP precision must be an integer between 0 and 9")
		}

		prec = v

		if e := p.expectPunct(")"); e != nil {
			return nil, e
		}
	}

	return &dtype{kind: kTimestamp, prec: prec}, nil
}

func (p *parser) parseEnumType() (*dtype, *APIError) {
	if e := p.expectPunct("("); e != nil {
		return nil, e
	}

	t := &dtype{kind: kEnum}

	for {
		name, e := p.ident("an enum value")
		if e != nil {
			return nil, e
		}

		t.enum = append(t.enum, name)

		if !p.acceptPunct(",") {
			break
		}
	}

	return t, p.expectPunct(")")
}

func (p *parser) parseContainerType(kind string) (*dtype, *APIError) {
	if e := p.expectPunct("("); e != nil {
		return nil, e
	}

	elem, e := p.parseType()
	if e != nil {
		return nil, e
	}

	return &dtype{kind: kind, elem: elem}, p.expectPunct(")")
}

func (p *parser) parseRecordType() (*dtype, *APIError) {
	if e := p.expectPunct("("); e != nil {
		return nil, e
	}

	t := &dtype{kind: kRecord}

	for {
		name, e := p.ident("a field name")
		if e != nil {
			return nil, e
		}

		ft, e := p.parseType()
		if e != nil {
			return nil, e
		}

		t.fields = append(t.fields, dfield{name: name, typ: ft})

		if !p.acceptPunct(",") {
			break
		}
	}

	return t, p.expectPunct(")")
}

func parseTypeString(s string) (*dtype, *APIError) {
	p, err := newParser(s)
	if err != nil {
		return nil, err
	}

	return p.parseType()
}
