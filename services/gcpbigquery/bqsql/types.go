// Package bqsql runs a GoogleSQL subset on SQLite and converts BigQuery values between their REST encodings.
package bqsql

import (
	"errors"
	"fmt"
	"strings"
)

// Kind is a GoogleSQL type family.
type Kind uint8

// Type kinds.
const (
	KindUnknown Kind = iota
	KindInt
	KindFloat
	KindBool
	KindString
	KindBytes
	KindTimestamp
	KindDate
	KindTime
	KindDatetime
	KindNumeric
	KindBigNumeric
	KindJSON
	KindGeography
	KindStruct
	KindArray
)

// Column is a named, typed struct member or relation column.
type Column struct {
	Name string
	Type Type
}

// Type is a GoogleSQL type; Elem is set for arrays and Fields for structs.
type Type struct {
	Elem   *Type
	Fields []Column
	Kind   Kind
}

// Field is one schema column in the REST TableFieldSchema encoding.
type Field struct {
	Name        string  `json:"name"`
	Type        string  `json:"type"`
	Mode        string  `json:"mode,omitempty"`
	Description string  `json:"description,omitempty"`
	MaxLength   string  `json:"maxLength,omitempty"`
	Precision   string  `json:"precision,omitempty"`
	Scale       string  `json:"scale,omitempty"`
	Fields      []Field `json:"fields,omitempty"`
}

// Schema modes.
const (
	ModeNullable = "NULLABLE"
	ModeRequired = "REQUIRED"
	ModeRepeated = "REPEATED"
)

// ErrSchema is wrapped by every schema validation failure.
var ErrSchema = errors.New("invalid schema")

func kindNames() map[string]Kind {
	return map[string]Kind{
		litString: KindString, litBytes: KindBytes, litInteger: KindInt, litInt64: KindInt, litFloat: KindFloat,
		litFloat64: KindFloat, litBoolean: KindBool, litBool: KindBool, litTimestamp: KindTimestamp, litDate: KindDate,
		litTime: KindTime, litDatetime: KindDatetime, litNumeric: KindNumeric, litDecimal: KindNumeric,
		litBignumeric: KindBigNumeric, "BIGDECIMAL": KindBigNumeric, litJSON: KindJSON, litGeography: KindGeography,
		"RECORD": KindStruct, litStruct: KindStruct,
	}
}

// KindOf resolves a REST or GoogleSQL scalar type name.
func KindOf(name string) (Kind, bool) {
	k, ok := kindNames()[strings.ToUpper(strings.TrimSpace(name))]

	return k, ok
}

// RESTName is the type name the REST API reports for k.
func (k Kind) RESTName() string {
	switch k {
	case KindInt:
		return litInteger
	case KindFloat:
		return litFloat
	case KindBool:
		return litBoolean
	case KindBytes:
		return litBytes
	case KindTimestamp:
		return litTimestamp
	case KindDate:
		return litDate
	case KindTime:
		return litTime
	case KindDatetime:
		return litDatetime
	case KindNumeric:
		return litNumeric
	case KindBigNumeric:
		return litBignumeric
	case KindJSON:
		return litJSON
	case KindGeography:
		return litGeography
	case KindStruct, KindArray:
		return "RECORD"
	case KindUnknown, KindString:
		return litString
	}

	return litString
}

// SQLName is the GoogleSQL spelling of k.
func (k Kind) SQLName() string {
	switch k {
	case KindInt:
		return litInt64
	case KindFloat:
		return litFloat64
	case KindBool:
		return litBool
	case KindStruct:
		return litStruct
	case KindArray:
		return litArray
	case KindUnknown, KindString, KindBytes, KindTimestamp, KindDate, KindTime, KindDatetime, KindNumeric,
		KindBigNumeric, KindJSON, KindGeography:
		return k.RESTName()
	}

	return k.RESTName()
}

func scalar(k Kind) Type { return Type{Kind: k} }

func arrayOf(t Type) Type { return Type{Kind: KindArray, Elem: &t} }

// IsNumeric reports whether the type is a number.
func (t Type) IsNumeric() bool {
	return t.Kind == KindInt || t.Kind == KindFloat || t.Kind == KindNumeric || t.Kind == KindBigNumeric
}

// SQLType converts the schema field, including its mode, into a Type.
func (f Field) SQLType() Type {
	k, _ := KindOf(f.Type)

	var t Type

	if k == KindStruct {
		t = Type{Kind: KindStruct, Fields: FieldColumns(f.Fields)}
	} else {
		t = scalar(k)
	}

	if strings.EqualFold(f.Mode, ModeRepeated) {
		return arrayOf(t)
	}

	return t
}

// FieldColumns converts schema fields into typed columns.
func FieldColumns(fs []Field) []Column {
	cols := make([]Column, len(fs))
	for i, f := range fs {
		cols[i] = Column{Name: f.Name, Type: f.SQLType()}
	}

	return cols
}

// TypeField renders a column as a schema field with REST type names.
func TypeField(name string, t Type) Field {
	f := Field{Name: name, Mode: ModeNullable}

	if t.Kind == KindArray && t.Elem != nil {
		f.Mode = ModeRepeated
		t = *t.Elem
	}

	f.Type = t.Kind.RESTName()

	if t.Kind == KindStruct {
		f.Fields = ColumnFields(t.Fields)
	}

	return f
}

// ColumnFields renders typed columns as schema fields.
func ColumnFields(cols []Column) []Field {
	out := make([]Field, len(cols))
	for i, c := range cols {
		out[i] = TypeField(c.Name, c.Type)
	}

	return out
}

// NormalizeSchema validates fs and rewrites type and mode names to their REST spellings.
func NormalizeSchema(fs []Field) error {
	seen := make(map[string]bool, len(fs))

	for i := range fs {
		f := &fs[i]

		if !validFieldName(f.Name) {
			return fmt.Errorf("%w: invalid field name %q", ErrSchema, f.Name)
		}

		lower := strings.ToLower(f.Name)
		if seen[lower] {
			return fmt.Errorf("%w: duplicate field name %q", ErrSchema, f.Name)
		}

		seen[lower] = true

		if err := normalizeField(f); err != nil {
			return err
		}
	}

	return nil
}

func normalizeField(f *Field) error {
	k, ok := KindOf(f.Type)
	if !ok {
		return fmt.Errorf("%w: unknown type %q for field %q", ErrSchema, f.Type, f.Name)
	}

	f.Type = k.RESTName()

	switch strings.ToUpper(f.Mode) {
	case "", ModeNullable:
		f.Mode = ModeNullable
	case ModeRequired:
		f.Mode = ModeRequired
	case ModeRepeated:
		f.Mode = ModeRepeated
	default:
		return fmt.Errorf("%w: invalid mode %q for field %q", ErrSchema, f.Mode, f.Name)
	}

	if k == KindStruct {
		if len(f.Fields) == 0 {
			return fmt.Errorf("%w: RECORD field %q has no fields", ErrSchema, f.Name)
		}

		return NormalizeSchema(f.Fields)
	}

	f.Fields = nil

	return nil
}

func validFieldName(n string) bool {
	if n == "" || len(n) > 300 {
		return false
	}

	for i := range len(n) {
		c := n[i]
		if c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || i > 0 && c >= '0' && c <= '9' {
			continue
		}

		return false
	}

	return true
}

// IsNumber reports whether k is INT64, FLOAT64, NUMERIC or BIGNUMERIC.
func (k Kind) IsNumber() bool {
	return k == KindInt || k == KindFloat || k == KindNumeric || k == KindBigNumeric
}
