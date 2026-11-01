// Package jsonx holds the shared option sets for encoding/json/v2 and the few
// helpers that have no 1:1 v2 equivalent.
//
// Every call site uses encoding/json/v2 and encoding/json/jsontext directly;
// V1 is passed so a mechanical swap from encoding/json keeps byte-identical
// wire output and error text (nil slices and maps as null, HTML escaping,
// case-insensitive member matching, tolerated duplicate names and invalid
// UTF-8, v1 error messages). Drop V1 per call only where the v2 default is
// wanted and verified.
//
// This is the only package allowed to import the v1 encoding/json package:
// DefaultOptionsV1 and the legacy-semantics options are exported from there.
//
// v1 to v2 mapping:
//
//	json.Marshal(v)               json.Marshal(v, jsonx.V1())
//	json.Unmarshal(b, &v)         json.Unmarshal(b, &v, jsonx.V1())
//	json.MarshalIndent(v, p, i)   json.Marshal(v, jsonx.V1(), jsontext.Multiline(true),
//	                                  jsontext.WithIndentPrefix(p), jsontext.WithIndent(i))
//	json.NewDecoder(r).Decode(&v) json.UnmarshalDecode(jsontext.NewDecoder(r), &v, jsonx.V1())
//	json.NewEncoder(w).Encode(v)  json.MarshalEncode(jsontext.NewEncoder(w, jsonx.V1()), v)
//	enc.SetIndent / SetEscapeHTML  jsontext.Multiline, WithIndent, EscapeForHTML on NewEncoder
//	dec.DisallowUnknownFields()   json.RejectUnknownMembers(true)
//	dec.UseNumber(), json.Number  jsonx.UseNumber(), jsonx.Number
//	json.RawMessage               jsontext.Value
//	json.Valid(b)                 jsontext.Value(b).IsValid(jsonx.V1())
//	json.Compact / Indent         jsontext.Value.Compact / Indent
//	json.Marshaler, Unmarshaler   same names in v2 (MarshalJSON / UnmarshalJSON still honored)
//	*json.SyntaxError             *jsontext.SyntacticError
//	*json.UnmarshalTypeError      *json.SemanticError
//
// Use UnmarshalRead instead of UnmarshalDecode only when trailing data after
// the first value must be an error; v1 Decoder.Decode ignored it.
package jsonx

import (
	jsonv1 "encoding/json"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strconv"
)

// V1 is the full option set that reproduces encoding/json v1 behavior.
func V1() json.Options {
	return jsonv1.DefaultOptionsV1()
}

// Number is a JSON number kept in its textual form, the v2 stand-in for
// v1 json.Number. It is produced for untyped numbers when UseNumber is set.
type Number string

// String returns the literal text of the number.
func (n Number) String() string { return string(n) }

// Float64 returns the number as a float64.
func (n Number) Float64() (float64, error) { return strconv.ParseFloat(string(n), 64) }

// Int64 returns the number as an int64.
func (n Number) Int64() (int64, error) { return strconv.ParseInt(string(n), 10, 64) }

// MarshalJSON emits the number literal; an empty Number is 0 like v1.
func (n Number) MarshalJSON() ([]byte, error) {
	if n == "" {
		return []byte("0"), nil
	}

	if !jsontext.Value(n).IsValid() || jsontext.Value(n).Kind() != '0' {
		return nil, fmt.Errorf("json: invalid number literal %q", string(n))
	}

	return []byte(n), nil
}

// UnmarshalJSON accepts a JSON number, or a string holding a valid number as v1 did.
func (n *Number) UnmarshalJSON(b []byte) error {
	v := jsontext.Value(b)

	switch v.Kind() {
	case '0':
		*n = Number(b)
	case '"':
		s, err := jsontext.AppendUnquote(nil, b)
		if err != nil {
			return err
		}

		if lit := jsontext.Value(s); !lit.IsValid() || lit.Kind() != '0' {
			return fmt.Errorf("json: invalid number literal %q", string(s))
		}

		*n = Number(s)
	case 'n':
	default:
		return fmt.Errorf("json: cannot unmarshal %s into Go value of type jsonx.Number", v.Kind())
	}

	return nil
}

// UseNumber makes every untyped (any) JSON number decode as Number instead of
// float64, replacing v1 Decoder.UseNumber.
func UseNumber() json.Options {
	return json.WithUnmarshalers(json.UnmarshalFromFunc(unmarshalAnyNumber))
}

func unmarshalAnyNumber(dec *jsontext.Decoder, v *any) error {
	switch dec.PeekKind() {
	case '0':
		val, err := dec.ReadValue()
		if err != nil {
			return err
		}

		*v = Number(val)
	case '{':
		var m map[string]any
		if err := json.UnmarshalDecode(dec, &m, V1(), UseNumber()); err != nil {
			return err
		}

		*v = m
	case '[':
		var s []any
		if err := json.UnmarshalDecode(dec, &s, V1(), UseNumber()); err != nil {
			return err
		}

		*v = s
	default:
		tok, err := dec.ReadToken()
		if err != nil {
			return err
		}

		switch tok.Kind() {
		case 'n':
			*v = nil
		case 't', 'f':
			*v = tok.Bool()
		default:
			*v = tok.String()
		}
	}

	return nil
}
