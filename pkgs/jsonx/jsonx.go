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
//	dec.Token()                   dec.ReadToken() (Kind: '{' '[' '"' '0' 't' 'f' 'n')
//	dec.More()                    inside a container dec.PeekKind() not '}' or ']'; at top level
//	                              compare InputOffset against the input (PeekKind()!=0 accepts garbage)
//	dec.Buffered()                bytes.NewReader(dec.UnreadBuffer())
//	json.Delim                    jsontext.Kind
//	json.Valid(b)                 jsontext.Value(b).IsValid(jsonx.V1())
//	json.Compact / Indent         jsontext.Value.Compact / Indent
//	json.Marshaler, Unmarshaler   same names in v2 (MarshalJSON / UnmarshalJSON still honored)
//	*json.SyntaxError             jsonx.SyntaxError / IsSyntaxError
//	*json.UnmarshalTypeError      jsonx.TypeError / IsTypeError
//
// Error types: with V1 the v2 engine returns the v1 types (*json.SyntaxError,
// *json.UnmarshalTypeError), not jsontext.SyntacticError or json.SemanticError,
// so errors.As on the v2 types silently misses and turns client errors into
// 500s. Always classify with SyntaxError, IsSyntaxError, TypeError and
// IsTypeError, which accept both families.
//
// A jsontext.Token is invalidated by the next decoder call; copy String()
// before reading on. Whitespace options (Multiline, WithIndent) belong on
// jsontext.NewEncoder, not on MarshalEncode. SDKDecoder returns a v1 Decoder
// for SDK helpers that require one.
//
// Use UnmarshalRead instead of UnmarshalDecode only when trailing data after
// the first value must be an error; v1 Decoder.Decode ignored it.
package jsonx

import (
	jsonv1 "encoding/json"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var (
	errInvalidNumber = errors.New("json: invalid number literal")
	errNotNumber     = errors.New("json: cannot unmarshal into jsonx.Number")
)

// V1 is the full option set that reproduces encoding/json v1 behavior.
func V1() json.Options {
	return jsonv1.DefaultOptionsV1()
}

// SyntaxError reports whether err is a JSON syntax error and its byte offset.
// With V1 options the v2 engine returns the v1 error types, so both are accepted.
func SyntaxError(err error) (int64, bool) {
	if se, ok := errors.AsType[*jsonv1.SyntaxError](err); ok {
		return se.Offset, true
	}

	if se, ok := errors.AsType[*jsontext.SyntacticError](err); ok {
		return se.ByteOffset, true
	}

	return 0, false
}

// IsSyntaxError reports whether err is a JSON syntax error.
func IsSyntaxError(err error) bool {
	_, ok := SyntaxError(err)

	return ok
}

// TypeError reports whether err is a JSON value/type mismatch and the dotted
// path of the offending member (the v1 UnmarshalTypeError.Field form).
func TypeError(err error) (string, bool) {
	if te, ok := errors.AsType[*jsonv1.UnmarshalTypeError](err); ok {
		return te.Field, true
	}

	if se, ok := errors.AsType[*json.SemanticError](err); ok {
		return strings.ReplaceAll(strings.TrimPrefix(string(se.JSONPointer), "/"), "/", "."), true
	}

	return "", false
}

// IsTypeError reports whether err is a JSON value/type mismatch.
func IsTypeError(err error) bool {
	_, ok := TypeError(err)

	return ok
}

// SDKDecoder returns a v1 decoder for AWS SDK helpers that still take
// *encoding/json.Decoder (e.g. restjson.GetErrorInfo).
func SDKDecoder(r io.Reader) *jsonv1.Decoder {
	return jsonv1.NewDecoder(r)
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

// Int returns the number as an int, failing when it is out of int range.
func (n Number) Int() (int, error) { return strconv.Atoi(string(n)) }

// MarshalJSON emits the number literal; an empty Number is 0 like v1.
func (n Number) MarshalJSON() ([]byte, error) {
	if n == "" {
		return []byte("0"), nil
	}

	if !jsontext.Value(n).IsValid() || jsontext.Value(n).Kind() != '0' {
		return nil, fmt.Errorf("%w %q", errInvalidNumber, string(n))
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
			return fmt.Errorf("%w %q", errInvalidNumber, string(s))
		}

		*n = Number(s)
	case 'n':
	default:
		return fmt.Errorf("%w: %s", errNotNumber, v.Kind())
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
