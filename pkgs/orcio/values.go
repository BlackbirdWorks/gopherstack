package orcio

import (
	"encoding/binary"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"math"
	"math/big"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// containerMark prefixes the JSON text of nested values produced by the SQL engine.
const containerMark = "\x01"

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		return fmt.Sprint(t)
	}
}

func toBool(v any) (bool, error) {
	if b, ok := v.(bool); ok {
		return b, nil
	}

	b, err := strconv.ParseBool(asString(v))
	if err != nil {
		return false, fmt.Errorf("%w: %v as boolean", ErrBadValue, v)
	}

	return b, nil
}

func toInt64(v any) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case float64:
		return int64(t), nil
	}

	s := strings.TrimSpace(asString(v))

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		f, ferr := strconv.ParseFloat(s, 64)
		if ferr != nil {
			return 0, fmt.Errorf("%w: %v as integer", ErrBadValue, v)
		}

		return int64(f), nil
	}

	return n, nil
}

func toFloat(v any) (float64, error) {
	if f, ok := v.(float64); ok {
		return f, nil
	}

	f, err := strconv.ParseFloat(strings.TrimSpace(asString(v)), 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %v as float", ErrBadValue, v)
	}

	return f, nil
}

func appendFloat32(b []byte, f float64) []byte {
	return binary.LittleEndian.AppendUint32(b, math.Float32bits(float32(f)))
}

func appendFloat64(b []byte, f float64) []byte {
	return binary.LittleEndian.AppendUint64(b, math.Float64bits(f))
}

func toDays(v any) (int64, error) {
	if n, ok := v.(int64); ok {
		return n, nil
	}

	s := strings.TrimSpace(asString(v))
	if len(s) > len(dateLayout) {
		s = s[:len(dateLayout)]
	}

	t, err := time.Parse(dateLayout, s)
	if err != nil {
		return 0, fmt.Errorf("%w: %v as date", ErrBadValue, v)
	}

	return t.Unix() / secondsPerDay, nil
}

func toTime(v any) (time.Time, error) {
	s := strings.TrimSpace(asString(v))

	for _, layout := range []string{"2006-01-02 15:04:05.999999999", time.RFC3339Nano, dateLayout} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("%w: %v as timestamp", ErrBadValue, v)
}

// toDecimal returns the unscaled value of v at the column scale.
func toDecimal(v any, scale int) (*big.Int, error) {
	s := strings.TrimSpace(asString(v))

	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("%w: %v as decimal", ErrBadValue, v)
	}

	pow := new(big.Int).Exp(big.NewInt(decimalBase), big.NewInt(int64(scale)), nil)
	r.Mul(r, new(big.Rat).SetInt(pow))

	return new(big.Int).Quo(r.Num(), r.Denom()), nil
}

func appendBigVarint(b []byte, v *big.Int) []byte {
	z := new(big.Int)
	if v.Sign() < 0 {
		z.Neg(v)
		z.Lsh(z, 1)
		z.Sub(z, big.NewInt(1))
	} else {
		z.Lsh(v, 1)
	}

	mask := big.NewInt(varintPayload)

	for {
		low := new(big.Int).And(z, mask).Uint64()
		z.Rsh(z, varintShift)

		if z.Sign() == 0 {
			return append(b, lowByte(low))
		}

		b = append(b, lowByte(low)|varintMore)
	}
}

// decodeJSONValue parses nested JSON text from the SQL engine.
func decodeJSONValue(s string) (any, error) {
	dec := jsontext.NewDecoder(strings.NewReader(strings.TrimPrefix(s, containerMark)))

	var out any
	if err := json.UnmarshalDecode(dec, &out, jsonx.V1(), jsonx.UseNumber()); err != nil {
		return nil, fmt.Errorf("%w: nested value %q: %w", ErrBadValue, s, err)
	}

	return normalizeJSON(out), nil
}

func normalizeJSON(v any) any {
	switch t := v.(type) {
	case jsonx.Number:
		if n, err := strconv.ParseInt(t.String(), 10, 64); err == nil {
			return n
		}

		f, _ := strconv.ParseFloat(t.String(), 64)

		return f
	case []any:
		for i, e := range t {
			t[i] = normalizeJSON(e)
		}

		return t
	case map[string]any:
		for k, e := range t {
			t[k] = normalizeJSON(e)
		}

		return t
	default:
		return v
	}
}

func toObject(v any) (map[string]any, error) {
	if s, ok := v.(string); ok {
		d, err := decodeJSONValue(s)
		if err != nil {
			return nil, err
		}

		v = d
	}

	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: %v is not an object", ErrBadValue, v)
	}

	return m, nil
}

func toArray(v any) ([]any, error) {
	if s, ok := v.(string); ok {
		d, err := decodeJSONValue(s)
		if err != nil {
			return nil, err
		}

		v = d
	}

	a, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: %v is not an array", ErrBadValue, v)
	}

	return a, nil
}

func lookupField(m map[string]any, name string) any {
	if v, ok := m[name]; ok {
		return v
	}

	for k, v := range m {
		if strings.EqualFold(k, name) {
			return v
		}
	}

	return nil
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return slices.Clone(keys)
}
