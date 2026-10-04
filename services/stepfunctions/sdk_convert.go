package stepfunctions

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	errSDKUnsupportedField = errors.New("unsupported parameter type")
	errSDKBadValue         = errors.New("invalid parameter value")
)

const sdkTimeLayout = "2006-01-02T15:04:05.000Z"

// decodeSDKInput fills the SDK input struct dst (a pointer) from Step Functions
// Parameters, whose keys are the SDK field names in PascalCase.
func decodeSDKInput(dst reflect.Value, params any) error {
	return assignValue(dst.Elem(), params)
}

func assignValue(v reflect.Value, src any) error {
	if src == nil {
		return nil
	}

	switch v.Kind() {
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))

		return assignValue(v.Elem(), src)
	case reflect.Interface:
		return assignInterface(v, src)
	case reflect.Struct:
		return assignStruct(v, src)
	case reflect.Slice:
		return assignSlice(v, src)
	case reflect.Map:
		return assignMap(v, src)
	default:
		return assignScalar(v, src)
	}
}

func assignInterface(v reflect.Value, src any) error {
	switch {
	case v.Type() == reflect.TypeFor[ddbtypes.AttributeValue]():
		av, err := decodeAttributeValue(src)
		if err != nil {
			return err
		}

		v.Set(reflect.ValueOf(av))
	case v.Type() == reflect.TypeFor[io.Reader]():
		v.Set(reflect.ValueOf(strings.NewReader(streamString(src))))
	default:
		return fmt.Errorf("%w: %s", errSDKUnsupportedField, v.Type())
	}

	return nil
}

func streamString(src any) string {
	if s, ok := src.(string); ok {
		return s
	}

	b, _ := json.Marshal(src)

	return string(b)
}

func assignStruct(v reflect.Value, src any) error {
	if v.Type() == reflect.TypeFor[time.Time]() {
		t, err := parseSDKTime(src)
		if err != nil {
			return err
		}

		v.Set(reflect.ValueOf(t))

		return nil
	}

	m, ok := src.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: expected object for %s", errSDKBadValue, v.Type())
	}

	for i := range v.NumField() {
		f := v.Type().Field(i)
		if !f.IsExported() {
			continue
		}

		val, found := lookupField(m, f.Name)
		if !found {
			continue
		}

		if err := assignValue(v.Field(i), val); err != nil {
			return fmt.Errorf("%s: %w", f.Name, err)
		}
	}

	return nil
}

func lookupField(m map[string]any, name string) (any, bool) {
	if v, ok := m[name]; ok {
		return v, true
	}

	for k, v := range m {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}

	return nil, false
}

func parseSDKTime(src any) (time.Time, error) {
	switch t := src.(type) {
	case string:
		parsed, err := time.Parse(time.RFC3339, t)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: %w", errSDKBadValue, err)
		}

		return parsed, nil
	case float64:
		sec := int64(t)

		return time.Unix(sec, int64((t-float64(sec))*float64(time.Second))).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("%w: timestamp %v", errSDKBadValue, src)
	}
}

func assignSlice(v reflect.Value, src any) error {
	if v.Type().Elem().Kind() == reflect.Uint8 {
		s, ok := src.(string)
		if !ok {
			return fmt.Errorf("%w: expected base64 string", errSDKBadValue)
		}

		if raw, err := base64.StdEncoding.DecodeString(s); err == nil {
			v.SetBytes(raw)
		} else {
			v.SetBytes([]byte(s))
		}

		return nil
	}

	items, ok := src.([]any)
	if !ok {
		return fmt.Errorf("%w: expected array for %s", errSDKBadValue, v.Type())
	}

	out := reflect.MakeSlice(v.Type(), len(items), len(items))
	for i, it := range items {
		if err := assignValue(out.Index(i), it); err != nil {
			return err
		}
	}

	v.Set(out)

	return nil
}

func assignMap(v reflect.Value, src any) error {
	m, ok := src.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: expected object for %s", errSDKBadValue, v.Type())
	}

	out := reflect.MakeMapWithSize(v.Type(), len(m))

	for k, it := range m {
		elem := reflect.New(v.Type().Elem()).Elem()
		if err := assignValue(elem, it); err != nil {
			return err
		}

		out.SetMapIndex(reflect.ValueOf(k).Convert(v.Type().Key()), elem)
	}

	v.Set(out)

	return nil
}

func assignScalar(v reflect.Value, src any) error {
	switch v.Kind() {
	case reflect.String:
		s, err := scalarString(src)
		if err != nil {
			return err
		}

		v.SetString(s)
	case reflect.Bool:
		b, ok := src.(bool)
		if !ok {
			return fmt.Errorf("%w: expected boolean", errSDKBadValue)
		}

		v.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		f, err := scalarNumber(src)
		if err != nil {
			return err
		}

		v.SetInt(int64(f))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		f, err := scalarNumber(src)
		if err != nil {
			return err
		}

		v.SetUint(uint64(f))
	case reflect.Float32, reflect.Float64:
		f, err := scalarNumber(src)
		if err != nil {
			return err
		}

		v.SetFloat(f)
	default:
		return fmt.Errorf("%w: %s", errSDKUnsupportedField, v.Type())
	}

	return nil
}

func scalarString(src any) (string, error) {
	switch t := src.(type) {
	case string:
		return t, nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(t), nil
	default:
		return "", fmt.Errorf("%w: expected string", errSDKBadValue)
	}
}

func scalarNumber(src any) (float64, error) {
	switch t := src.(type) {
	case float64:
		return t, nil
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %w", errSDKBadValue, err)
		}

		return f, nil
	default:
		return 0, fmt.Errorf("%w: expected number", errSDKBadValue)
	}
}

// decodeAttributeValue builds a DynamoDB AttributeValue from its JSON form,
// e.g. {"S": "x"} or {"N": "1"}.
func decodeAttributeValue(src any) (ddbtypes.AttributeValue, error) {
	m, ok := src.(map[string]any)
	if !ok || len(m) != 1 {
		return nil, fmt.Errorf("%w: AttributeValue must hold exactly one type key", errSDKBadValue)
	}

	for key, val := range m {
		return attributeMember(key, val)
	}

	return nil, errSDKBadValue
}

func attributeMember(key string, val any) (ddbtypes.AttributeValue, error) {
	switch key {
	case "S":
		s, err := scalarString(val)

		return &ddbtypes.AttributeValueMemberS{Value: s}, err
	case "N":
		s, err := scalarString(val)

		return &ddbtypes.AttributeValueMemberN{Value: s}, err
	case "B":
		s, _ := val.(string)
		raw, err := base64.StdEncoding.DecodeString(s)

		return &ddbtypes.AttributeValueMemberB{Value: raw}, err
	case "BOOL":
		b, _ := val.(bool)

		return &ddbtypes.AttributeValueMemberBOOL{Value: b}, nil
	case "NULL":
		return &ddbtypes.AttributeValueMemberNULL{Value: true}, nil
	case "SS", "NS", "BS", "L", "M":
		return attributeCollection(key, val)
	default:
		return nil, fmt.Errorf("%w: unknown AttributeValue type %q", errSDKBadValue, key)
	}
}

func attributeCollection(key string, val any) (ddbtypes.AttributeValue, error) {
	if key == "M" {
		return attributeMap(val)
	}

	items, _ := val.([]any)

	switch key {
	case "L":
		list := make([]ddbtypes.AttributeValue, 0, len(items))

		for _, it := range items {
			av, err := decodeAttributeValue(it)
			if err != nil {
				return nil, err
			}

			list = append(list, av)
		}

		return &ddbtypes.AttributeValueMemberL{Value: list}, nil
	case "BS":
		bs := make([][]byte, 0, len(items))

		for _, it := range items {
			s, _ := it.(string)

			raw, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", errSDKBadValue, err)
			}

			bs = append(bs, raw)
		}

		return &ddbtypes.AttributeValueMemberBS{Value: bs}, nil
	}

	strs := make([]string, 0, len(items))
	for _, it := range items {
		s, err := scalarString(it)
		if err != nil {
			return nil, err
		}

		strs = append(strs, s)
	}

	if key == "NS" {
		return &ddbtypes.AttributeValueMemberNS{Value: strs}, nil
	}

	return &ddbtypes.AttributeValueMemberSS{Value: strs}, nil
}

func attributeMap(val any) (ddbtypes.AttributeValue, error) {
	m, _ := val.(map[string]any)
	out := make(map[string]ddbtypes.AttributeValue, len(m))

	for k, it := range m {
		av, err := decodeAttributeValue(it)
		if err != nil {
			return nil, err
		}

		out[k] = av
	}

	return &ddbtypes.AttributeValueMemberM{Value: out}, nil
}

// encodeSDKOutput renders an SDK output value as the JSON-shaped value Step
// Functions exposes: PascalCase field names, nil members omitted.
func encodeSDKOutput(v reflect.Value) any {
	out, _ := encodeValue(v)

	return out
}

// encodeValue returns the JSON-shaped value and whether it is present (non-nil).
func encodeValue(v reflect.Value) (any, bool) {
	if !v.IsValid() {
		return nil, false
	}

	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return nil, false
		}

		return encodeValue(v.Elem())
	case reflect.Interface:
		if v.IsNil() {
			return nil, false
		}

		return encodeInterface(v)
	case reflect.Struct:
		return encodeStruct(v), true
	case reflect.Slice:
		return encodeSlice(v)
	case reflect.Map:
		return encodeMap(v)
	case reflect.String:
		return v.String(), true
	case reflect.Bool:
		return v.Bool(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint()), true
	case reflect.Float32, reflect.Float64:
		return v.Float(), true
	default:
		return nil, false
	}
}

func encodeInterface(v reflect.Value) (any, bool) {
	concrete := v.Elem()

	if r, ok := reflect.TypeAssert[io.Reader](concrete); ok {
		return drainReader(r), true
	}

	if ct := concrete.Type(); ct.Kind() == reflect.Pointer {
		if _, member, ok := strings.Cut(ct.Elem().Name(), "Member"); ok {
			inner, present := encodeValue(concrete.Elem().FieldByName("Value"))
			if !present {
				return nil, false
			}

			return map[string]any{member: inner}, true
		}
	}

	return encodeValue(concrete)
}

func drainReader(r io.Reader) string {
	b, _ := io.ReadAll(r)

	if c, ok := r.(io.Closer); ok {
		_ = c.Close()
	}

	return string(b)
}

func encodeStruct(v reflect.Value) any {
	if v.Type() == reflect.TypeFor[time.Time]() {
		t, _ := reflect.TypeAssert[time.Time](v)

		return t.UTC().Format(sdkTimeLayout)
	}

	out := map[string]any{}

	for i := range v.NumField() {
		f := v.Type().Field(i)
		if !f.IsExported() || f.Name == "ResultMetadata" {
			continue
		}

		if val, ok := encodeValue(v.Field(i)); ok {
			out[f.Name] = val
		}
	}

	return out
}

func encodeSlice(v reflect.Value) (any, bool) {
	if v.IsNil() {
		return nil, false
	}

	if v.Type().Elem().Kind() == reflect.Uint8 {
		return base64.StdEncoding.EncodeToString(v.Bytes()), true
	}

	out := make([]any, 0, v.Len())

	for i := range v.Len() {
		if val, ok := encodeValue(v.Index(i)); ok {
			out = append(out, val)
		}
	}

	return out, true
}

func encodeMap(v reflect.Value) (any, bool) {
	if v.IsNil() {
		return nil, false
	}

	out := make(map[string]any, v.Len())

	for it := v.MapRange(); it.Next(); {
		if val, ok := encodeValue(it.Value()); ok {
			out[fmt.Sprint(it.Key().Interface())] = val
		}
	}

	return out, true
}
