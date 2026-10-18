package backup

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func encodeDDBItem(item map[string]ddbtypes.AttributeValue) (map[string]json.RawMessage, error) {
	out := make(map[string]json.RawMessage, len(item))
	for k, v := range item {
		w, err := ddbAttrToWire(v)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(w)
		if err != nil {
			return nil, fmt.Errorf("encode attribute %s: %w", k, err)
		}
		out[k] = raw
	}

	return out, nil
}

func decodeDDBItem(raw map[string]json.RawMessage) (map[string]ddbtypes.AttributeValue, error) {
	out := make(map[string]ddbtypes.AttributeValue, len(raw))
	for k, r := range raw {
		var w map[string]any
		if err := json.Unmarshal(r, &w); err != nil {
			return nil, fmt.Errorf("decode attribute %s: %w", k, err)
		}
		av, err := ddbWireToAttr(w)
		if err != nil {
			return nil, err
		}
		out[k] = av
	}

	return out, nil
}

//nolint:ireturn // SDK union interface
func ddbWireToAttr(w map[string]any) (ddbtypes.AttributeValue, error) {
	for tag, v := range w {
		switch tag {
		case "S":
			s, _ := v.(string)

			return &ddbtypes.AttributeValueMemberS{Value: s}, nil
		case "N":
			s, _ := v.(string)

			return &ddbtypes.AttributeValueMemberN{Value: s}, nil
		case "B":
			s, _ := v.(string)
			raw, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, fmt.Errorf("decode binary attribute: %w", err)
			}

			return &ddbtypes.AttributeValueMemberB{Value: raw}, nil
		case "BOOL":
			b, _ := v.(bool)

			return &ddbtypes.AttributeValueMemberBOOL{Value: b}, nil
		case "NULL":
			return &ddbtypes.AttributeValueMemberNULL{Value: true}, nil
		case "SS":
			return &ddbtypes.AttributeValueMemberSS{Value: toStrings(v)}, nil
		case "NS":
			return &ddbtypes.AttributeValueMemberNS{Value: toStrings(v)}, nil
		case "BS":
			var bs [][]byte
			for _, s := range toStrings(v) {
				raw, err := base64.StdEncoding.DecodeString(s)
				if err != nil {
					return nil, fmt.Errorf("decode binary set: %w", err)
				}
				bs = append(bs, raw)
			}

			return &ddbtypes.AttributeValueMemberBS{Value: bs}, nil
		case "M":
			m, _ := v.(map[string]any)

			return ddbWireMap(m)
		case "L":
			l, _ := v.([]any)

			return ddbWireList(l)
		}
	}

	return nil, fmt.Errorf("%w: unknown DynamoDB attribute %v", ErrValidation, w)
}

//nolint:ireturn // SDK union interface
func ddbWireMap(m map[string]any) (ddbtypes.AttributeValue, error) {
	out := make(map[string]ddbtypes.AttributeValue, len(m))
	for k, v := range m {
		inner, _ := v.(map[string]any)
		av, err := ddbWireToAttr(inner)
		if err != nil {
			return nil, err
		}
		out[k] = av
	}

	return &ddbtypes.AttributeValueMemberM{Value: out}, nil
}

//nolint:ireturn // SDK union interface
func ddbWireList(l []any) (ddbtypes.AttributeValue, error) {
	out := make([]ddbtypes.AttributeValue, 0, len(l))
	for _, v := range l {
		inner, _ := v.(map[string]any)
		av, err := ddbWireToAttr(inner)
		if err != nil {
			return nil, err
		}
		out = append(out, av)
	}

	return &ddbtypes.AttributeValueMemberL{Value: out}, nil
}

func toStrings(v any) []string {
	l, _ := v.([]any)
	out := make([]string, 0, len(l))
	for _, e := range l {
		s, _ := e.(string)
		out = append(out, s)
	}

	return out
}

func ddbAttrToWire(av ddbtypes.AttributeValue) (map[string]any, error) {
	switch v := av.(type) {
	case *ddbtypes.AttributeValueMemberM:
		m := make(map[string]any, len(v.Value))
		for k, inner := range v.Value {
			w, err := ddbAttrToWire(inner)
			if err != nil {
				return nil, err
			}
			m[k] = w
		}

		return map[string]any{"M": m}, nil
	case *ddbtypes.AttributeValueMemberL:
		l := make([]any, 0, len(v.Value))
		for _, inner := range v.Value {
			w, err := ddbAttrToWire(inner)
			if err != nil {
				return nil, err
			}
			l = append(l, w)
		}

		return map[string]any{"L": l}, nil
	}

	return ddbScalarToWire(av)
}

func ddbScalarToWire(av ddbtypes.AttributeValue) (map[string]any, error) {
	switch v := av.(type) {
	case *ddbtypes.AttributeValueMemberS:
		return map[string]any{"S": v.Value}, nil
	case *ddbtypes.AttributeValueMemberN:
		return map[string]any{"N": v.Value}, nil
	case *ddbtypes.AttributeValueMemberB:
		return map[string]any{"B": base64.StdEncoding.EncodeToString(v.Value)}, nil
	case *ddbtypes.AttributeValueMemberBOOL:
		return map[string]any{"BOOL": v.Value}, nil
	case *ddbtypes.AttributeValueMemberNULL:
		return map[string]any{"NULL": true}, nil
	case *ddbtypes.AttributeValueMemberSS:
		return map[string]any{"SS": v.Value}, nil
	case *ddbtypes.AttributeValueMemberNS:
		return map[string]any{"NS": v.Value}, nil
	case *ddbtypes.AttributeValueMemberBS:
		bs := make([]string, 0, len(v.Value))
		for _, b := range v.Value {
			bs = append(bs, base64.StdEncoding.EncodeToString(b))
		}

		return map[string]any{"BS": bs}, nil
	}

	return nil, fmt.Errorf("%w: unsupported DynamoDB attribute %T", ErrValidation, av)
}
