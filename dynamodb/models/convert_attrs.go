package models

import (
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	ErrNotMap               = errors.New("expected map[string]any for AttributeValue")
	ErrInvalidTypeKeyCount  = errors.New("expected exactly one type key in AttributeValue map")
	ErrInvalidTypeS         = errors.New("expected string for S type")
	ErrInvalidTypeN         = errors.New("expected string for N type")
	ErrInvalidTypeB         = errors.New("expected base64 string for B type")
	ErrInvalidTypeBOOL      = errors.New("expected bool for BOOL type")
	ErrInvalidTypeNULL      = errors.New("expected true for NULL type")
	ErrInvalidTypeM         = errors.New("expected map for M type")
	ErrInvalidTypeL         = errors.New("expected slice for L type")
	ErrInvalidTypeSS        = errors.New("expected slice for SS type")
	ErrInvalidStringInSS    = errors.New("expected string in SS")
	ErrInvalidTypeNS        = errors.New("expected slice for NS type")
	ErrInvalidStringInNS    = errors.New("expected string in NS")
	ErrInvalidTypeBS        = errors.New("expected slice for BS type")
	ErrInvalidStringInBS    = errors.New("expected string in BS")
	ErrUnknownAttributeType = errors.New("unknown attribute value type")
)

func toStringSlice(val any, errType error, errItem error) ([]string, error) {
	switch v := val.(type) {
	case []string:
		return v, nil
	case []any:
		var ss []string
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%w, got %T", errItem, item)
			}
			ss = append(ss, s)
		}

		return ss, nil
	default:
		return nil, fmt.Errorf("%w, got %T", errType, val)
	}
}

func decodeBinary(val any, errType error) ([]byte, error) {
	switch v := val.(type) {
	case []byte:
		return v, nil
	case string:
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid base64, %w", errType, err)
		}

		return b, nil
	default:
		return nil, fmt.Errorf("%w, got %T", errType, val)
	}
}

func toByteSlice(val any, errType error, errItem error) ([][]byte, error) {
	switch v := val.(type) {
	case [][]byte:
		return v, nil
	case []any:
		var bs [][]byte
		for _, item := range v {
			b, err := decodeBinary(item, errItem)
			if err != nil {
				return nil, err
			}
			bs = append(bs, b)
		}

		return bs, nil
	default:
		return nil, fmt.Errorf("%w, got %T", errType, val)
	}
}

func convertStringType(val any) (types.AttributeValue, error) {
	s, matched := val.(string)
	if !matched {
		return nil, fmt.Errorf("%w, got %T", ErrInvalidTypeS, val)
	}

	return &types.AttributeValueMemberS{Value: s}, nil
}

func convertNumberType(val any) (types.AttributeValue, error) {
	s, matched := val.(string)
	if !matched {
		return nil, fmt.Errorf("%w, got %T", ErrInvalidTypeN, val)
	}

	return &types.AttributeValueMemberN{Value: s}, nil
}

func convertBinaryType(val any) (types.AttributeValue, error) {
	b, err := decodeBinary(val, ErrInvalidTypeB)
	if err != nil {
		return nil, err
	}

	return &types.AttributeValueMemberB{Value: b}, nil
}

func convertBoolType(val any) (types.AttributeValue, error) {
	bVal, matched := val.(bool)
	if !matched {
		return nil, fmt.Errorf("%w, got %T", ErrInvalidTypeBOOL, val)
	}

	return &types.AttributeValueMemberBOOL{Value: bVal}, nil
}

func convertNullType(val any) (types.AttributeValue, error) {
	b, matched := val.(bool)
	if !matched {
		return nil, fmt.Errorf("%w, got %T", ErrInvalidTypeNULL, val)
	}

	return &types.AttributeValueMemberNULL{Value: b}, nil
}

func convertStringSetType(val any) (types.AttributeValue, error) {
	ss, err := toStringSlice(val, ErrInvalidTypeSS, ErrInvalidStringInSS)
	if err != nil {
		return nil, err
	}

	return &types.AttributeValueMemberSS{Value: ss}, nil
}

func convertNumberSetType(val any) (types.AttributeValue, error) {
	ns, err := toStringSlice(val, ErrInvalidTypeNS, ErrInvalidStringInNS)
	if err != nil {
		return nil, err
	}

	return &types.AttributeValueMemberNS{Value: ns}, nil
}

func convertBinarySetType(val any) (types.AttributeValue, error) {
	bs, err := toByteSlice(val, ErrInvalidTypeBS, ErrInvalidStringInBS)
	if err != nil {
		return nil, err
	}

	return &types.AttributeValueMemberBS{Value: bs}, nil
}

func convertMapType(val any) (types.AttributeValue, error) {
	mVal, matched := val.(map[string]any)
	if !matched {
		return nil, fmt.Errorf("%w, got %T", ErrInvalidTypeM, val)
	}

	return ToSDKMapAttribute(mVal)
}

func convertListType(val any) (types.AttributeValue, error) {
	lVal, matched := val.([]any)
	if !matched {
		return nil, fmt.Errorf("%w, got %T", ErrInvalidTypeL, val)
	}

	return ToSDKListAttribute(lVal)
}

// ToSDKAttributeValue converts a raw Go value (from JSON unmarshal) to an SDK AttributeValue.
func ToSDKAttributeValue(v any) (types.AttributeValue, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w, got %T", ErrNotMap, v)
	}

	if len(m) != 1 {
		return nil, fmt.Errorf("%w, got %d", ErrInvalidTypeKeyCount, len(m))
	}

	for k, val := range m {
		switch k {
		case "S":
			return convertStringType(val)
		case "N":
			return convertNumberType(val)
		case "B":
			return convertBinaryType(val)
		case "BOOL":
			return convertBoolType(val)
		case "NULL":
			return convertNullType(val)
		case "M":
			return convertMapType(val)
		case "L":
			return convertListType(val)
		case "SS":
			return convertStringSetType(val)
		case "NS":
			return convertNumberSetType(val)
		case "BS":
			return convertBinarySetType(val)
		}
	}

	return nil, ErrUnknownAttributeType
}

func ToSDKMapAttribute(m map[string]any) (*types.AttributeValueMemberM, error) {
	out := make(map[string]types.AttributeValue)
	for k, v := range m {
		sdkVal, err := ToSDKAttributeValue(v)
		if err != nil {
			return nil, err
		}
		out[k] = sdkVal
	}

	return &types.AttributeValueMemberM{Value: out}, nil
}

func ToSDKListAttribute(l []any) (types.AttributeValue, error) {
	var out []types.AttributeValue
	for _, v := range l {
		sdkVal, err := ToSDKAttributeValue(v)
		if err != nil {
			return nil, err
		}
		out = append(out, sdkVal)
	}

	return &types.AttributeValueMemberL{Value: out}, nil
}

// FromSDKAttributeValue converts an SDK AttributeValue back to a raw map[string]any
// suitable for JSON marshaling (wire format).
func FromSDKAttributeValue(av types.AttributeValue) any {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return map[string]any{"S": v.Value}
	case *types.AttributeValueMemberN:
		return map[string]any{"N": v.Value}
	case *types.AttributeValueMemberB:
		return map[string]any{"B": base64.StdEncoding.EncodeToString(v.Value)}
	case *types.AttributeValueMemberBOOL:
		return map[string]any{"BOOL": v.Value}
	case *types.AttributeValueMemberNULL:
		return map[string]any{"NULL": true}
	case *types.AttributeValueMemberM:
		m := make(map[string]any)
		for k, val := range v.Value {
			m[k] = FromSDKAttributeValue(val)
		}

		return map[string]any{"M": m}
	case *types.AttributeValueMemberL:
		l := make([]any, len(v.Value))
		for i, val := range v.Value {
			l[i] = FromSDKAttributeValue(val)
		}

		return map[string]any{"L": l}
	case *types.AttributeValueMemberSS:
		return map[string]any{"SS": v.Value}
	case *types.AttributeValueMemberNS:
		return map[string]any{"NS": v.Value}
	case *types.AttributeValueMemberBS:
		bs := make([]any, len(v.Value))
		for i, val := range v.Value {
			bs[i] = base64.StdEncoding.EncodeToString(val)
		}

		return map[string]any{"BS": bs}
	default:
		return nil
	}
}

// ToSDKItem converts a map[string]any (wire item) to map[string]types.AttributeValue.
func ToSDKItem(item map[string]any) (map[string]types.AttributeValue, error) {
	out := make(map[string]types.AttributeValue)
	for k, v := range item {
		av, err := ToSDKAttributeValue(v)
		if err != nil {
			return nil, err
		}
		out[k] = av
	}

	return out, nil
}

// FromSDKItem converts map[string]types.AttributeValue back to map[string]any (wire item).
func FromSDKItem(item map[string]types.AttributeValue) map[string]any {
	out := make(map[string]any)
	for k, v := range item {
		out[k] = FromSDKAttributeValue(v)
	}

	return out
}

// ToSDKKeySchema converts internal KeySchema to SDK type.
func ToSDKKeySchema(schema []KeySchemaElement) []types.KeySchemaElement {
	sdkSchema := make([]types.KeySchemaElement, len(schema))
	for i, k := range schema {
		name := k.AttributeName
		sdkSchema[i] = types.KeySchemaElement{
			AttributeName: &name,
			KeyType:       types.KeyType(k.KeyType),
		}
	}

	return sdkSchema
}

func FromSDKKeySchema(schema []types.KeySchemaElement) []KeySchemaElement {
	out := make([]KeySchemaElement, len(schema))
	for i, k := range schema {
		out[i] = KeySchemaElement{
			AttributeName: aws.ToString(k.AttributeName),
			KeyType:       string(k.KeyType),
		}
	}

	return out
}

// ToSDKAttributeDefinitions converts internal AttributeDefinitions to SDK type.
func ToSDKAttributeDefinitions(defs []AttributeDefinition) []types.AttributeDefinition {
	sdkDefs := make([]types.AttributeDefinition, len(defs))
	for i, d := range defs {
		name := d.AttributeName
		sdkDefs[i] = types.AttributeDefinition{
			AttributeName: &name,
			AttributeType: types.ScalarAttributeType(d.AttributeType),
		}
	}

	return sdkDefs
}

func FromSDKAttributeDefinitions(defs []types.AttributeDefinition) []AttributeDefinition {
	out := make([]AttributeDefinition, len(defs))
	for i, d := range defs {
		out[i] = AttributeDefinition{
			AttributeName: ptrconv.String(d.AttributeName),
			AttributeType: string(d.AttributeType),
		}
	}

	return out
}

func ToSDKGlobalSecondaryIndexes(gsis []GlobalSecondaryIndex) []types.GlobalSecondaryIndex {
	if len(gsis) == 0 {
		return nil
	}
	out := make([]types.GlobalSecondaryIndex, len(gsis))
	for i, gsi := range gsis {
		name := gsi.IndexName
		out[i] = types.GlobalSecondaryIndex{
			IndexName:             &name,
			KeySchema:             ToSDKKeySchema(gsi.KeySchema),
			Projection:            ToSDKProjection(gsi.Projection),
			ProvisionedThroughput: ToSDKProvisionedThroughput(gsi.ProvisionedThroughput),
		}
	}

	return out
}

func ToSDKLocalSecondaryIndexes(lsis []LocalSecondaryIndex) []types.LocalSecondaryIndex {
	if len(lsis) == 0 {
		return nil
	}
	out := make([]types.LocalSecondaryIndex, len(lsis))
	for i, lsi := range lsis {
		name := lsi.IndexName
		out[i] = types.LocalSecondaryIndex{
			IndexName:  &name,
			KeySchema:  ToSDKKeySchema(lsi.KeySchema),
			Projection: ToSDKProjection(lsi.Projection),
		}
	}

	return out
}

func ToSDKProvisionedThroughput(pt ProvisionedThroughput) *types.ProvisionedThroughput {
	return &types.ProvisionedThroughput{
		ReadCapacityUnits:  pt.ReadCapacityUnits,
		WriteCapacityUnits: pt.WriteCapacityUnits,
	}
}

func FromSDKGlobalSecondaryIndexes(gsis []types.GlobalSecondaryIndex) []GlobalSecondaryIndex {
	out := make([]GlobalSecondaryIndex, len(gsis))
	for i, gsi := range gsis {
		pt := ProvisionedThroughput{}
		if gsi.ProvisionedThroughput != nil {
			pt.ReadCapacityUnits = gsi.ProvisionedThroughput.ReadCapacityUnits
			pt.WriteCapacityUnits = gsi.ProvisionedThroughput.WriteCapacityUnits
		}

		out[i] = GlobalSecondaryIndex{
			IndexName:             ptrconv.String(gsi.IndexName),
			KeySchema:             FromSDKKeySchema(gsi.KeySchema),
			Projection:            FromSDKProjection(gsi.Projection),
			ProvisionedThroughput: pt,
		}
	}

	return out
}

func FromSDKLocalSecondaryIndexes(lsis []types.LocalSecondaryIndex) []LocalSecondaryIndex {
	out := make([]LocalSecondaryIndex, len(lsis))
	for i, lsi := range lsis {
		out[i] = LocalSecondaryIndex{
			IndexName:  ptrconv.String(lsi.IndexName),
			KeySchema:  FromSDKKeySchema(lsi.KeySchema),
			Projection: FromSDKProjection(lsi.Projection),
		}
	}

	return out
}

func FromSDKProjection(p *types.Projection) Projection {
	if p == nil {
		return Projection{}
	}

	return Projection{
		ProjectionType:   string(p.ProjectionType),
		NonKeyAttributes: p.NonKeyAttributes,
	}
}

func ToSDKGlobalSecondaryIndexDescriptions(
	gsis []GlobalSecondaryIndexDescription,
) []types.GlobalSecondaryIndexDescription {
	out := make([]types.GlobalSecondaryIndexDescription, len(gsis))
	for i, gsi := range gsis {
		name := gsi.IndexName
		status := types.IndexStatus(gsi.IndexStatus)
		itemCount := int64(gsi.ItemCount)
		rcu := int64(gsi.ProvisionedThroughput.ReadCapacityUnits)
		wcu := int64(gsi.ProvisionedThroughput.WriteCapacityUnits)

		out[i] = types.GlobalSecondaryIndexDescription{
			IndexName:   &name,
			IndexStatus: status,
			KeySchema:   ToSDKKeySchema(gsi.KeySchema),
			Projection:  ToSDKProjection(gsi.Projection),
			ProvisionedThroughput: &types.ProvisionedThroughputDescription{
				ReadCapacityUnits:  &rcu,
				WriteCapacityUnits: &wcu,
			},
			ItemCount: &itemCount,
		}
	}

	return out
}

func ToSDKLocalSecondaryIndexDescriptions(
	lsis []LocalSecondaryIndexDescription,
) []types.LocalSecondaryIndexDescription {
	out := make([]types.LocalSecondaryIndexDescription, len(lsis))
	for i, lsi := range lsis {
		name := lsi.IndexName
		itemCount := int64(lsi.ItemCount)
		size := lsi.IndexSizeBytes

		out[i] = types.LocalSecondaryIndexDescription{
			IndexName:      &name,
			KeySchema:      ToSDKKeySchema(lsi.KeySchema),
			Projection:     ToSDKProjection(lsi.Projection),
			IndexSizeBytes: &size,
			ItemCount:      &itemCount,
		}
	}

	return out
}

func ToSDKProjection(p Projection) *types.Projection {
	pt := types.ProjectionType(p.ProjectionType)

	return &types.Projection{
		ProjectionType:   pt,
		NonKeyAttributes: p.NonKeyAttributes,
	}
}
