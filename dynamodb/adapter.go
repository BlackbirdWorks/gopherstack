package dynamodb

import (
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	errNotMap               = errors.New("expected map[string]any for AttributeValue")
	errInvalidTypeKeyCount  = errors.New("expected exactly one type key in AttributeValue map")
	errInvalidTypeS         = errors.New("expected string for S type")
	errInvalidTypeN         = errors.New("expected string for N type")
	errInvalidTypeB         = errors.New("expected base64 string for B type")
	errInvalidTypeBOOL      = errors.New("expected bool for BOOL type")
	errInvalidTypeNULL      = errors.New("expected true for NULL type")
	errInvalidTypeM         = errors.New("expected map for M type")
	errInvalidTypeL         = errors.New("expected slice for L type")
	errInvalidTypeSS        = errors.New("expected slice for SS type")
	errInvalidStringInSS    = errors.New("expected string in SS")
	errInvalidTypeNS        = errors.New("expected slice for NS type")
	errInvalidStringInNS    = errors.New("expected string in NS")
	errInvalidTypeBS        = errors.New("expected slice for BS type")
	errInvalidStringInBS    = errors.New("expected string in BS")
	errUnknownAttributeType = errors.New("unknown attribute value type")
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
			return nil, fmt.Errorf("%w: invalid base64, %v", errType, err)
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
		return nil, fmt.Errorf("%w, got %T", errInvalidTypeS, val)
	}

	return &types.AttributeValueMemberS{Value: s}, nil
}

func convertNumberType(val any) (types.AttributeValue, error) {
	s, matched := val.(string)
	if !matched {
		return nil, fmt.Errorf("%w, got %T", errInvalidTypeN, val)
	}

	return &types.AttributeValueMemberN{Value: s}, nil
}

func convertBinaryType(val any) (types.AttributeValue, error) {
	b, err := decodeBinary(val, errInvalidTypeB)
	if err != nil {
		return nil, err
	}

	return &types.AttributeValueMemberB{Value: b}, nil
}

func convertBoolType(val any) (types.AttributeValue, error) {
	bVal, matched := val.(bool)
	if !matched {
		return nil, fmt.Errorf("%w, got %T", errInvalidTypeBOOL, val)
	}

	return &types.AttributeValueMemberBOOL{Value: bVal}, nil
}

func convertNullType(val any) (types.AttributeValue, error) {
	b, matched := val.(bool)
	if !matched {
		return nil, fmt.Errorf("%w, got %T", errInvalidTypeNULL, val)
	}

	return &types.AttributeValueMemberNULL{Value: b}, nil
}

func convertStringSetType(val any) (types.AttributeValue, error) {
	ss, err := toStringSlice(val, errInvalidTypeSS, errInvalidStringInSS)
	if err != nil {
		return nil, err
	}

	return &types.AttributeValueMemberSS{Value: ss}, nil
}

func convertNumberSetType(val any) (types.AttributeValue, error) {
	ns, err := toStringSlice(val, errInvalidTypeNS, errInvalidStringInNS)
	if err != nil {
		return nil, err
	}

	return &types.AttributeValueMemberNS{Value: ns}, nil
}

func convertBinarySetType(val any) (types.AttributeValue, error) {
	bs, err := toByteSlice(val, errInvalidTypeBS, errInvalidStringInBS)
	if err != nil {
		return nil, err
	}

	return &types.AttributeValueMemberBS{Value: bs}, nil
}

func convertMapType(val any) (types.AttributeValue, error) {
	mVal, matched := val.(map[string]any)
	if !matched {
		return nil, fmt.Errorf("%w, got %T", errInvalidTypeM, val)
	}

	return ToSDKMapAttribute(mVal)
}

func convertListType(val any) (types.AttributeValue, error) {
	lVal, matched := val.([]any)
	if !matched {
		return nil, fmt.Errorf("%w, got %T", errInvalidTypeL, val)
	}

	return ToSDKListAttribute(lVal)
}

// ToSDKAttributeValue converts a raw Go value (from JSON unmarshal) to an SDK AttributeValue.
func ToSDKAttributeValue(v any) (types.AttributeValue, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w, got %T", errNotMap, v)
	}

	if len(m) != 1 {
		return nil, fmt.Errorf("%w, got %d", errInvalidTypeKeyCount, len(m))
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

	return nil, errUnknownAttributeType
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
			AttributeName: safeToString(d.AttributeName),
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
			IndexName:             safeToString(gsi.IndexName),
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
			IndexName:  safeToString(lsi.IndexName),
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

// --- CRUD Adapters ---

func ToSDKPutItemInput(input *PutItemInput) (*dynamodb.PutItemInput, error) {
	item, err := ToSDKItem(input.Item)
	if err != nil {
		return nil, err
	}

	out := &dynamodb.PutItemInput{
		TableName:                   &input.TableName,
		Item:                        item,
		ConditionExpression:         nilIfEmpty(input.ConditionExpression),
		ExpressionAttributeNames:    input.ExpressionAttributeNames,
		ReturnValues:                types.ReturnValue(input.ReturnValues),
		ReturnConsumedCapacity:      types.ReturnConsumedCapacity(input.ReturnConsumedCapacity),
		ReturnItemCollectionMetrics: types.ReturnItemCollectionMetrics(input.ReturnItemCollectionMetrics),
	}

	if len(input.ExpressionAttributeValues) > 0 {
		vals, valsErr := ToSDKItem(input.ExpressionAttributeValues)
		if valsErr != nil {
			return nil, valsErr
		}
		out.ExpressionAttributeValues = vals
	}

	return out, nil
}

func FromSDKPutItemOutput(output *dynamodb.PutItemOutput) *PutItemOutput {
	out := &PutItemOutput{}
	if len(output.Attributes) > 0 {
		out.Attributes = FromSDKItem(output.Attributes)
	}
	if output.ConsumedCapacity != nil {
		out.ConsumedCapacity = FromSDKConsumedCapacity(output.ConsumedCapacity)
	}
	if output.ItemCollectionMetrics != nil {
		out.ItemCollectionMetrics = FromSDKItemCollectionMetrics(output.ItemCollectionMetrics)
	}

	return out
}

func ToSDKGetItemInput(input *GetItemInput) (*dynamodb.GetItemInput, error) {
	key, err := ToSDKItem(input.Key)
	if err != nil {
		return nil, err
	}

	return &dynamodb.GetItemInput{
		TableName:                &input.TableName,
		Key:                      key,
		ExpressionAttributeNames: input.ExpressionAttributeNames,
		ProjectionExpression:     nilIfEmpty(input.ProjectionExpression),
	}, nil
}

func FromSDKGetItemOutput(output *dynamodb.GetItemOutput) *GetItemOutput {
	out := &GetItemOutput{}
	if len(output.Item) > 0 {
		out.Item = FromSDKItem(output.Item)
	}
	// ConsumedCapacity missing in current types.go GetItemOutput
	return out
}

func ToSDKDeleteItemInput(input *DeleteItemInput) (*dynamodb.DeleteItemInput, error) {
	key, err := ToSDKItem(input.Key)
	if err != nil {
		return nil, err
	}

	out := &dynamodb.DeleteItemInput{
		TableName:                &input.TableName,
		Key:                      key,
		ConditionExpression:      nilIfEmpty(input.ConditionExpression),
		ExpressionAttributeNames: input.ExpressionAttributeNames,
	}

	if len(input.ExpressionAttributeValues) > 0 {
		vals, valsErr := ToSDKItem(input.ExpressionAttributeValues)
		if valsErr != nil {
			return nil, valsErr
		}
		out.ExpressionAttributeValues = vals
	}

	return out, nil
}

func FromSDKDeleteItemOutput(*dynamodb.DeleteItemOutput) *DeleteItemOutput {
	return &DeleteItemOutput{}
}

func ToSDKUpdateItemInput(input *UpdateItemInput) (*dynamodb.UpdateItemInput, error) {
	key, err := ToSDKItem(input.Key)
	if err != nil {
		return nil, err
	}

	out := &dynamodb.UpdateItemInput{
		TableName:                   &input.TableName,
		Key:                         key,
		UpdateExpression:            nilIfEmpty(input.UpdateExpression),
		ConditionExpression:         nilIfEmpty(input.ConditionExpression),
		ExpressionAttributeNames:    input.ExpressionAttributeNames,
		ReturnValues:                types.ReturnValue(input.ReturnValues),
		ReturnConsumedCapacity:      types.ReturnConsumedCapacity(input.ReturnConsumedCapacity),
		ReturnItemCollectionMetrics: types.ReturnItemCollectionMetrics(input.ReturnItemCollectionMetrics),
	}

	if len(input.ExpressionAttributeValues) > 0 {
		vals, valsErr := ToSDKItem(input.ExpressionAttributeValues)
		if valsErr != nil {
			return nil, valsErr
		}
		out.ExpressionAttributeValues = vals
	}

	return out, nil
}

func FromSDKUpdateItemOutput(output *dynamodb.UpdateItemOutput) *UpdateItemOutput {
	out := &UpdateItemOutput{}
	if len(output.Attributes) > 0 {
		out.Attributes = FromSDKItem(output.Attributes)
	}
	if output.ConsumedCapacity != nil {
		out.ConsumedCapacity = FromSDKConsumedCapacity(output.ConsumedCapacity)
	}
	if output.ItemCollectionMetrics != nil {
		out.ItemCollectionMetrics = FromSDKItemCollectionMetrics(output.ItemCollectionMetrics)
	}

	return out
}

func ToSDKScanInput(input *ScanInput) (*dynamodb.ScanInput, error) {
	out := &dynamodb.ScanInput{
		TableName:                &input.TableName,
		IndexName:                nilIfEmpty(input.IndexName),
		FilterExpression:         nilIfEmpty(input.FilterExpression),
		ProjectionExpression:     nilIfEmpty(input.ProjectionExpression),
		ExpressionAttributeNames: input.ExpressionAttributeNames,
		Limit:                    input.Limit,
	}

	if len(input.ExpressionAttributeValues) > 0 {
		vals, valsErr := ToSDKItem(input.ExpressionAttributeValues)
		if valsErr != nil {
			return nil, valsErr
		}
		out.ExpressionAttributeValues = vals
	}

	return out, nil
}

func FromSDKScanOutput(output *dynamodb.ScanOutput) *ScanOutput {
	out := &ScanOutput{
		Count:        int(output.Count),
		ScannedCount: int(output.ScannedCount),
	}
	if len(output.Items) > 0 {
		out.Items = make([]map[string]any, len(output.Items))
		for i, item := range output.Items {
			out.Items[i] = FromSDKItem(item)
		}
	} else {
		out.Items = []map[string]any{}
	}

	return out
}

func ToSDKQueryInput(input *QueryInput) (*dynamodb.QueryInput, error) {
	out := &dynamodb.QueryInput{
		TableName:                &input.TableName,
		IndexName:                nilIfEmpty(input.IndexName),
		KeyConditionExpression:   nilIfEmpty(input.KeyConditionExpression),
		FilterExpression:         nilIfEmpty(input.FilterExpression),
		ProjectionExpression:     nilIfEmpty(input.ProjectionExpression),
		ExpressionAttributeNames: input.ExpressionAttributeNames,
		ScanIndexForward:         input.ScanIndexForward,
	}

	if input.Limit > 0 {
		out.Limit = &input.Limit
	}

	if len(input.ExpressionAttributeValues) > 0 {
		vals, valsErr := ToSDKItem(input.ExpressionAttributeValues)
		if valsErr != nil {
			return nil, valsErr
		}
		out.ExpressionAttributeValues = vals
	}

	if len(input.ExclusiveStartKey) > 0 {
		key, keyErr := ToSDKItem(input.ExclusiveStartKey)
		if keyErr != nil {
			return nil, keyErr
		}
		out.ExclusiveStartKey = key
	}

	return out, nil
}

func FromSDKQueryOutput(output *dynamodb.QueryOutput) *QueryOutput {
	out := &QueryOutput{
		Count:        int(output.Count),
		ScannedCount: int(output.ScannedCount),
	}
	if len(output.Items) > 0 {
		out.Items = make([]map[string]any, len(output.Items))
		for i, item := range output.Items {
			out.Items[i] = FromSDKItem(item)
		}
	} else {
		out.Items = []map[string]any{}
	}
	if len(output.LastEvaluatedKey) > 0 {
		out.LastEvaluatedKey = FromSDKItem(output.LastEvaluatedKey)
	}
	if output.ConsumedCapacity != nil {
		out.ConsumedCapacity = FromSDKConsumedCapacity(output.ConsumedCapacity)
	}

	return out
}

// --- Batch Adapters ---

func ToSDKBatchGetItemInput(input *BatchGetItemInput) (*dynamodb.BatchGetItemInput, error) {
	requestItems := make(map[string]types.KeysAndAttributes)
	for tableName, keysAndAttrs := range input.RequestItems {
		var sdkKeys []map[string]types.AttributeValue
		for _, k := range keysAndAttrs.Keys {
			sdkKey, err := ToSDKItem(k)
			if err != nil {
				return nil, err
			}
			sdkKeys = append(sdkKeys, sdkKey)
		}

		requestItems[tableName] = types.KeysAndAttributes{
			Keys:                     sdkKeys,
			AttributesToGet:          keysAndAttrs.AttributesToGet,
			ConsistentRead:           keysAndAttrs.ConsistentRead,
			ExpressionAttributeNames: keysAndAttrs.ExpressionAttributeNames,
			ProjectionExpression:     nilIfEmpty(keysAndAttrs.ProjectionExpression),
		}
	}

	return &dynamodb.BatchGetItemInput{
		RequestItems: requestItems,
	}, nil
}

func FromSDKBatchGetItemOutput(output *dynamodb.BatchGetItemOutput) *BatchGetItemOutput {
	responses := make(map[string][]map[string]any)
	for tableName, items := range output.Responses {
		convertedItems := make([]map[string]any, len(items))
		for i, item := range items {
			convertedItems[i] = FromSDKItem(item)
		}
		responses[tableName] = convertedItems
	}

	unprocessedKeys := make(map[string]KeysAndAttributes)
	for tableName, keysAndAttrs := range output.UnprocessedKeys {
		convertedKeys := make([]map[string]any, len(keysAndAttrs.Keys))
		for i, k := range keysAndAttrs.Keys {
			convertedKeys[i] = FromSDKItem(k)
		}
		unprocessedKeys[tableName] = KeysAndAttributes{
			Keys:                     convertedKeys,
			AttributesToGet:          keysAndAttrs.AttributesToGet,
			ConsistentRead:           keysAndAttrs.ConsistentRead,
			ExpressionAttributeNames: keysAndAttrs.ExpressionAttributeNames,
			ProjectionExpression:     safeToString(keysAndAttrs.ProjectionExpression),
		}
	}

	return &BatchGetItemOutput{
		Responses:       responses,
		UnprocessedKeys: unprocessedKeys,
	}
}

func ToSDKBatchWriteItemInput(input *BatchWriteItemInput) (*dynamodb.BatchWriteItemInput, error) {
	requestItems := make(map[string][]types.WriteRequest)

	for tableName, requests := range input.RequestItems {
		sdkRequests := make([]types.WriteRequest, 0, len(requests))
		for _, req := range requests {
			sdkReq := types.WriteRequest{}
			if req.PutRequest != nil {
				item, err := ToSDKItem(req.PutRequest.Item)
				if err != nil {
					return nil, err
				}
				sdkReq.PutRequest = &types.PutRequest{Item: item}
			}
			if req.DeleteRequest != nil {
				key, err := ToSDKItem(req.DeleteRequest.Key)
				if err != nil {
					return nil, err
				}
				sdkReq.DeleteRequest = &types.DeleteRequest{Key: key}
			}
			sdkRequests = append(sdkRequests, sdkReq)
		}
		requestItems[tableName] = sdkRequests
	}

	return &dynamodb.BatchWriteItemInput{
		RequestItems: requestItems,
	}, nil
}

func FromSDKBatchWriteItemOutput(output *dynamodb.BatchWriteItemOutput) *BatchWriteItemOutput {
	unprocessedItems := make(map[string][]WriteRequest)
	for tableName, requests := range output.UnprocessedItems {
		convertedRequests := make([]WriteRequest, len(requests))
		for i, req := range requests {
			cnvReq := WriteRequest{}
			if req.PutRequest != nil {
				cnvReq.PutRequest = &PutRequest{Item: FromSDKItem(req.PutRequest.Item)}
			}
			if req.DeleteRequest != nil {
				cnvReq.DeleteRequest = &DeleteRequest{Key: FromSDKItem(req.DeleteRequest.Key)}
			}
			convertedRequests[i] = cnvReq
		}
		unprocessedItems[tableName] = convertedRequests
	}

	consumedCapacity := make([]ConsumedCapacity, len(output.ConsumedCapacity))
	for i, cc := range output.ConsumedCapacity {
		consumedCapacity[i] = *FromSDKConsumedCapacity(&cc)
	}

	return &BatchWriteItemOutput{
		UnprocessedItems: unprocessedItems,
		ConsumedCapacity: consumedCapacity,
	}
}

// --- Transact Adapters ---

func createPutTransactItem(item *TransactWriteItem) (*types.Put, error) {
	sdkPut, err := ToSDKPutItemInput(item.Put)
	if err != nil {
		return nil, err
	}

	return &types.Put{
		Item:                      sdkPut.Item,
		TableName:                 sdkPut.TableName,
		ConditionExpression:       sdkPut.ConditionExpression,
		ExpressionAttributeNames:  sdkPut.ExpressionAttributeNames,
		ExpressionAttributeValues: sdkPut.ExpressionAttributeValues,
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailure(
			item.Put.ReturnValues,
		),
	}, nil
}

func createDeleteTransactItem(item *TransactWriteItem) (*types.Delete, error) {
	sdkDel, err := ToSDKDeleteItemInput(item.Delete)
	if err != nil {
		return nil, err
	}

	return &types.Delete{
		Key:                       sdkDel.Key,
		TableName:                 sdkDel.TableName,
		ConditionExpression:       sdkDel.ConditionExpression,
		ExpressionAttributeNames:  sdkDel.ExpressionAttributeNames,
		ExpressionAttributeValues: sdkDel.ExpressionAttributeValues,
	}, nil
}

func createUpdateTransactItem(item *TransactWriteItem) (*types.Update, error) {
	sdkUpd, err := ToSDKUpdateItemInput(item.Update)
	if err != nil {
		return nil, err
	}

	return &types.Update{
		Key:                       sdkUpd.Key,
		TableName:                 sdkUpd.TableName,
		UpdateExpression:          sdkUpd.UpdateExpression,
		ConditionExpression:       sdkUpd.ConditionExpression,
		ExpressionAttributeNames:  sdkUpd.ExpressionAttributeNames,
		ExpressionAttributeValues: sdkUpd.ExpressionAttributeValues,
	}, nil
}

func createConditionCheckTransactItem(item *TransactWriteItem) (*types.ConditionCheck, error) {
	key, err := ToSDKItem(item.ConditionCheck.Key)
	if err != nil {
		return nil, err
	}
	cc := &types.ConditionCheck{
		Key:                      key,
		TableName:                &item.ConditionCheck.TableName,
		ConditionExpression:      &item.ConditionCheck.ConditionExpression,
		ExpressionAttributeNames: item.ConditionCheck.ExpressionAttributeNames,
	}
	if len(item.ConditionCheck.ExpressionAttributeValues) > 0 {
		vals, vErr := ToSDKItem(item.ConditionCheck.ExpressionAttributeValues)
		if vErr != nil {
			return nil, vErr
		}
		cc.ExpressionAttributeValues = vals
	}

	return cc, nil
}

func convertTransactWriteItem(item TransactWriteItem) (types.TransactWriteItem, error) {
	twi := types.TransactWriteItem{}

	if item.Put != nil {
		put, err := createPutTransactItem(&item)
		if err != nil {
			return twi, err
		}
		twi.Put = put
	}

	if item.Delete != nil {
		del, err := createDeleteTransactItem(&item)
		if err != nil {
			return twi, err
		}
		twi.Delete = del
	}

	if item.Update != nil {
		upd, err := createUpdateTransactItem(&item)
		if err != nil {
			return twi, err
		}
		twi.Update = upd
	}

	if item.ConditionCheck != nil {
		cc, err := createConditionCheckTransactItem(&item)
		if err != nil {
			return twi, err
		}
		twi.ConditionCheck = cc
	}

	return twi, nil
}

func ToSDKTransactWriteItemsInput(input *TransactWriteItemsInput) (*dynamodb.TransactWriteItemsInput, error) {
	items := make([]types.TransactWriteItem, 0, len(input.TransactItems))
	for _, item := range input.TransactItems {
		twi, err := convertTransactWriteItem(item)
		if err != nil {
			return nil, err
		}
		items = append(items, twi)
	}

	return &dynamodb.TransactWriteItemsInput{
		TransactItems:          items,
		ClientRequestToken:     nilIfEmpty(input.ClientRequestToken),
		ReturnConsumedCapacity: types.ReturnConsumedCapacity(input.ReturnConsumedCapacity),
		// ReturnItemCollectionMetrics
	}, nil
}

func FromSDKTransactWriteItemsOutput(*dynamodb.TransactWriteItemsOutput) *TransactWriteItemsOutput {
	return &TransactWriteItemsOutput{
		// Metrics
	}
}

func ToSDKTransactGetItemsInput(input *TransactGetItemsInput) (*dynamodb.TransactGetItemsInput, error) {
	items := make([]types.TransactGetItem, 0, len(input.TransactItems))
	for _, item := range input.TransactItems {
		if item.Get != nil {
			sdkGet, err := ToSDKGetItemInput(item.Get)
			if err != nil {
				return nil, err
			}
			items = append(items, types.TransactGetItem{
				Get: &types.Get{
					Key:                      sdkGet.Key,
					TableName:                sdkGet.TableName,
					ExpressionAttributeNames: sdkGet.ExpressionAttributeNames,
					ProjectionExpression:     sdkGet.ProjectionExpression,
				},
			})
		}
	}

	return &dynamodb.TransactGetItemsInput{
		TransactItems:          items,
		ReturnConsumedCapacity: types.ReturnConsumedCapacity(input.ReturnConsumedCapacity),
	}, nil
}

func FromSDKTransactGetItemsOutput(output *dynamodb.TransactGetItemsOutput) *TransactGetItemsOutput {
	responses := make([]ItemResponse, 0, len(output.Responses))
	for _, resp := range output.Responses {
		responses = append(responses, ItemResponse{
			Item: FromSDKItem(resp.Item),
		})
	}

	return &TransactGetItemsOutput{
		Responses: responses,
	}
}

// --- Table Adapters ---

func ToSDKCreateTableInput(input *CreateTableInput) *dynamodb.CreateTableInput {
	// ProvisionedThroughput input.ProvisionedThroughput is 'any' in types.go, handled loosely
	// We'll approximate for now or assume map structure.

	var pt *types.ProvisionedThroughput
	if m, ok := input.ProvisionedThroughput.(map[string]any); ok {
		pt = &types.ProvisionedThroughput{
			ReadCapacityUnits:  awsInt64FromAny(m["ReadCapacityUnits"]),
			WriteCapacityUnits: awsInt64FromAny(m["WriteCapacityUnits"]),
		}
	}

	return &dynamodb.CreateTableInput{
		TableName:              &input.TableName,
		KeySchema:              ToSDKKeySchema(input.KeySchema),
		AttributeDefinitions:   ToSDKAttributeDefinitions(input.AttributeDefinitions),
		GlobalSecondaryIndexes: ToSDKGlobalSecondaryIndexes(input.GlobalSecondaryIndexes),
		LocalSecondaryIndexes:  ToSDKLocalSecondaryIndexes(input.LocalSecondaryIndexes),
		ProvisionedThroughput:  pt,
	}
}

func FromSDKCreateTableOutput(output *dynamodb.CreateTableOutput) *CreateTableOutput {
	return &CreateTableOutput{
		TableDescription: FromSDKTableDescription(output.TableDescription),
	}
}

func ToSDKDeleteTableInput(input *DeleteTableInput) *dynamodb.DeleteTableInput {
	return &dynamodb.DeleteTableInput{
		TableName: &input.TableName,
	}
}

func FromSDKDeleteTableOutput(output *dynamodb.DeleteTableOutput) *DeleteTableOutput {
	return &DeleteTableOutput{
		TableDescription: FromSDKTableDescription(output.TableDescription),
	}
}

func ToSDKDescribeTableInput(input *DescribeTableInput) *dynamodb.DescribeTableInput {
	return &dynamodb.DescribeTableInput{
		TableName: &input.TableName,
	}
}

func FromSDKDescribeTableOutput(output *dynamodb.DescribeTableOutput) *DescribeTableOutput {
	return &DescribeTableOutput{
		Table: FromSDKTableDescription(output.Table),
	}
}

func ToSDKListTablesInput(input *ListTablesInput) *dynamodb.ListTablesInput {
	const maxInt32Value = 2147483647
	var l *int32

	if input.Limit > 0 {
		if input.Limit > maxInt32Value {
			val := int32(maxInt32Value)
			l = &val
		} else {
			val := int32(input.Limit) // #nosec G115
			l = &val
		}
	}

	return &dynamodb.ListTablesInput{
		Limit: l,
	}
}

func FromSDKListTablesOutput(output *dynamodb.ListTablesOutput) *ListTablesOutput {
	return &ListTablesOutput{
		TableNames: output.TableNames,
	}
}

func ToSDKUpdateTimeToLiveInput(input *UpdateTimeToLiveInput) *dynamodb.UpdateTimeToLiveInput {
	return &dynamodb.UpdateTimeToLiveInput{
		TableName: &input.TableName,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: &input.TimeToLiveSpecification.AttributeName,
			Enabled:       &input.TimeToLiveSpecification.Enabled,
		},
	}
}

func FromSDKUpdateTimeToLiveOutput(output *dynamodb.UpdateTimeToLiveOutput) *UpdateTimeToLiveOutput {
	return &UpdateTimeToLiveOutput{
		TimeToLiveSpecification: TimeToLiveSpecification{
			AttributeName: safeToString(output.TimeToLiveSpecification.AttributeName),
			Enabled:       safeToBool(output.TimeToLiveSpecification.Enabled),
		},
	}
}

func ToSDKDescribeTimeToLiveInput(input *DescribeTimeToLiveInput) *dynamodb.DescribeTimeToLiveInput {
	return &dynamodb.DescribeTimeToLiveInput{
		TableName: &input.TableName,
	}
}

func FromSDKDescribeTimeToLiveOutput(output *dynamodb.DescribeTimeToLiveOutput) *DescribeTimeToLiveOutput {
	status := ""
	if output.TimeToLiveDescription != nil {
		status = string(output.TimeToLiveDescription.TimeToLiveStatus)
	}
	attr := ""
	if output.TimeToLiveDescription != nil {
		attr = safeToString(output.TimeToLiveDescription.AttributeName)
	}

	return &DescribeTimeToLiveOutput{
		TimeToLiveDescription: TimeToLiveDescription{
			AttributeName:    attr,
			TimeToLiveStatus: status,
		},
	}
}

// Helpers 2

func FromSDKTableDescription(td *types.TableDescription) TableDescription {
	if td == nil {
		return TableDescription{}
	}

	cnt := 0
	if td.ItemCount != nil {
		cnt = int(*td.ItemCount)
	}

	return TableDescription{
		TableName:              safeToString(td.TableName),
		TableStatus:            string(td.TableStatus),
		ItemCount:              cnt,
		KeySchema:              FromSDKKeySchema(td.KeySchema),
		AttributeDefinitions:   FromSDKAttributeDefinitions(td.AttributeDefinitions),
		GlobalSecondaryIndexes: FromSDKGlobalSecondaryIndexDescriptions(td.GlobalSecondaryIndexes),
		LocalSecondaryIndexes:  FromSDKLocalSecondaryIndexDescriptions(td.LocalSecondaryIndexes),
		ProvisionedThroughput:  FromSDKProvisionedThroughputDescription(td.ProvisionedThroughput),
	}
}

func FromSDKGlobalSecondaryIndexDescriptions(
	gsis []types.GlobalSecondaryIndexDescription,
) []GlobalSecondaryIndexDescription {
	if len(gsis) == 0 {
		return nil
	}
	out := make([]GlobalSecondaryIndexDescription, len(gsis))
	for i, gsi := range gsis {
		out[i] = GlobalSecondaryIndexDescription{
			IndexName:   safeToString(gsi.IndexName),
			IndexStatus: string(gsi.IndexStatus),
			KeySchema:   FromSDKKeySchema(gsi.KeySchema),
			Projection:  FromSDKProjection(gsi.Projection),
			ProvisionedThroughput: ProvisionedThroughputDescription{
				ReadCapacityUnits:  int(safeToInt64(gsi.ProvisionedThroughput.ReadCapacityUnits)),
				WriteCapacityUnits: int(safeToInt64(gsi.ProvisionedThroughput.WriteCapacityUnits)),
			},
			ItemCount: int(safeToInt64(gsi.ItemCount)),
		}
	}

	return out
}

func FromSDKLocalSecondaryIndexDescriptions(
	lsis []types.LocalSecondaryIndexDescription,
) []LocalSecondaryIndexDescription {
	if len(lsis) == 0 {
		return nil
	}
	out := make([]LocalSecondaryIndexDescription, len(lsis))
	for i, lsi := range lsis {
		out[i] = LocalSecondaryIndexDescription{
			IndexName:      safeToString(lsi.IndexName),
			KeySchema:      FromSDKKeySchema(lsi.KeySchema),
			Projection:     FromSDKProjection(lsi.Projection),
			IndexSizeBytes: safeToInt64(lsi.IndexSizeBytes),
			ItemCount:      int(safeToInt64(lsi.ItemCount)),
		}
	}
	return out
}

func FromSDKProvisionedThroughputDescription(
	ptd *types.ProvisionedThroughputDescription,
) *ProvisionedThroughputDescription {
	if ptd == nil {
		return nil
	}
	return &ProvisionedThroughputDescription{
		ReadCapacityUnits:  int(safeToInt64(ptd.ReadCapacityUnits)),
		WriteCapacityUnits: int(safeToInt64(ptd.WriteCapacityUnits)),
	}
}

func safeToInt64(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

func awsInt64FromAny(v any) *int64 {
	switch val := v.(type) {
	case float64:
		i := int64(val)

		return &i
	case int:
		i := int64(val)

		return &i
	}

	return nil
}

func safeToBool(b *bool) bool {
	if b == nil {
		return false
	}

	return *b
}

func FromSDKConsumedCapacity(cc *types.ConsumedCapacity) *ConsumedCapacity {
	if cc == nil {
		return nil
	}

	return &ConsumedCapacity{
		TableName:          safeToString(cc.TableName),
		CapacityUnits:      safeToFloat(cc.CapacityUnits),
		ReadCapacityUnits:  safeToFloat(cc.ReadCapacityUnits),
		WriteCapacityUnits: safeToFloat(cc.WriteCapacityUnits),
	}
}

func FromSDKItemCollectionMetrics(icm *types.ItemCollectionMetrics) *ItemCollectionMetrics {
	if icm == nil {
		return nil
	}

	return &ItemCollectionMetrics{
		ItemCollectionKey:   FromSDKItem(icm.ItemCollectionKey),
		SizeEstimateRangeGB: icm.SizeEstimateRangeGB,
	}
}

func safeToString(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

func safeToFloat(f *float64) float64 {
	if f == nil {
		return 0
	}

	return *f
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}
