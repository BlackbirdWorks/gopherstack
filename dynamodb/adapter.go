package dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ToSDKAttributeValue converts a raw Go value (from JSON unmarshal) to an SDK AttributeValue.
func ToSDKAttributeValue(v any) (types.AttributeValue, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any for AttributeValue, got %T", v)
	}

	if len(m) != 1 {
		return nil, fmt.Errorf("expected exactly one type key in AttributeValue map, got %d", len(m))
	}

	for k, val := range m {
		switch k {
		case "S":
			s, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("expected string for S, got %T", val)
			}
			return &types.AttributeValueMemberS{Value: s}, nil
		case "N":
			s, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("expected string for N, got %T", val)
			}
			return &types.AttributeValueMemberN{Value: s}, nil
		case "B":
			s, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("expected base64 string for B, got %T", val)
			}
			return &types.AttributeValueMemberB{Value: []byte(s)}, nil
		case "BOOL":
			b, ok := val.(bool)
			if !ok {
				return nil, fmt.Errorf("expected bool for BOOL, got %T", val)
			}
			return &types.AttributeValueMemberBOOL{Value: b}, nil
		case "NULL":
			b, ok := val.(bool)
			if !ok || !b {
				return nil, fmt.Errorf("expected true for NULL, got %v", val)
			}
			return &types.AttributeValueMemberNULL{Value: true}, nil
		case "M":
			mVal, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected map for M, got %T", val)
			}
			return ToSDKMapAttribute(mVal)
		case "L":
			lVal, ok := val.([]any)
			if !ok {
				return nil, fmt.Errorf("expected slice for L, got %T", val)
			}
			return ToSDKListAttribute(lVal)
		case "SS":
			lVal, ok := val.([]any)
			if !ok {
				return nil, fmt.Errorf("expected slice for SS, got %T", val)
			}
			var ss []string
			for _, item := range lVal {
				s, ok := item.(string)
				if !ok {
					return nil, fmt.Errorf("expected string in SS, got %T", item)
				}
				ss = append(ss, s)
			}
			return &types.AttributeValueMemberSS{Value: ss}, nil
		case "NS":
			lVal, ok := val.([]any)
			if !ok {
				return nil, fmt.Errorf("expected slice for NS, got %T", val)
			}
			var ns []string
			for _, item := range lVal {
				s, ok := item.(string)
				if !ok {
					return nil, fmt.Errorf("expected string in NS, got %T", item)
				}
				ns = append(ns, s)
			}
			return &types.AttributeValueMemberNS{Value: ns}, nil
		case "BS":
			lVal, ok := val.([]any)
			if !ok {
				return nil, fmt.Errorf("expected slice for BS, got %T", val)
			}
			var bs [][]byte
			for _, item := range lVal {
				s, ok := item.(string)
				if !ok {
					return nil, fmt.Errorf("expected string in BS, got %T", item)
				}
				bs = append(bs, []byte(s))
			}
			return &types.AttributeValueMemberBS{Value: bs}, nil
		}
	}

	return nil, fmt.Errorf("unknown attribute value type")
}

func ToSDKMapAttribute(m map[string]any) (*types.AttributeValueMemberM, error) {
	out := make(map[string]types.AttributeValue)
	for k, v := range m {
		av, err := ToSDKAttributeValue(v)
		if err != nil {
			return nil, err
		}
		out[k] = av
	}
	return &types.AttributeValueMemberM{Value: out}, nil
}

func ToSDKListAttribute(l []any) (*types.AttributeValueMemberL, error) {
	out := make([]types.AttributeValue, len(l))
	for i, v := range l {
		av, err := ToSDKAttributeValue(v)
		if err != nil {
			return nil, err
		}
		out[i] = av
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
		return map[string]any{"B": string(v.Value)}
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
		bs := make([]string, len(v.Value))
		for i, val := range v.Value {
			bs[i] = string(val)
		}
		return map[string]any{"BS": bs}
	default:
		return nil
	}
}

// Helper to convert map[string]any (wire item) to map[string]types.AttributeValue
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

// Helper to convert map[string]types.AttributeValue back to map[string]any (wire item)
func FromSDKItem(item map[string]types.AttributeValue) map[string]any {
	out := make(map[string]any)
	for k, v := range item {
		out[k] = FromSDKAttributeValue(v)
	}
	return out
}

// ToSDKKeySchema converts internal KeySchema to SDK type
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
			AttributeName: safeToString(k.AttributeName),
			KeyType:       string(k.KeyType),
		}
	}
	return out
}

// ToSDKAttributeDefinitions converts internal AttributeDefinitions to SDK type
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
		out[i] = GlobalSecondaryIndex{
			IndexName:  safeToString(gsi.IndexName),
			KeySchema:  FromSDKKeySchema(gsi.KeySchema),
			Projection: FromSDKProjection(gsi.Projection),
			ProvisionedThroughput: ProvisionedThroughput{
				ReadCapacityUnits:  gsi.ProvisionedThroughput.ReadCapacityUnits,
				WriteCapacityUnits: gsi.ProvisionedThroughput.WriteCapacityUnits,
			},
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

func ToSDKGlobalSecondaryIndexDescriptions(gsis []GlobalSecondaryIndexDescription) []types.GlobalSecondaryIndexDescription {
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

func ToSDKLocalSecondaryIndexDescriptions(lsis []LocalSecondaryIndexDescription) []types.LocalSecondaryIndexDescription {
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
		vals, err := ToSDKItem(input.ExpressionAttributeValues)
		if err != nil {
			return nil, err
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
		vals, err := ToSDKItem(input.ExpressionAttributeValues)
		if err != nil {
			return nil, err
		}
		out.ExpressionAttributeValues = vals
	}

	return out, nil
}

func FromSDKDeleteItemOutput(output *dynamodb.DeleteItemOutput) *DeleteItemOutput {
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
		vals, err := ToSDKItem(input.ExpressionAttributeValues)
		if err != nil {
			return nil, err
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
		vals, err := ToSDKItem(input.ExpressionAttributeValues)
		if err != nil {
			return nil, err
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
		Limit:                    &input.Limit,
		ScanIndexForward:         input.ScanIndexForward,
	}

	if len(input.ExpressionAttributeValues) > 0 {
		vals, err := ToSDKItem(input.ExpressionAttributeValues)
		if err != nil {
			return nil, err
		}
		out.ExpressionAttributeValues = vals
	}

	if len(input.ExclusiveStartKey) > 0 {
		key, err := ToSDKItem(input.ExclusiveStartKey)
		if err != nil {
			return nil, err
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
		var convertedItems []map[string]any
		for _, item := range items {
			convertedItems = append(convertedItems, FromSDKItem(item))
		}
		responses[tableName] = convertedItems
	}

	unprocessedKeys := make(map[string]KeysAndAttributes)
	for tableName, keysAndAttrs := range output.UnprocessedKeys {
		var convertedKeys []map[string]any
		for _, k := range keysAndAttrs.Keys {
			convertedKeys = append(convertedKeys, FromSDKItem(k))
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
		var sdkRequests []types.WriteRequest
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
		var convertedRequests []WriteRequest
		for _, req := range requests {
			cnvReq := WriteRequest{}
			if req.PutRequest != nil {
				cnvReq.PutRequest = &PutRequest{Item: FromSDKItem(req.PutRequest.Item)}
			}
			if req.DeleteRequest != nil {
				cnvReq.DeleteRequest = &DeleteRequest{Key: FromSDKItem(req.DeleteRequest.Key)}
			}
			convertedRequests = append(convertedRequests, cnvReq)
		}
		unprocessedItems[tableName] = convertedRequests
	}

	var consumedCapacity []ConsumedCapacity
	for _, cc := range output.ConsumedCapacity {
		consumedCapacity = append(consumedCapacity, *FromSDKConsumedCapacity(&cc))
	}

	return &BatchWriteItemOutput{
		UnprocessedItems: unprocessedItems,
		ConsumedCapacity: consumedCapacity,
	}
}

// --- Transact Adapters ---

func ToSDKTransactWriteItemsInput(input *TransactWriteItemsInput) (*dynamodb.TransactWriteItemsInput, error) {
	var items []types.TransactWriteItem
	for _, item := range input.TransactItems {
		twi := types.TransactWriteItem{}
		if item.Put != nil {
			sdkPut, err := ToSDKPutItemInput(item.Put)
			if err != nil {
				return nil, err
			}
			// Map dynamodb.PutItemInput to types.Put
			twi.Put = &types.Put{
				Item:                                sdkPut.Item,
				TableName:                           sdkPut.TableName,
				ConditionExpression:                 sdkPut.ConditionExpression,
				ExpressionAttributeNames:            sdkPut.ExpressionAttributeNames,
				ExpressionAttributeValues:           sdkPut.ExpressionAttributeValues,
				ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailure(item.Put.ReturnValues), // Approximate mapping
			}
		}
		if item.Delete != nil {
			sdkDel, err := ToSDKDeleteItemInput(item.Delete)
			if err != nil {
				return nil, err
			}
			twi.Delete = &types.Delete{
				Key:                       sdkDel.Key,
				TableName:                 sdkDel.TableName,
				ConditionExpression:       sdkDel.ConditionExpression,
				ExpressionAttributeNames:  sdkDel.ExpressionAttributeNames,
				ExpressionAttributeValues: sdkDel.ExpressionAttributeValues,
			}
		}
		if item.Update != nil {
			sdkUpd, err := ToSDKUpdateItemInput(item.Update)
			if err != nil {
				return nil, err
			}
			twi.Update = &types.Update{
				Key:                       sdkUpd.Key,
				TableName:                 sdkUpd.TableName,
				UpdateExpression:          sdkUpd.UpdateExpression,
				ConditionExpression:       sdkUpd.ConditionExpression,
				ExpressionAttributeNames:  sdkUpd.ExpressionAttributeNames,
				ExpressionAttributeValues: sdkUpd.ExpressionAttributeValues,
			}
		}
		if item.ConditionCheck != nil {
			key, err := ToSDKItem(item.ConditionCheck.Key)
			if err != nil {
				return nil, err
			}
			twi.ConditionCheck = &types.ConditionCheck{
				Key:                      key,
				TableName:                &item.ConditionCheck.TableName,
				ConditionExpression:      &item.ConditionCheck.ConditionExpression,
				ExpressionAttributeNames: item.ConditionCheck.ExpressionAttributeNames,
			}
			if len(item.ConditionCheck.ExpressionAttributeValues) > 0 {
				vals, err := ToSDKItem(item.ConditionCheck.ExpressionAttributeValues)
				if err != nil {
					return nil, err
				}
				twi.ConditionCheck.ExpressionAttributeValues = vals
			}
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

func FromSDKTransactWriteItemsOutput(output *dynamodb.TransactWriteItemsOutput) *TransactWriteItemsOutput {
	return &TransactWriteItemsOutput{
		// Metrics
	}
}

func ToSDKTransactGetItemsInput(input *TransactGetItemsInput) (*dynamodb.TransactGetItemsInput, error) {
	var items []types.TransactGetItem
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
	var responses []ItemResponse
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
	l := int32(input.Limit)
	return &dynamodb.ListTablesInput{
		Limit: &l,
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
		TableName:   safeToString(td.TableName),
		TableStatus: string(td.TableStatus),
		ItemCount:   cnt,
		// ... expand as needed for other fields
	}
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
