package models

import (
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// --- CRUD Adapters ---

func ToSDKPutItemInput(input *PutItemInput) (*dynamodb.PutItemInput, error) {
	item, err := ToSDKItem(input.Item)
	if err != nil {
		return nil, err
	}

	out := &dynamodb.PutItemInput{
		TableName:                &input.TableName,
		Item:                     item,
		ConditionExpression:      ptrconv.NilIfEmpty(input.ConditionExpression),
		ExpressionAttributeNames: input.ExpressionAttributeNames,
		ReturnValues:             types.ReturnValue(input.ReturnValues),
		ReturnConsumedCapacity:   types.ReturnConsumedCapacity(input.ReturnConsumedCapacity),
		ReturnItemCollectionMetrics: types.ReturnItemCollectionMetrics(
			input.ReturnItemCollectionMetrics,
		),
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
		ProjectionExpression:     ptrconv.NilIfEmpty(input.ProjectionExpression),
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
		ConditionExpression:      ptrconv.NilIfEmpty(input.ConditionExpression),
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
		TableName:                &input.TableName,
		Key:                      key,
		UpdateExpression:         ptrconv.NilIfEmpty(input.UpdateExpression),
		ConditionExpression:      ptrconv.NilIfEmpty(input.ConditionExpression),
		ExpressionAttributeNames: input.ExpressionAttributeNames,
		ReturnValues:             types.ReturnValue(input.ReturnValues),
		ReturnConsumedCapacity:   types.ReturnConsumedCapacity(input.ReturnConsumedCapacity),
		ReturnItemCollectionMetrics: types.ReturnItemCollectionMetrics(
			input.ReturnItemCollectionMetrics,
		),
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
		IndexName:                ptrconv.NilIfEmpty(input.IndexName),
		FilterExpression:         ptrconv.NilIfEmpty(input.FilterExpression),
		ProjectionExpression:     ptrconv.NilIfEmpty(input.ProjectionExpression),
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
		IndexName:                ptrconv.NilIfEmpty(input.IndexName),
		KeyConditionExpression:   ptrconv.NilIfEmpty(input.KeyConditionExpression),
		FilterExpression:         ptrconv.NilIfEmpty(input.FilterExpression),
		ProjectionExpression:     ptrconv.NilIfEmpty(input.ProjectionExpression),
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
			ProjectionExpression:     ptrconv.NilIfEmpty(keysAndAttrs.ProjectionExpression),
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
			ProjectionExpression:     ptrconv.String(keysAndAttrs.ProjectionExpression),
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

	metrics := make(map[string][]any)
	for tableName, sdkMetrics := range output.ItemCollectionMetrics {
		cnvMetrics := make([]any, len(sdkMetrics))
		for i, m := range sdkMetrics {
			cnvMetrics[i] = FromSDKItemCollectionMetrics(&m)
		}
		metrics[tableName] = cnvMetrics
	}

	cc := make([]ConsumedCapacity, len(output.ConsumedCapacity))
	for i, c := range output.ConsumedCapacity {
		cc[i] = *FromSDKConsumedCapacity(&c)
	}

	return &BatchWriteItemOutput{
		UnprocessedItems:      unprocessedItems,
		ConsumedCapacity:      cc,
		ItemCollectionMetrics: metrics,
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

func ToSDKTransactWriteItemsInput(
	input *TransactWriteItemsInput,
) (*dynamodb.TransactWriteItemsInput, error) {
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
		ClientRequestToken:     ptrconv.NilIfEmpty(input.ClientRequestToken),
		ReturnConsumedCapacity: types.ReturnConsumedCapacity(input.ReturnConsumedCapacity),
		// ReturnItemCollectionMetrics
	}, nil
}

func FromSDKTransactWriteItemsOutput(
	output *dynamodb.TransactWriteItemsOutput,
) *TransactWriteItemsOutput {
	metrics := make(map[string][]ItemCollectionMetrics)
	for tableName, sdkMetrics := range output.ItemCollectionMetrics {
		cnvMetrics := make([]ItemCollectionMetrics, len(sdkMetrics))
		for i, m := range sdkMetrics {
			cnvMetrics[i] = *FromSDKItemCollectionMetrics(&m)
		}
		metrics[tableName] = cnvMetrics
	}

	cc := make([]ConsumedCapacity, len(output.ConsumedCapacity))
	for i, c := range output.ConsumedCapacity {
		cc[i] = *FromSDKConsumedCapacity(&c)
	}

	return &TransactWriteItemsOutput{
		ItemCollectionMetrics: metrics,
		ConsumedCapacity:      cc,
	}
}

func ToSDKTransactGetItemsInput(
	input *TransactGetItemsInput,
) (*dynamodb.TransactGetItemsInput, error) {
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

func FromSDKTransactGetItemsOutput(
	output *dynamodb.TransactGetItemsOutput,
) *TransactGetItemsOutput {
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
