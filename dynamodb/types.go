package dynamodb

// --- Request/Response Structs ---

type CreateTableInput struct {
	TableName              string                 `json:"TableName"`
	KeySchema              []KeySchemaElement     `json:"KeySchema"`
	AttributeDefinitions   []AttributeDefinition  `json:"AttributeDefinitions"`
	GlobalSecondaryIndexes []GlobalSecondaryIndex `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []LocalSecondaryIndex  `json:"LocalSecondaryIndexes,omitempty"`
	ProvisionedThroughput  interface{}            `json:"ProvisionedThroughput"`
}

type CreateTableOutput struct {
	TableDescription TableDescription `json:"TableDescription"`
}

type DeleteTableInput struct {
	TableName string `json:"TableName"`
}

type DeleteTableOutput struct {
	TableDescription TableDescription `json:"TableDescription"`
}

type DescribeTableInput struct {
	TableName string `json:"TableName"`
}

type DescribeTableOutput struct {
	Table TableDescription `json:"Table"`
}

type TableDescription struct {
	TableName              string                            `json:"TableName"`
	TableStatus            string                            `json:"TableStatus"`
	KeySchema              []KeySchemaElement                `json:"KeySchema"`
	AttributeDefinitions   []AttributeDefinition             `json:"AttributeDefinitions"`
	GlobalSecondaryIndexes []GlobalSecondaryIndexDescription `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []LocalSecondaryIndexDescription  `json:"LocalSecondaryIndexes,omitempty"`
	ItemCount              int                               `json:"ItemCount"`
	ProvisionedThroughput  *ProvisionedThroughputDescription `json:"ProvisionedThroughput,omitempty"`
}

type ProvisionedThroughputDescription struct {
	ReadCapacityUnits  int `json:"ReadCapacityUnits"`
	WriteCapacityUnits int `json:"WriteCapacityUnits"`
}

type GlobalSecondaryIndex struct {
	IndexName             string                `json:"IndexName"`
	KeySchema             []KeySchemaElement    `json:"KeySchema"`
	Projection            Projection            `json:"Projection"`
	ProvisionedThroughput ProvisionedThroughput `json:"ProvisionedThroughput"`
}

type GlobalSecondaryIndexDescription struct {
	IndexName             string                           `json:"IndexName"`
	KeySchema             []KeySchemaElement               `json:"KeySchema"`
	Projection            Projection                       `json:"Projection"`
	ProvisionedThroughput ProvisionedThroughputDescription `json:"ProvisionedThroughput"`
	IndexStatus           string                           `json:"IndexStatus"`
	ItemCount             int                              `json:"ItemCount"`
}

type LocalSecondaryIndex struct {
	IndexName  string             `json:"IndexName"`
	KeySchema  []KeySchemaElement `json:"KeySchema"`
	Projection Projection         `json:"Projection"`
}

type LocalSecondaryIndexDescription struct {
	IndexName      string             `json:"IndexName"`
	KeySchema      []KeySchemaElement `json:"KeySchema"`
	Projection     Projection         `json:"Projection"`
	IndexSizeBytes int64              `json:"IndexSizeBytes"`
	ItemCount      int                `json:"ItemCount"`
}

type Projection struct {
	ProjectionType   string   `json:"ProjectionType"`
	NonKeyAttributes []string `json:"NonKeyAttributes,omitempty"`
}

type ProvisionedThroughput struct {
	ReadCapacityUnits  *int64 `json:"ReadCapacityUnits"`
	WriteCapacityUnits *int64 `json:"WriteCapacityUnits"`
}

type ListTablesInput struct {
	Limit int `json:"Limit"`
}

type ListTablesOutput struct {
	TableNames []string `json:"TableNames"`
}

type PutItemInput struct {
	TableName                   string                 `json:"TableName"`
	Item                        map[string]interface{} `json:"Item"`
	ConditionExpression         string                 `json:"ConditionExpression,omitempty"`
	ExpressionAttributeNames    map[string]string      `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues   map[string]interface{} `json:"ExpressionAttributeValues,omitempty"`
	ReturnValues                string                 `json:"ReturnValues,omitempty"`
	ReturnConsumedCapacity      string                 `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string                 `json:"ReturnItemCollectionMetrics,omitempty"`
}

type PutItemOutput struct {
	Attributes            map[string]interface{} `json:"Attributes,omitempty"`
	ConsumedCapacity      *ConsumedCapacity      `json:"ConsumedCapacity,omitempty"`
	ItemCollectionMetrics *ItemCollectionMetrics `json:"ItemCollectionMetrics,omitempty"`
}

type UpdateItemInput struct {
	TableName                   string                 `json:"TableName"`
	Key                         map[string]interface{} `json:"Key"`
	UpdateExpression            string                 `json:"UpdateExpression,omitempty"`
	ConditionExpression         string                 `json:"ConditionExpression,omitempty"`
	ExpressionAttributeNames    map[string]string      `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues   map[string]interface{} `json:"ExpressionAttributeValues,omitempty"`
	ReturnValues                string                 `json:"ReturnValues,omitempty"`
	ReturnConsumedCapacity      string                 `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string                 `json:"ReturnItemCollectionMetrics,omitempty"`
}

type UpdateItemOutput struct {
	Attributes            map[string]interface{} `json:"Attributes,omitempty"`
	ConsumedCapacity      *ConsumedCapacity      `json:"ConsumedCapacity,omitempty"`
	ItemCollectionMetrics *ItemCollectionMetrics `json:"ItemCollectionMetrics,omitempty"`
}

type QueryInput struct {
	TableName                 string                 `json:"TableName"`
	IndexName                 string                 `json:"IndexName,omitempty"`
	KeyConditionExpression    string                 `json:"KeyConditionExpression"`
	FilterExpression          string                 `json:"FilterExpression,omitempty"`
	ProjectionExpression      string                 `json:"ProjectionExpression,omitempty"`
	ExpressionAttributeNames  map[string]string      `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]interface{} `json:"ExpressionAttributeValues,omitempty"`
	ScanIndexForward          *bool                  `json:"ScanIndexForward,omitempty"`
	Limit                     int32                  `json:"Limit,omitempty"`
	ExclusiveStartKey         map[string]interface{} `json:"ExclusiveStartKey,omitempty"`
	ReturnConsumedCapacity    string                 `json:"ReturnConsumedCapacity,omitempty"`
}

type QueryOutput struct {
	Items            []map[string]interface{} `json:"Items,omitempty"`
	Count            int                      `json:"Count"`
	ScannedCount     int                      `json:"ScannedCount"`
	LastEvaluatedKey map[string]interface{}   `json:"LastEvaluatedKey,omitempty"`
	ConsumedCapacity *ConsumedCapacity        `json:"ConsumedCapacity,omitempty"`
}

type BatchGetItemInput struct {
	RequestItems map[string]KeysAndAttributes `json:"RequestItems"`
}

type KeysAndAttributes struct {
	Keys                     []map[string]interface{} `json:"Keys"`
	AttributesToGet          []string                 `json:"AttributesToGet,omitempty"` // Legacy
	ProjectionExpression     string                   `json:"ProjectionExpression,omitempty"`
	ExpressionAttributeNames map[string]string        `json:"ExpressionAttributeNames,omitempty"`
	ConsistentRead           *bool                    `json:"ConsistentRead,omitempty"`
}

type BatchGetItemOutput struct {
	Responses       map[string][]map[string]interface{} `json:"Responses,omitempty"`
	UnprocessedKeys map[string]KeysAndAttributes        `json:"UnprocessedKeys,omitempty"`
}

type BatchWriteItemInput struct {
	RequestItems map[string][]WriteRequest `json:"RequestItems"`
}

type WriteRequest struct {
	PutRequest    *PutRequest    `json:"PutRequest,omitempty"`
	DeleteRequest *DeleteRequest `json:"DeleteRequest,omitempty"`
}

type PutRequest struct {
	Item map[string]interface{} `json:"Item"`
}

type DeleteRequest struct {
	Key map[string]interface{} `json:"Key"`
}

type BatchWriteItemOutput struct {
	UnprocessedItems      map[string][]WriteRequest `json:"UnprocessedItems,omitempty"`
	ItemCollectionMetrics map[string][]interface{}  `json:"ItemCollectionMetrics,omitempty"` // Simplified for now
	ConsumedCapacity      []ConsumedCapacity        `json:"ConsumedCapacity,omitempty"`
}

type ConsumedCapacity struct {
	TableName          string  `json:"TableName,omitempty"`
	CapacityUnits      float64 `json:"CapacityUnits,omitempty"`
	ReadCapacityUnits  float64 `json:"ReadCapacityUnits,omitempty"`
	WriteCapacityUnits float64 `json:"WriteCapacityUnits,omitempty"`
}

type ItemCollectionMetrics struct {
	ItemCollectionKey   map[string]interface{} `json:"ItemCollectionKey,omitempty"`
	SizeEstimateRangeGB []float64              `json:"SizeEstimateRangeGB,omitempty"`
}

type GetItemInput struct {
	TableName                string                 `json:"TableName"`
	Key                      map[string]interface{} `json:"Key"`
	ProjectionExpression     string                 `json:"ProjectionExpression,omitempty"`
	ExpressionAttributeNames map[string]string      `json:"ExpressionAttributeNames,omitempty"`
}

type GetItemOutput struct {
	Item map[string]interface{} `json:"Item,omitempty"`
}

type DeleteItemInput struct {
	TableName                 string                 `json:"TableName"`
	Key                       map[string]interface{} `json:"Key"`
	ConditionExpression       string                 `json:"ConditionExpression,omitempty"`
	ExpressionAttributeNames  map[string]string      `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]interface{} `json:"ExpressionAttributeValues,omitempty"`
}

type DeleteItemOutput struct{}

type ScanInput struct {
	TableName                 string                 `json:"TableName"`
	IndexName                 string                 `json:"IndexName,omitempty"`
	FilterExpression          string                 `json:"FilterExpression,omitempty"`
	ProjectionExpression      string                 `json:"ProjectionExpression,omitempty"`
	ExpressionAttributeNames  map[string]string      `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]interface{} `json:"ExpressionAttributeValues,omitempty"`
}

type ScanOutput struct {
	Items        []map[string]interface{} `json:"Items"`
	Count        int                      `json:"Count"`
	ScannedCount int                      `json:"ScannedCount"`
}
