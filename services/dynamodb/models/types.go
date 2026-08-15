package models

import "time"

// --- Constants ---

const (
	KeyTypeHash            = "HASH"
	KeyTypeRange           = "RANGE"
	ReturnValuesAllOld     = "ALL_OLD"
	ReturnValuesAllNew     = "ALL_NEW"
	ReturnValuesUpdatedOld = "UPDATED_OLD"
	ReturnValuesUpdatedNew = "UPDATED_NEW"
	TableStatusActive      = "ACTIVE"

	DefaultReadCapacity  = 5
	DefaultWriteCapacity = 5
	ConsumedReadUnit     = 0.5
	ConsumedWriteUnit    = 0.5
)

// --- Shared Schema Types ---

// KeySchemaElement represents a key attribute in a table or index schema.
type KeySchemaElement struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"` // "HASH" or "RANGE"
}

// AttributeDefinition defines the name and type of an attribute.
type AttributeDefinition struct {
	AttributeName string `json:"AttributeName"`
	AttributeType string `json:"AttributeType"` // "S", "N", "B"
}

// --- Table Operations ---

type CreateTableInput struct {
	ProvisionedThroughput     any                    `json:"ProvisionedThroughput"`
	StreamSpecification       any                    `json:"StreamSpecification,omitempty"`
	SSESpecification          *SSESpecification      `json:"SSESpecification,omitempty"`
	OnDemandThroughput        *OnDemandThroughput    `json:"OnDemandThroughput,omitempty"`
	DeletionProtectionEnabled *bool                  `json:"DeletionProtectionEnabled,omitempty"`
	TableName                 string                 `json:"TableName"`
	BillingMode               string                 `json:"BillingMode,omitempty"`
	TableClass                string                 `json:"TableClass,omitempty"`
	KeySchema                 []KeySchemaElement     `json:"KeySchema"`
	AttributeDefinitions      []AttributeDefinition  `json:"AttributeDefinitions"`
	GlobalSecondaryIndexes    []GlobalSecondaryIndex `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes     []LocalSecondaryIndex  `json:"LocalSecondaryIndexes,omitempty"`
	Tags                      []Tag                  `json:"Tags,omitempty"`
}

// SSESpecification is the wire format for a table's server-side encryption
// settings, mirrored on CreateTableInput and UpdateTableInput.
type SSESpecification struct {
	Enabled        *bool  `json:"Enabled,omitempty"`
	KMSMasterKeyID string `json:"KMSMasterKeyId,omitempty"`
	SSEType        string `json:"SSEType,omitempty"`
}

// OnDemandThroughput is the wire format for a table's on-demand max
// read/write request units, mirrored on CreateTableInput and UpdateTableInput.
type OnDemandThroughput struct {
	MaxReadRequestUnits  *int64 `json:"MaxReadRequestUnits,omitempty"`
	MaxWriteRequestUnits *int64 `json:"MaxWriteRequestUnits,omitempty"`
}

// TableClassSummaryDescription is the wire format for TableDescription's
// TableClassSummary member.
type TableClassSummaryDescription struct {
	TableClass string `json:"TableClass,omitempty"`
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
	ProvisionedThroughput     *ProvisionedThroughputDescription `json:"ProvisionedThroughput,omitempty"`
	StreamSpecification       *StreamSpecificationInput         `json:"StreamSpecification,omitempty"`
	BillingModeSummary        *BillingModeSummaryDescription    `json:"BillingModeSummary,omitempty"`
	SSEDescription            *SSEDescription                   `json:"SSEDescription,omitempty"`
	OnDemandThroughput        *OnDemandThroughput               `json:"OnDemandThroughput,omitempty"`
	TableClassSummary         *TableClassSummaryDescription     `json:"TableClassSummary,omitempty"`
	TableName                 string                            `json:"TableName"`
	TableStatus               string                            `json:"TableStatus"`
	TableArn                  string                            `json:"TableArn,omitempty"`
	TableID                   string                            `json:"TableId,omitempty"`
	LatestStreamArn           string                            `json:"LatestStreamArn,omitempty"`
	LatestStreamLabel         string                            `json:"LatestStreamLabel,omitempty"`
	GlobalTableVersion        string                            `json:"GlobalTableVersion,omitempty"`
	KeySchema                 []KeySchemaElement                `json:"KeySchema"`
	AttributeDefinitions      []AttributeDefinition             `json:"AttributeDefinitions"`
	GlobalSecondaryIndexes    []GlobalSecondaryIndexDescription `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes     []LocalSecondaryIndexDescription  `json:"LocalSecondaryIndexes,omitempty"`
	Replicas                  []ReplicaDescription              `json:"Replicas,omitempty"`
	CreationDateTime          float64                           `json:"CreationDateTime,omitempty"`
	TableSizeBytes            int64                             `json:"TableSizeBytes"`
	DeletionProtectionEnabled bool                              `json:"DeletionProtectionEnabled,omitempty"`
	ItemCount                 int                               `json:"ItemCount"`
}

type SSEDescription struct {
	Status          string `json:"Status,omitempty"`
	SSEType         string `json:"SSEType,omitempty"`
	KMSMasterKeyArn string `json:"KMSMasterKeyArn,omitempty"`
}

// BillingModeSummaryDescription describes the billing mode of a DynamoDB table.
type BillingModeSummaryDescription struct {
	BillingMode string `json:"BillingMode,omitempty"`
}

type ProvisionedThroughputDescription struct {
	ReadCapacityUnits  int `json:"ReadCapacityUnits"`
	WriteCapacityUnits int `json:"WriteCapacityUnits"`
}

type GlobalSecondaryIndex struct {
	ProvisionedThroughput ProvisionedThroughput `json:"ProvisionedThroughput"`
	IndexStatusTimer      *time.Timer           `json:"-"`
	IndexName             string                `json:"IndexName"`
	IndexStatus           string                `json:"IndexStatus,omitempty"`
	Projection            Projection            `json:"Projection"`
	KeySchema             []KeySchemaElement    `json:"KeySchema"`
}

type GlobalSecondaryIndexDescription struct {
	IndexName             string                           `json:"IndexName"`
	IndexArn              string                           `json:"IndexArn"`
	IndexStatus           string                           `json:"IndexStatus"`
	Projection            Projection                       `json:"Projection"`
	KeySchema             []KeySchemaElement               `json:"KeySchema"`
	ProvisionedThroughput ProvisionedThroughputDescription `json:"ProvisionedThroughput"`
	ItemCount             int                              `json:"ItemCount"`
	IndexSizeBytes        int64                            `json:"IndexSizeBytes"`
	Backfilling           bool                             `json:"Backfilling,omitempty"`
}

type LocalSecondaryIndex struct {
	IndexName  string             `json:"IndexName"`
	KeySchema  []KeySchemaElement `json:"KeySchema"`
	Projection Projection         `json:"Projection"`
}

type LocalSecondaryIndexDescription struct {
	IndexName      string             `json:"IndexName"`
	IndexArn       string             `json:"IndexArn"`
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

// UpdateTableInput is the wire-format for a DynamoDB UpdateTable request.
type UpdateTableInput struct {
	ProvisionedThroughput       *ProvisionedThroughput       `json:"ProvisionedThroughput,omitempty"`
	StreamSpecification         *StreamSpecificationInput    `json:"StreamSpecification,omitempty"`
	SSESpecification            *SSESpecification            `json:"SSESpecification,omitempty"`
	DeletionProtectionEnabled   *bool                        `json:"DeletionProtectionEnabled,omitempty"`
	TableName                   string                       `json:"TableName"`
	BillingMode                 string                       `json:"BillingMode,omitempty"`
	TableClass                  string                       `json:"TableClass,omitempty"`
	AttributeDefinitions        []AttributeDefinition        `json:"AttributeDefinitions,omitempty"`
	GlobalSecondaryIndexUpdates []GlobalSecondaryIndexUpdate `json:"GlobalSecondaryIndexUpdates,omitempty"`
	ReplicaUpdates              []ReplicaUpdate              `json:"ReplicaUpdates,omitempty"`
}

// ReplicaUpdate describes a create, update, or delete action for a Global Tables v2 replica.
type ReplicaUpdate struct {
	Create *CreateReplicationGroupMemberAction `json:"Create,omitempty"`
	Update *UpdateReplicationGroupMemberAction `json:"Update,omitempty"`
	Delete *DeleteReplicationGroupMemberAction `json:"Delete,omitempty"`
}

// CreateReplicationGroupMemberAction specifies parameters for creating a new replica.
type CreateReplicationGroupMemberAction struct {
	RegionName string `json:"RegionName"`
}

// UpdateReplicationGroupMemberAction specifies per-replica setting overrides.
type UpdateReplicationGroupMemberAction struct {
	ProvisionedReadCapacityUnits *int64 `json:"ProvisionedReadCapacityUnits,omitempty"`
	RegionName                   string `json:"RegionName"`
	TableClassOverride           string `json:"TableClassOverride,omitempty"`
}

// DeleteReplicationGroupMemberAction specifies the region of the replica to delete.
type DeleteReplicationGroupMemberAction struct {
	RegionName string `json:"RegionName"`
}

// ReplicaGSIOverride stores per-replica read-capacity override for one GSI.
type ReplicaGSIOverride struct {
	ProvisionedReadCapacity *int64 `json:"ProvisionedReadCapacity,omitempty"`
	IndexName               string `json:"IndexName"`
}

type ReplicaDescription struct {
	ProvisionedReadCapacityUnits *int64               `json:"ProvisionedReadCapacityUnits,omitempty"`
	RegionName                   string               `json:"RegionName,omitempty"`
	ReplicaStatus                string               `json:"ReplicaStatus,omitempty"`
	TableClassOverride           string               `json:"TableClassOverride,omitempty"`
	GlobalSecondaryIndexes       []ReplicaGSIOverride `json:"GlobalSecondaryIndexes,omitempty"`
}

// GlobalSecondaryIndexUpdate describes a single GSI change.
type GlobalSecondaryIndexUpdate struct {
	Create *CreateGlobalSecondaryIndexAction `json:"Create,omitempty"`
	Update *UpdateGlobalSecondaryIndexAction `json:"Update,omitempty"`
	Delete *DeleteGlobalSecondaryIndexAction `json:"Delete,omitempty"`
}

// CreateGlobalSecondaryIndexAction adds a new GSI.
type CreateGlobalSecondaryIndexAction struct {
	ProvisionedThroughput *ProvisionedThroughput `json:"ProvisionedThroughput,omitempty"`
	IndexName             string                 `json:"IndexName"`
	Projection            Projection             `json:"Projection"`
	KeySchema             []KeySchemaElement     `json:"KeySchema"`
}

// UpdateGlobalSecondaryIndexAction updates the throughput of an existing GSI.
type UpdateGlobalSecondaryIndexAction struct {
	ProvisionedThroughput ProvisionedThroughput `json:"ProvisionedThroughput"`
	IndexName             string                `json:"IndexName"`
}

// DeleteGlobalSecondaryIndexAction removes an existing GSI.
type DeleteGlobalSecondaryIndexAction struct {
	IndexName string `json:"IndexName"`
}

// StreamSpecificationInput is the stream spec used in UpdateTable requests.
type StreamSpecificationInput struct {
	StreamViewType string `json:"StreamViewType,omitempty"`
	StreamEnabled  bool   `json:"StreamEnabled"`
}

// UpdateTableOutput is the wire-format for a DynamoDB UpdateTable response.
type UpdateTableOutput struct {
	TableDescription TableDescription `json:"TableDescription"`
}

type ListTablesInput struct {
	ExclusiveStartTableName string `json:"ExclusiveStartTableName,omitempty"`
	Limit                   int    `json:"Limit"`
}

type ListTablesOutput struct {
	LastEvaluatedTableName string   `json:"LastEvaluatedTableName,omitempty"`
	TableNames             []string `json:"TableNames"`
}

// --- Item Operations ---

// LegacyExpected is the wire format for one entry of the legacy
// Expected parameter (pre-2013 conditional-write API, predates
// ConditionExpression). AWS SDK: types.ExpectedAttributeValue; wire keys
// confirmed against aws-sdk-go-v2/service/dynamodb@v1.63.1/serializers.go
// (awsAwsjson10_serializeDocumentExpectedAttributeValue).
type LegacyExpected struct {
	Value              any    `json:"Value,omitempty"`
	Exists             *bool  `json:"Exists,omitempty"`
	ComparisonOperator string `json:"ComparisonOperator,omitempty"`
	AttributeValueList []any  `json:"AttributeValueList,omitempty"`
}

// LegacyUpdate is the wire format for one entry of the legacy
// AttributeUpdates parameter (pre-2013 UpdateItem API, predates
// UpdateExpression). AWS SDK: types.AttributeValueUpdate; wire keys confirmed
// against serializers.go (awsAwsjson10_serializeDocumentAttributeValueUpdate).
type LegacyUpdate struct {
	Value  any    `json:"Value,omitempty"`
	Action string `json:"Action,omitempty"`
}

// LegacyCondition is the wire format for one entry of the legacy
// KeyConditions/QueryFilter/ScanFilter parameters (pre-2013 Query/Scan API,
// predates KeyConditionExpression/FilterExpression). AWS SDK: types.Condition;
// wire keys confirmed against serializers.go
// (awsAwsjson10_serializeDocumentCondition).
type LegacyCondition struct {
	ComparisonOperator string `json:"ComparisonOperator,omitempty"`
	AttributeValueList []any  `json:"AttributeValueList,omitempty"`
}

type PutItemInput struct {
	Item                                map[string]any            `json:"Item"`
	ExpressionAttributeNames            map[string]string         `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues           map[string]any            `json:"ExpressionAttributeValues,omitempty"`
	Expected                            map[string]LegacyExpected `json:"Expected,omitempty"`
	TableName                           string                    `json:"TableName"`
	ConditionExpression                 string                    `json:"ConditionExpression,omitempty"`
	ReturnValues                        string                    `json:"ReturnValues,omitempty"`
	ReturnConsumedCapacity              string                    `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics         string                    `json:"ReturnItemCollectionMetrics,omitempty"`
	ReturnValuesOnConditionCheckFailure string                    `json:"ReturnValuesOnConditionCheckFailure,omitempty"`
	ConditionalOperator                 string                    `json:"ConditionalOperator,omitempty"`
}

type PutItemOutput struct {
	Attributes            map[string]any         `json:"Attributes,omitempty"`
	ConsumedCapacity      *ConsumedCapacity      `json:"ConsumedCapacity,omitempty"`
	ItemCollectionMetrics *ItemCollectionMetrics `json:"ItemCollectionMetrics,omitempty"`
}

type UpdateItemInput struct {
	ExpressionAttributeValues           map[string]any            `json:"ExpressionAttributeValues,omitempty"`
	Key                                 map[string]any            `json:"Key"`
	Expected                            map[string]LegacyExpected `json:"Expected,omitempty"`
	AttributeUpdates                    map[string]LegacyUpdate   `json:"AttributeUpdates,omitempty"`
	ExpressionAttributeNames            map[string]string         `json:"ExpressionAttributeNames,omitempty"`
	ReturnValues                        string                    `json:"ReturnValues,omitempty"`
	TableName                           string                    `json:"TableName"`
	ReturnConsumedCapacity              string                    `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics         string                    `json:"ReturnItemCollectionMetrics,omitempty"`
	ReturnValuesOnConditionCheckFailure string                    `json:"ReturnValuesOnConditionCheckFailure,omitempty"`
	ConditionalOperator                 string                    `json:"ConditionalOperator,omitempty"`
	ConditionExpression                 string                    `json:"ConditionExpression,omitempty"`
	UpdateExpression                    string                    `json:"UpdateExpression,omitempty"`
}

type UpdateItemOutput struct {
	Attributes            map[string]any         `json:"Attributes,omitempty"`
	ConsumedCapacity      *ConsumedCapacity      `json:"ConsumedCapacity,omitempty"`
	ItemCollectionMetrics *ItemCollectionMetrics `json:"ItemCollectionMetrics,omitempty"`
}

type GetItemInput struct {
	ConsistentRead           *bool             `json:"ConsistentRead,omitempty"`
	Key                      map[string]any    `json:"Key"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames,omitempty"`
	TableName                string            `json:"TableName"`
	ProjectionExpression     string            `json:"ProjectionExpression,omitempty"`
	ReturnConsumedCapacity   string            `json:"ReturnConsumedCapacity,omitempty"`
	AttributesToGet          []string          `json:"AttributesToGet,omitempty"`
}

type GetItemOutput struct {
	ConsumedCapacity *ConsumedCapacity `json:"ConsumedCapacity,omitempty"`
	Item             map[string]any    `json:"Item,omitempty"`
}

type DeleteItemInput struct {
	Key                                 map[string]any            `json:"Key"`
	ExpressionAttributeNames            map[string]string         `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues           map[string]any            `json:"ExpressionAttributeValues,omitempty"`
	Expected                            map[string]LegacyExpected `json:"Expected,omitempty"`
	TableName                           string                    `json:"TableName"`
	ConditionExpression                 string                    `json:"ConditionExpression,omitempty"`
	ReturnValues                        string                    `json:"ReturnValues,omitempty"`
	ReturnConsumedCapacity              string                    `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics         string                    `json:"ReturnItemCollectionMetrics,omitempty"`
	ReturnValuesOnConditionCheckFailure string                    `json:"ReturnValuesOnConditionCheckFailure,omitempty"`
	ConditionalOperator                 string                    `json:"ConditionalOperator,omitempty"`
}

type DeleteItemOutput struct {
	Attributes            map[string]any         `json:"Attributes,omitempty"`
	ConsumedCapacity      *ConsumedCapacity      `json:"ConsumedCapacity,omitempty"`
	ItemCollectionMetrics *ItemCollectionMetrics `json:"ItemCollectionMetrics,omitempty"`
}

// --- Query & Scan ---

type StreamRecord struct {
	OldImage                    map[string]any `json:"oldImage,omitempty"`
	NewImage                    map[string]any `json:"newImage,omitempty"`
	Keys                        map[string]any `json:"keys,omitempty"`
	EventID                     string         `json:"eventID"`
	EventName                   string         `json:"eventName"`
	SequenceNumber              string         `json:"sequenceNumber"`
	StreamViewType              string         `json:"streamViewType,omitempty"`
	UserIdentityPrincipalID     string         `json:"userIdentityPrincipalId,omitempty"`
	UserIdentityType            string         `json:"userIdentityType,omitempty"`
	ApproximateCreationDateTime int64          `json:"approximateCreationDateTime"`
	ExpireAt                    int64          `json:"expireAt,omitempty"`
	SizeBytes                   int64          `json:"sizeBytes,omitempty"`
}

type QueryInput struct {
	ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames,omitempty"`
	ExclusiveStartKey         map[string]any             `json:"ExclusiveStartKey,omitempty"`
	ScanIndexForward          *bool                      `json:"ScanIndexForward,omitempty"`
	ExpressionAttributeValues map[string]any             `json:"ExpressionAttributeValues,omitempty"`
	KeyConditions             map[string]LegacyCondition `json:"KeyConditions,omitempty"`
	QueryFilter               map[string]LegacyCondition `json:"QueryFilter,omitempty"`
	KeyConditionExpression    string                     `json:"KeyConditionExpression"`
	ProjectionExpression      string                     `json:"ProjectionExpression,omitempty"`
	ReturnConsumedCapacity    string                     `json:"ReturnConsumedCapacity,omitempty"`
	Select                    string                     `json:"Select,omitempty"`
	FilterExpression          string                     `json:"FilterExpression,omitempty"`
	IndexName                 string                     `json:"IndexName,omitempty"`
	TableName                 string                     `json:"TableName"`
	ConditionalOperator       string                     `json:"ConditionalOperator,omitempty"`
	AttributesToGet           []string                   `json:"AttributesToGet,omitempty"`
	Limit                     int32                      `json:"Limit,omitempty"`
	ConsistentRead            bool                       `json:"ConsistentRead,omitempty"`
}

type QueryOutput struct {
	LastEvaluatedKey map[string]any    `json:"LastEvaluatedKey,omitempty"`
	ConsumedCapacity *ConsumedCapacity `json:"ConsumedCapacity,omitempty"`
	Items            []map[string]any  `json:"Items,omitempty"`
	Count            int               `json:"Count"`
	ScannedCount     int               `json:"ScannedCount"`
}

// SearchVectorsInput mirrors dynamodb.SearchVectorsInput. SearchVector's
// elements are wire-format AttributeValue objects (e.g. {"N": "0.5"}), same
// convention as ExpressionAttributeValues' map values.
type SearchVectorsInput struct {
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues,omitempty"`
	TopK                      *int32            `json:"TopK"`
	TableName                 string            `json:"TableName"`
	IndexName                 string            `json:"IndexName"`
	ProjectionExpression      string            `json:"ProjectionExpression,omitempty"`
	SearchConditionExpression string            `json:"SearchConditionExpression,omitempty"`
	ReturnConsumedCapacity    string            `json:"ReturnConsumedCapacity,omitempty"`
	SearchVector              []any             `json:"SearchVector"`
}

type SearchVectorsOutput struct {
	ConsumedCapacity *VectorCapacity    `json:"ConsumedCapacity,omitempty"`
	SearchResults    []SearchResultItem `json:"SearchResults,omitempty"`
}

// SearchResultItem mirrors types.SearchResultItem.
type SearchResultItem struct {
	Item  map[string]any `json:"Item,omitempty"`
	Score float64        `json:"Score"`
}

type ScanInput struct {
	ConsistentRead            *bool                      `json:"ConsistentRead,omitempty"`
	ExclusiveStartKey         map[string]any             `json:"ExclusiveStartKey,omitempty"`
	ExpressionAttributeValues map[string]any             `json:"ExpressionAttributeValues,omitempty"`
	ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames,omitempty"`
	ScanFilter                map[string]LegacyCondition `json:"ScanFilter,omitempty"`
	TotalSegments             *int32                     `json:"TotalSegments,omitempty"`
	Segment                   *int32                     `json:"Segment,omitempty"`
	Limit                     *int32                     `json:"Limit,omitempty"`
	FilterExpression          string                     `json:"FilterExpression,omitempty"`
	Select                    string                     `json:"Select,omitempty"`
	ReturnConsumedCapacity    string                     `json:"ReturnConsumedCapacity,omitempty"`
	ProjectionExpression      string                     `json:"ProjectionExpression,omitempty"`
	IndexName                 string                     `json:"IndexName,omitempty"`
	TableName                 string                     `json:"TableName"`
	ConditionalOperator       string                     `json:"ConditionalOperator,omitempty"`
	AttributesToGet           []string                   `json:"AttributesToGet,omitempty"`
}

type ScanOutput struct {
	ConsumedCapacity *ConsumedCapacity `json:"ConsumedCapacity,omitempty"`
	LastEvaluatedKey map[string]any    `json:"LastEvaluatedKey,omitempty"`
	Items            []map[string]any  `json:"Items"`
	Count            int               `json:"Count"`
	ScannedCount     int               `json:"ScannedCount"`
}

// --- Batch Operations ---

type BatchGetItemInput struct {
	RequestItems           map[string]KeysAndAttributes `json:"RequestItems"`
	ReturnConsumedCapacity string                       `json:"ReturnConsumedCapacity,omitempty"`
}

type KeysAndAttributes struct {
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ConsistentRead           *bool             `json:"ConsistentRead,omitempty"`
	ProjectionExpression     string            `json:"ProjectionExpression,omitempty"`
	Keys                     []map[string]any  `json:"Keys"`
	AttributesToGet          []string          `json:"AttributesToGet,omitempty"`
}

type BatchGetItemOutput struct {
	Responses       map[string][]map[string]any  `json:"Responses,omitempty"`
	UnprocessedKeys map[string]KeysAndAttributes `json:"UnprocessedKeys,omitempty"`
}

type BatchWriteItemInput struct {
	RequestItems                map[string][]WriteRequest `json:"RequestItems"`
	ReturnConsumedCapacity      string                    `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string                    `json:"ReturnItemCollectionMetrics,omitempty"`
}

type WriteRequest struct {
	PutRequest    *PutRequest    `json:"PutRequest,omitempty"`
	DeleteRequest *DeleteRequest `json:"DeleteRequest,omitempty"`
}

type PutRequest struct {
	Item map[string]any `json:"Item"`
}

type DeleteRequest struct {
	Key map[string]any `json:"Key"`
}

type BatchWriteItemOutput struct {
	UnprocessedItems      map[string][]WriteRequest `json:"UnprocessedItems,omitempty"`
	ItemCollectionMetrics map[string][]any          `json:"ItemCollectionMetrics,omitempty"` // Simplified for now
	ConsumedCapacity      []ConsumedCapacity        `json:"ConsumedCapacity,omitempty"`
}

// --- Capacity & Metrics ---

type ConsumedCapacity struct {
	TableName          string  `json:"TableName,omitempty"`
	CapacityUnits      float64 `json:"CapacityUnits,omitempty"`
	ReadCapacityUnits  float64 `json:"ReadCapacityUnits,omitempty"`
	WriteCapacityUnits float64 `json:"WriteCapacityUnits,omitempty"`
}

type ItemCollectionMetrics struct {
	ItemCollectionKey   map[string]any `json:"ItemCollectionKey,omitempty"`
	SizeEstimateRangeGB []float64      `json:"SizeEstimateRangeGB,omitempty"`
}

// VectorCapacity mirrors types.VectorCapacity, the capacity units
// SearchVectors reports (distinct in shape from ConsumedCapacity).
type VectorCapacity struct {
	VectorSearchRequestBytes float64 `json:"VectorSearchRequestBytes,omitempty"`
	VectorWriteRequestBytes  float64 `json:"VectorWriteRequestBytes,omitempty"`
}

// --- TTL ---

type UpdateTimeToLiveInput struct {
	TableName               string                  `json:"TableName"`
	TimeToLiveSpecification TimeToLiveSpecification `json:"TimeToLiveSpecification"`
}

type UpdateTimeToLiveOutput struct {
	TimeToLiveSpecification TimeToLiveSpecification `json:"TimeToLiveSpecification"`
}

type TimeToLiveSpecification struct {
	AttributeName string `json:"AttributeName"`
	Enabled       bool   `json:"Enabled"`
}

type DescribeTimeToLiveInput struct {
	TableName string `json:"TableName"`
}

type DescribeTimeToLiveOutput struct {
	TimeToLiveDescription TimeToLiveDescription `json:"TimeToLiveDescription"`
}

type TimeToLiveDescription struct {
	AttributeName    string `json:"AttributeName,omitempty"`
	TimeToLiveStatus string `json:"TimeToLiveStatus"` // "ENABLED" or "DISABLED"
}

// --- Transact ---

type TransactWriteItemsInput struct {
	ReturnConsumedCapacity      string              `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string              `json:"ReturnItemCollectionMetrics,omitempty"`
	ClientRequestToken          string              `json:"ClientRequestToken,omitempty"`
	TransactItems               []TransactWriteItem `json:"TransactItems"`
}

type TransactWriteItem struct {
	Put            *PutItemInput        `json:"Put,omitempty"`
	Delete         *DeleteItemInput     `json:"Delete,omitempty"`
	Update         *UpdateItemInput     `json:"Update,omitempty"`
	ConditionCheck *ConditionCheckInput `json:"ConditionCheck,omitempty"`
}

type ConditionCheckInput struct {
	Key                                 map[string]any    `json:"Key"`
	ExpressionAttributeNames            map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues           map[string]any    `json:"ExpressionAttributeValues,omitempty"`
	TableName                           string            `json:"TableName"`
	ConditionExpression                 string            `json:"ConditionExpression"`
	ReturnValuesOnConditionCheckFailure string            `json:"ReturnValuesOnConditionCheckFailure,omitempty"`
}

type TransactWriteItemsOutput struct {
	ItemCollectionMetrics map[string][]ItemCollectionMetrics `json:"ItemCollectionMetrics,omitempty"`
	ConsumedCapacity      []ConsumedCapacity                 `json:"ConsumedCapacity,omitempty"`
}

type TransactGetItemsInput struct {
	ReturnConsumedCapacity string            `json:"ReturnConsumedCapacity,omitempty"`
	TransactItems          []TransactGetItem `json:"TransactItems"`
}

type TransactGetItem struct {
	Get *GetItemInput `json:"Get"`
}

type TransactGetItemsOutput struct {
	ConsumedCapacity []ConsumedCapacity `json:"ConsumedCapacity,omitempty"`
	Responses        []ItemResponse     `json:"Responses"`
}

type ItemResponse struct {
	Item map[string]any `json:"Item,omitempty"`
}

// --- Tagging ---

// Tag is a key-value pair attached to a DynamoDB resource.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// TagResourceInput is the wire format for TagResource.
type TagResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	Tags        []Tag  `json:"Tags"`
}

// TagResourceOutput is the wire format for TagResource response (empty body).
type TagResourceOutput struct{}

// UntagResourceInput is the wire format for UntagResource.
type UntagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

// UntagResourceOutput is the wire format for UntagResource response (empty body).
type UntagResourceOutput struct{}

// ListTagsOfResourceInput is the wire format for ListTagsOfResource.
type ListTagsOfResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	NextToken   string `json:"NextToken,omitempty"`
}

// ListTagsOfResourceOutput is the wire format for ListTagsOfResource response.
type ListTagsOfResourceOutput struct {
	NextToken string `json:"NextToken,omitempty"`
	Tags      []Tag  `json:"Tags"`
}

// --- Backups ---

// BackupStatus values.
const (
	BackupStatusCreating  = "CREATING"
	BackupStatusDeleted   = "DELETED"
	BackupStatusAvailable = "AVAILABLE"
)

// BackupType values.
const (
	BackupTypeUser      = "USER"
	BackupTypeSystem    = "SYSTEM"
	BackupTypeAwsBackup = "AWS_BACKUP"
)

// CreateBackupInput is the wire format for CreateBackup.
type CreateBackupInput struct {
	TableName  string `json:"TableName"`
	BackupName string `json:"BackupName"`
}

// CreateBackupOutput is the wire format for CreateBackup response.
type CreateBackupOutput struct {
	BackupDetails BackupDetails `json:"BackupDetails"`
}

// BackupDetails contains details of a backup.
//
// BackupCreationDateTime is Unix epoch seconds (with optional fractional
// part), matching how the real aws-sdk-go-v2 awsjson1_0 protocol serializes a
// *time.Time via smithytime.FormatEpochSeconds -- it is emitted as a JSON
// number, never a JSON string.
type BackupDetails struct {
	BackupArn              string  `json:"BackupArn"`
	BackupName             string  `json:"BackupName"`
	BackupStatus           string  `json:"BackupStatus"`
	BackupType             string  `json:"BackupType"`
	BackupCreationDateTime float64 `json:"BackupCreationDateTime"`
	BackupSizeBytes        int64   `json:"BackupSizeBytes,omitempty"`
}

// BackupDescription contains a full description of a backup.
type BackupDescription struct {
	BackupDetails      BackupDetails      `json:"BackupDetails"`
	SourceTableDetails SourceTableDetails `json:"SourceTableDetails"`
}

// SourceTableDetails describes the source table at backup creation time.
//
// TableCreationDateTime is Unix epoch seconds, matching how the real
// aws-sdk-go-v2 awsjson1_0 protocol serializes a *time.Time (see
// BackupDetails.BackupCreationDateTime above).
type SourceTableDetails struct {
	ProvisionedThroughput ProvisionedThroughput `json:"ProvisionedThroughput"`
	TableName             string                `json:"TableName"`
	TableArn              string                `json:"TableArn,omitempty"`
	TableID               string                `json:"TableId,omitempty"`
	KeySchema             []KeySchemaElement    `json:"KeySchema"`
	TableCreationDateTime float64               `json:"TableCreationDateTime"`
	ItemCount             int64                 `json:"ItemCount,omitempty"`
}

// DescribeBackupInput is the wire format for DescribeBackup.
type DescribeBackupInput struct {
	BackupArn string `json:"BackupArn"`
}

// DescribeBackupOutput is the wire format for DescribeBackup response.
type DescribeBackupOutput struct {
	BackupDescription BackupDescription `json:"BackupDescription"`
}

// DeleteBackupInput is the wire format for DeleteBackup.
type DeleteBackupInput struct {
	BackupArn string `json:"BackupArn"`
}

// DeleteBackupOutput is the wire format for DeleteBackup response.
type DeleteBackupOutput struct {
	BackupDescription BackupDescription `json:"BackupDescription"`
}

// ListBackupsInput is the wire format for ListBackups.
type ListBackupsInput struct {
	TimeRangeLowerBound     *float64 `json:"TimeRangeLowerBound,omitempty"`
	TimeRangeUpperBound     *float64 `json:"TimeRangeUpperBound,omitempty"`
	TableName               string   `json:"TableName,omitempty"`
	ExclusiveStartBackupArn string   `json:"ExclusiveStartBackupArn,omitempty"`
	BackupType              string   `json:"BackupType,omitempty"`
	Limit                   int      `json:"Limit,omitempty"`
}

// BackupSummary contains summary information about a backup.
//
// BackupCreationDateTime is Unix epoch seconds (with optional fractional
// part), matching how the real aws-sdk-go-v2 awsjson1_0 protocol serializes a
// *time.Time via smithytime.FormatEpochSeconds -- it is emitted as a JSON
// number, never a JSON string.
type BackupSummary struct {
	BackupArn              string  `json:"BackupArn"`
	BackupName             string  `json:"BackupName"`
	BackupStatus           string  `json:"BackupStatus"`
	BackupType             string  `json:"BackupType"`
	TableName              string  `json:"TableName"`
	TableArn               string  `json:"TableArn,omitempty"`
	TableID                string  `json:"TableId,omitempty"`
	BackupCreationDateTime float64 `json:"BackupCreationDateTime"`
	BackupSizeBytes        int64   `json:"BackupSizeBytes,omitempty"`
}

// ListBackupsOutput is the wire format for ListBackups response.
type ListBackupsOutput struct {
	LastEvaluatedBackupArn string          `json:"LastEvaluatedBackupArn,omitempty"`
	BackupSummaries        []BackupSummary `json:"BackupSummaries"`
}

// RestoreTableFromBackupInput is the wire format for RestoreTableFromBackup.
type RestoreTableFromBackupInput struct {
	ProvisionedThroughputOverride *ProvisionedThroughput `json:"ProvisionedThroughputOverride,omitempty"`
	OnDemandThroughputOverride    *OnDemandThroughput    `json:"OnDemandThroughputOverride,omitempty"`
	SSESpecificationOverride      *SSESpecification      `json:"SSESpecificationOverride,omitempty"`
	BackupArn                     string                 `json:"BackupArn"`
	TargetTableName               string                 `json:"TargetTableName"`
	BillingModeOverride           string                 `json:"BillingModeOverride,omitempty"`
	GlobalSecondaryIndexOverride  []GlobalSecondaryIndex `json:"GlobalSecondaryIndexOverride,omitempty"`
}

// RestoreTableFromBackupOutput is the wire format for RestoreTableFromBackup response.
type RestoreTableFromBackupOutput struct {
	TableDescription TableDescription `json:"TableDescription"`
}

// RestoreTableToPointInTimeInput is the wire format for RestoreTableToPointInTime.
//
// RestoreDateTime is a pointer to Unix epoch seconds (with optional fractional
// part), matching how the real aws-sdk-go-v2 awsjson1_0 protocol serializes a
// *time.Time via smithytime.FormatEpochSeconds -- it is emitted as a JSON
// number, never a JSON string. A pointer (rather than a bare float64) lets us
// distinguish "field omitted" from "epoch zero" (1970-01-01), which a bare
// zero value cannot.
type RestoreTableToPointInTimeInput struct {
	ProvisionedThroughputOverride *ProvisionedThroughput `json:"ProvisionedThroughputOverride,omitempty"`
	RestoreDateTime               *float64               `json:"RestoreDateTime,omitempty"`
	OnDemandThroughputOverride    *OnDemandThroughput    `json:"OnDemandThroughputOverride,omitempty"`
	SSESpecificationOverride      *SSESpecification      `json:"SSESpecificationOverride,omitempty"`
	SourceTableName               string                 `json:"SourceTableName"`
	TargetTableName               string                 `json:"TargetTableName"`
	BillingModeOverride           string                 `json:"BillingModeOverride,omitempty"`
	GlobalSecondaryIndexOverride  []GlobalSecondaryIndex `json:"GlobalSecondaryIndexOverride,omitempty"`
	UseLatestRestorableTime       bool                   `json:"UseLatestRestorableTime,omitempty"`
}

// RestoreTableToPointInTimeOutput is the wire format for RestoreTableToPointInTime response.
type RestoreTableToPointInTimeOutput struct {
	TableDescription TableDescription `json:"TableDescription"`
}
