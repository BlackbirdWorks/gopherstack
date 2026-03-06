package models

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
	ProvisionedThroughput  any                    `json:"ProvisionedThroughput"`
	StreamSpecification    any                    `json:"StreamSpecification,omitempty"`
	TableName              string                 `json:"TableName"`
	KeySchema              []KeySchemaElement     `json:"KeySchema"`
	AttributeDefinitions   []AttributeDefinition  `json:"AttributeDefinitions"`
	GlobalSecondaryIndexes []GlobalSecondaryIndex `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []LocalSecondaryIndex  `json:"LocalSecondaryIndexes,omitempty"`
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
	ProvisionedThroughput  *ProvisionedThroughputDescription `json:"ProvisionedThroughput,omitempty"`
	TableName              string                            `json:"TableName"`
	TableStatus            string                            `json:"TableStatus"`
	TableArn               string                            `json:"TableArn,omitempty"`
	TableID                string                            `json:"TableId,omitempty"`
	KeySchema              []KeySchemaElement                `json:"KeySchema"`
	AttributeDefinitions   []AttributeDefinition             `json:"AttributeDefinitions"`
	GlobalSecondaryIndexes []GlobalSecondaryIndexDescription `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []LocalSecondaryIndexDescription  `json:"LocalSecondaryIndexes,omitempty"`
	Replicas               []ReplicaDescription              `json:"Replicas,omitempty"`
	ItemCount              int                               `json:"ItemCount"`
}

type ProvisionedThroughputDescription struct {
	ReadCapacityUnits  int `json:"ReadCapacityUnits"`
	WriteCapacityUnits int `json:"WriteCapacityUnits"`
}

type GlobalSecondaryIndex struct {
	ProvisionedThroughput ProvisionedThroughput `json:"ProvisionedThroughput"`
	IndexName             string                `json:"IndexName"`
	Projection            Projection            `json:"Projection"`
	KeySchema             []KeySchemaElement    `json:"KeySchema"`
}

type GlobalSecondaryIndexDescription struct {
	IndexName             string                           `json:"IndexName"`
	IndexStatus           string                           `json:"IndexStatus"`
	Projection            Projection                       `json:"Projection"`
	KeySchema             []KeySchemaElement               `json:"KeySchema"`
	ProvisionedThroughput ProvisionedThroughputDescription `json:"ProvisionedThroughput"`
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

// UpdateTableInput is the wire-format for a DynamoDB UpdateTable request.
type UpdateTableInput struct {
	ProvisionedThroughput       *ProvisionedThroughput       `json:"ProvisionedThroughput,omitempty"`
	StreamSpecification         *StreamSpecificationInput    `json:"StreamSpecification,omitempty"`
	TableName                   string                       `json:"TableName"`
	AttributeDefinitions        []AttributeDefinition        `json:"AttributeDefinitions,omitempty"`
	GlobalSecondaryIndexUpdates []GlobalSecondaryIndexUpdate `json:"GlobalSecondaryIndexUpdates,omitempty"`
	ReplicaUpdates              []ReplicaUpdate              `json:"ReplicaUpdates,omitempty"`
}

// ReplicaUpdate describes a create or delete action for a Global Tables v2 replica.
type ReplicaUpdate struct {
	Create *CreateReplicationGroupMemberAction `json:"Create,omitempty"`
	Delete *DeleteReplicationGroupMemberAction `json:"Delete,omitempty"`
}

// CreateReplicationGroupMemberAction specifies parameters for creating a new replica.
type CreateReplicationGroupMemberAction struct {
	RegionName string `json:"RegionName"`
}

// DeleteReplicationGroupMemberAction specifies the region of the replica to delete.
type DeleteReplicationGroupMemberAction struct {
	RegionName string `json:"RegionName"`
}

// ReplicaDescription contains status information about a Global Tables v2 replica.
type ReplicaDescription struct {
	RegionName    string `json:"RegionName,omitempty"`
	ReplicaStatus string `json:"ReplicaStatus,omitempty"`
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
	Limit int `json:"Limit"`
}

type ListTablesOutput struct {
	TableNames []string `json:"TableNames"`
}

// --- Item Operations ---

type PutItemInput struct {
	TableName                   string            `json:"TableName"`
	Item                        map[string]any    `json:"Item"`
	ConditionExpression         string            `json:"ConditionExpression,omitempty"`
	ExpressionAttributeNames    map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues   map[string]any    `json:"ExpressionAttributeValues,omitempty"`
	ReturnValues                string            `json:"ReturnValues,omitempty"`
	ReturnConsumedCapacity      string            `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string            `json:"ReturnItemCollectionMetrics,omitempty"`
}

type PutItemOutput struct {
	Attributes            map[string]any         `json:"Attributes,omitempty"`
	ConsumedCapacity      *ConsumedCapacity      `json:"ConsumedCapacity,omitempty"`
	ItemCollectionMetrics *ItemCollectionMetrics `json:"ItemCollectionMetrics,omitempty"`
}

type UpdateItemInput struct {
	TableName                   string            `json:"TableName"`
	Key                         map[string]any    `json:"Key"`
	UpdateExpression            string            `json:"UpdateExpression,omitempty"`
	ConditionExpression         string            `json:"ConditionExpression,omitempty"`
	ExpressionAttributeNames    map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues   map[string]any    `json:"ExpressionAttributeValues,omitempty"`
	ReturnValues                string            `json:"ReturnValues,omitempty"`
	ReturnConsumedCapacity      string            `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string            `json:"ReturnItemCollectionMetrics,omitempty"`
}

type UpdateItemOutput struct {
	Attributes            map[string]any         `json:"Attributes,omitempty"`
	ConsumedCapacity      *ConsumedCapacity      `json:"ConsumedCapacity,omitempty"`
	ItemCollectionMetrics *ItemCollectionMetrics `json:"ItemCollectionMetrics,omitempty"`
}

type GetItemInput struct {
	Key                      map[string]any    `json:"Key"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames,omitempty"`
	TableName                string            `json:"TableName"`
	ProjectionExpression     string            `json:"ProjectionExpression,omitempty"`
}

type GetItemOutput struct {
	Item map[string]any `json:"Item,omitempty"`
}

type DeleteItemInput struct {
	Key                       map[string]any    `json:"Key"`
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues,omitempty"`
	TableName                 string            `json:"TableName"`
	ConditionExpression       string            `json:"ConditionExpression,omitempty"`
}

type DeleteItemOutput struct{}

// --- Query & Scan ---

type QueryInput struct {
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues,omitempty"`
	ScanIndexForward          *bool             `json:"ScanIndexForward,omitempty"`
	ExclusiveStartKey         map[string]any    `json:"ExclusiveStartKey,omitempty"`
	TableName                 string            `json:"TableName"`
	IndexName                 string            `json:"IndexName,omitempty"`
	KeyConditionExpression    string            `json:"KeyConditionExpression"`
	FilterExpression          string            `json:"FilterExpression,omitempty"`
	ProjectionExpression      string            `json:"ProjectionExpression,omitempty"`
	ReturnConsumedCapacity    string            `json:"ReturnConsumedCapacity,omitempty"`
	Limit                     int32             `json:"Limit,omitempty"`
}

type QueryOutput struct {
	LastEvaluatedKey map[string]any    `json:"LastEvaluatedKey,omitempty"`
	ConsumedCapacity *ConsumedCapacity `json:"ConsumedCapacity,omitempty"`
	Items            []map[string]any  `json:"Items,omitempty"`
	Count            int               `json:"Count"`
	ScannedCount     int               `json:"ScannedCount"`
}

type ScanInput struct {
	Limit                     *int32            `json:"Limit,omitempty"`
	Segment                   *int32            `json:"Segment,omitempty"`
	TotalSegments             *int32            `json:"TotalSegments,omitempty"`
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues,omitempty"`
	ExclusiveStartKey         map[string]any    `json:"ExclusiveStartKey,omitempty"`
	TableName                 string            `json:"TableName"`
	IndexName                 string            `json:"IndexName,omitempty"`
	FilterExpression          string            `json:"FilterExpression,omitempty"`
	ProjectionExpression      string            `json:"ProjectionExpression,omitempty"`
}

type ScanOutput struct {
	LastEvaluatedKey map[string]any   `json:"LastEvaluatedKey,omitempty"`
	Items            []map[string]any `json:"Items"`
	Count            int              `json:"Count"`
	ScannedCount     int              `json:"ScannedCount"`
}

// --- Batch Operations ---

type BatchGetItemInput struct {
	RequestItems map[string]KeysAndAttributes `json:"RequestItems"`
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
	RequestItems map[string][]WriteRequest `json:"RequestItems"`
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
	Key                       map[string]any    `json:"Key"`
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues,omitempty"`
	TableName                 string            `json:"TableName"`
	ConditionExpression       string            `json:"ConditionExpression"`
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
type BackupDetails struct {
	BackupArn              string `json:"BackupArn"`
	BackupName             string `json:"BackupName"`
	BackupStatus           string `json:"BackupStatus"`
	BackupType             string `json:"BackupType"`
	BackupCreationDateTime string `json:"BackupCreationDateTime"`
	BackupSizeBytes        int64  `json:"BackupSizeBytes,omitempty"`
}

// BackupDescription contains a full description of a backup.
type BackupDescription struct {
	BackupDetails      BackupDetails      `json:"BackupDetails"`
	SourceTableDetails SourceTableDetails `json:"SourceTableDetails"`
}

// SourceTableDetails describes the source table at backup creation time.
type SourceTableDetails struct {
	TableName string             `json:"TableName"`
	TableArn  string             `json:"TableArn,omitempty"`
	TableID   string             `json:"TableId,omitempty"`
	KeySchema []KeySchemaElement `json:"KeySchema"`
	ItemCount int64              `json:"ItemCount,omitempty"`
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
	TableName               string `json:"TableName,omitempty"`
	ExclusiveStartBackupArn string `json:"ExclusiveStartBackupArn,omitempty"`
	BackupType              string `json:"BackupType,omitempty"`
	Limit                   int    `json:"Limit,omitempty"`
}

// BackupSummary contains summary information about a backup.
type BackupSummary struct {
	BackupArn              string `json:"BackupArn"`
	BackupName             string `json:"BackupName"`
	BackupStatus           string `json:"BackupStatus"`
	BackupType             string `json:"BackupType"`
	BackupCreationDateTime string `json:"BackupCreationDateTime"`
	TableName              string `json:"TableName"`
	TableArn               string `json:"TableArn,omitempty"`
	TableID                string `json:"TableId,omitempty"`
}

// ListBackupsOutput is the wire format for ListBackups response.
type ListBackupsOutput struct {
	LastEvaluatedBackupArn string          `json:"LastEvaluatedBackupArn,omitempty"`
	BackupSummaries        []BackupSummary `json:"BackupSummaries"`
}

// RestoreTableFromBackupInput is the wire format for RestoreTableFromBackup.
type RestoreTableFromBackupInput struct {
	BackupArn       string `json:"BackupArn"`
	TargetTableName string `json:"TargetTableName"`
}

// RestoreTableFromBackupOutput is the wire format for RestoreTableFromBackup response.
type RestoreTableFromBackupOutput struct {
	TableDescription TableDescription `json:"TableDescription"`
}

// RestoreTableToPointInTimeInput is the wire format for RestoreTableToPointInTime.
type RestoreTableToPointInTimeInput struct {
	SourceTableName         string `json:"SourceTableName"`
	TargetTableName         string `json:"TargetTableName"`
	RestoreDateTime         string `json:"RestoreDateTime,omitempty"`
	UseLatestRestorableTime bool   `json:"UseLatestRestorableTime,omitempty"`
}

// RestoreTableToPointInTimeOutput is the wire format for RestoreTableToPointInTime response.
type RestoreTableToPointInTimeOutput struct {
	TableDescription TableDescription `json:"TableDescription"`
}
