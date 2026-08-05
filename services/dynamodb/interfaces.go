package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// StorageBackend defines the interface for DynamoDB storage operations
// using official AWS SDK Go v2 types.
type StorageBackend interface {
	// Table Operations
	CreateTable(context.Context, *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
	DeleteTable(context.Context, *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error)
	DescribeTable(
		context.Context,
		*dynamodb.DescribeTableInput,
	) (*dynamodb.DescribeTableOutput, error)
	ListTables(context.Context, *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error)
	UpdateTable(
		context.Context,
		*dynamodb.UpdateTableInput,
	) (*dynamodb.UpdateTableOutput, error)
	TagResource(
		context.Context,
		*dynamodb.TagResourceInput,
	) (*dynamodb.TagResourceOutput, error)
	UntagResource(
		context.Context,
		*dynamodb.UntagResourceInput,
	) (*dynamodb.UntagResourceOutput, error)
	ListTagsOfResource(
		context.Context,
		*dynamodb.ListTagsOfResourceInput,
	) (*dynamodb.ListTagsOfResourceOutput, error)
	UpdateTimeToLive(
		context.Context,
		*dynamodb.UpdateTimeToLiveInput,
	) (*dynamodb.UpdateTimeToLiveOutput, error)
	DescribeTimeToLive(
		context.Context,
		*dynamodb.DescribeTimeToLiveInput,
	) (*dynamodb.DescribeTimeToLiveOutput, error)

	// Item Operations
	PutItem(context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	GetItem(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	Scan(context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
	Query(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
	SearchVectors(
		context.Context,
		*dynamodb.SearchVectorsInput,
	) (*dynamodb.SearchVectorsOutput, error)
	BatchGetItem(context.Context, *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error)
	BatchWriteItem(
		context.Context,
		*dynamodb.BatchWriteItemInput,
	) (*dynamodb.BatchWriteItemOutput, error)

	// Transaction Operations
	TransactWriteItems(
		context.Context,
		*dynamodb.TransactWriteItemsInput,
	) (*dynamodb.TransactWriteItemsOutput, error)
	TransactGetItems(
		context.Context,
		*dynamodb.TransactGetItemsInput,
	) (*dynamodb.TransactGetItemsOutput, error)

	// Global Table Operations
	CreateGlobalTable(
		context.Context,
		*dynamodb.CreateGlobalTableInput,
	) (*dynamodb.CreateGlobalTableOutput, error)
	DescribeGlobalTable(
		context.Context,
		*dynamodb.DescribeGlobalTableInput,
	) (*dynamodb.DescribeGlobalTableOutput, error)
	DescribeGlobalTableSettings(
		context.Context,
		*dynamodb.DescribeGlobalTableSettingsInput,
	) (*dynamodb.DescribeGlobalTableSettingsOutput, error)
	ListGlobalTables(
		context.Context,
		*dynamodb.ListGlobalTablesInput,
	) (*dynamodb.ListGlobalTablesOutput, error)
	UpdateGlobalTable(
		context.Context,
		*dynamodb.UpdateGlobalTableInput,
	) (*dynamodb.UpdateGlobalTableOutput, error)

	// Kinesis Streaming Operations
	EnableKinesisStreamingDestination(
		context.Context,
		*dynamodb.EnableKinesisStreamingDestinationInput,
	) (*dynamodb.EnableKinesisStreamingDestinationOutput, error)
	DescribeKinesisStreamingDestination(
		context.Context,
		*dynamodb.DescribeKinesisStreamingDestinationInput,
	) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error)
	DisableKinesisStreamingDestination(
		context.Context,
		*dynamodb.DisableKinesisStreamingDestinationInput,
	) (*dynamodb.DisableKinesisStreamingDestinationOutput, error)

	// Resource Policy Operations
	GetResourcePolicy(
		context.Context,
		*dynamodb.GetResourcePolicyInput,
	) (*dynamodb.GetResourcePolicyOutput, error)
	PutResourcePolicy(
		context.Context,
		*dynamodb.PutResourcePolicyInput,
	) (*dynamodb.PutResourcePolicyOutput, error)
	DeleteResourcePolicy(
		context.Context,
		*dynamodb.DeleteResourcePolicyInput,
	) (*dynamodb.DeleteResourcePolicyOutput, error)

	// Miscellaneous Operations
	DescribeLimits(
		context.Context,
		*dynamodb.DescribeLimitsInput,
	) (*dynamodb.DescribeLimitsOutput, error)
	DescribeEndpoints(
		context.Context,
		*dynamodb.DescribeEndpointsInput,
	) (*dynamodb.DescribeEndpointsOutput, error)
	DescribeContributorInsights(
		context.Context,
		*dynamodb.DescribeContributorInsightsInput,
	) (*dynamodb.DescribeContributorInsightsOutput, error)
	DescribeImport(
		context.Context,
		*dynamodb.DescribeImportInput,
	) (*dynamodb.DescribeImportOutput, error)
	ListContributorInsights(
		context.Context,
		*dynamodb.ListContributorInsightsInput,
	) (*dynamodb.ListContributorInsightsOutput, error)
	UpdateContributorInsights(
		context.Context,
		*dynamodb.UpdateContributorInsightsInput,
	) (*dynamodb.UpdateContributorInsightsOutput, error)
	UpdateGlobalTableSettings(
		context.Context,
		*dynamodb.UpdateGlobalTableSettingsInput,
	) (*dynamodb.UpdateGlobalTableSettingsOutput, error)
	UpdateKinesisStreamingDestination(
		context.Context,
		*dynamodb.UpdateKinesisStreamingDestinationInput,
	) (*dynamodb.UpdateKinesisStreamingDestinationOutput, error)
	UpdateTableReplicaAutoScaling(
		context.Context,
		*dynamodb.UpdateTableReplicaAutoScalingInput,
	) (*dynamodb.UpdateTableReplicaAutoScalingOutput, error)
	ExecuteTransaction(
		context.Context,
		*dynamodb.ExecuteTransactionInput,
	) (*dynamodb.ExecuteTransactionOutput, error)
	ImportTable(
		context.Context,
		*dynamodb.ImportTableInput,
	) (*dynamodb.ImportTableOutput, error)
	ListImports(
		context.Context,
		*dynamodb.ListImportsInput,
	) (*dynamodb.ListImportsOutput, error)

	// Backup Operations
	CreateBackup(
		context.Context,
		*dynamodb.CreateBackupInput,
	) (*dynamodb.CreateBackupOutput, error)
	DescribeBackup(
		context.Context,
		*dynamodb.DescribeBackupInput,
	) (*dynamodb.DescribeBackupOutput, error)
	DeleteBackup(
		context.Context,
		*dynamodb.DeleteBackupInput,
	) (*dynamodb.DeleteBackupOutput, error)

	// PartiQL Batch Operations
	BatchExecuteStatement(
		context.Context,
		*dynamodb.BatchExecuteStatementInput,
	) (*dynamodb.BatchExecuteStatementOutput, error)
}
