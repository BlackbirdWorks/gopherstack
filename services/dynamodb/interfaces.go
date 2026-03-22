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

	// Kinesis Streaming Operations
	DescribeKinesisStreamingDestination(
		context.Context,
		*dynamodb.DescribeKinesisStreamingDestinationInput,
	) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error)
	DisableKinesisStreamingDestination(
		context.Context,
		*dynamodb.DisableKinesisStreamingDestinationInput,
	) (*dynamodb.DisableKinesisStreamingDestinationOutput, error)

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
	DeleteResourcePolicy(
		context.Context,
		*dynamodb.DeleteResourcePolicyInput,
	) (*dynamodb.DeleteResourcePolicyOutput, error)
	DescribeImport(
		context.Context,
		*dynamodb.DescribeImportInput,
	) (*dynamodb.DescribeImportOutput, error)
}
