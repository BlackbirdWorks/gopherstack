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
}
