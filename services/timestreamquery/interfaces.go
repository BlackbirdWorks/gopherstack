package timestreamquery

import (
	"context"
	"time"
)

// StorageBackend defines the interface for Timestream Query backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	AccountID() string
	Region() string
	CreateScheduledQuery(
		ctx context.Context,
		name, queryString, scheduleExpression, executionRoleArn,
		notificationTopicArn, errorReportS3BucketName, targetDatabase, targetTable, clientToken, kmsKeyID string,
		tags map[string]string,
		targetDetail *ScheduledQueryTargetDetail,
	) (*ScheduledQuery, error)
	DescribeScheduledQuery(ctx context.Context, arnStr string) (*ScheduledQuery, error)
	DeleteScheduledQuery(ctx context.Context, arnStr string) error
	ListScheduledQueries(ctx context.Context) []ScheduledQuerySummary
	ListScheduledQueriesFull(ctx context.Context) []*ScheduledQuery
	ListScheduledQueriesEnriched(ctx context.Context, nextToken string, maxResults int32) ListScheduledQueriesResult
	UpdateScheduledQuery(ctx context.Context, arnStr, state string) error
	ExecuteScheduledQuery(ctx context.Context, arnStr string, invocationTime time.Time) error
	Query(ctx context.Context, queryString string) *QueryResult
	QueryWithOptions(ctx context.Context, opts QueryOptions) (*QueryPage, error)
	CancelQuery(ctx context.Context, queryID string) error
	TagResource(ctx context.Context, arnStr string, tags map[string]string) error
	UntagResource(ctx context.Context, arnStr string, tagKeys []string) error
	ListTagsForResource(ctx context.Context, arnStr string) ([]map[string]string, error)
	DescribeAccountSettings(ctx context.Context) AccountSettings
	PrepareQuery(ctx context.Context, queryString string, validateOnly bool) (*PrepareQueryResult, error)
	UpdateAccountSettings(
		ctx context.Context, queryPricingModel string, maxQueryTCU *int32, queryCompute *QueryComputeUpdate,
	) (AccountSettings, error)
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
