package timestreamquery

import "time"

// StorageBackend defines the interface for Timestream Query backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	AccountID() string
	Region() string
	CreateScheduledQuery(
		name, queryString, scheduleExpression, executionRoleArn,
		notificationTopicArn, errorReportS3BucketName, targetDatabase, targetTable string,
		tags map[string]string,
	) (*ScheduledQuery, error)
	DescribeScheduledQuery(arnStr string) (*ScheduledQuery, error)
	DeleteScheduledQuery(arnStr string) error
	ListScheduledQueries() []ScheduledQuerySummary
	UpdateScheduledQuery(arnStr, state string) error
	ExecuteScheduledQuery(arnStr string, invocationTime time.Time) error
	Query(queryString string) *QueryResult
	CancelQuery(queryID string) error
	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, tagKeys []string) error
	ListTagsForResource(arn string) ([]map[string]string, error)
	DescribeAccountSettings() AccountSettings
	PrepareQuery(queryString string, validateOnly bool) (*PrepareQueryResult, error)
	UpdateAccountSettings(queryPricingModel string, maxQueryTCU *int32) (AccountSettings, error)
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
