package dashboard

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	defaultTableLimit = 100
	maxSearchTables   = 1000
)

const (
	defaultCapacity   = 5
	maxS3ObjectSearch = 100
)

var (
	errAttrDefNotFound = errors.New("attribute definition not found")
)

// TableInfo represents table information for display.
type TableInfo struct {
	Pagination             *PaginationInfo
	TTLStatus              string
	TableName              string
	PartitionKey           string
	PartitionKeyType       string
	SortKey                string
	SortKeyType            string
	TTLAttribute           string
	StreamViewType         string
	StreamARN              string
	GlobalSecondaryIndexes []IndexInfo
	LocalSecondaryIndexes  []IndexInfo
	StreamEvents           []StreamEventRow
	GSICount               int
	LSICount               int
	ItemCount              int64
	StreamsEnabled         bool
}

// PaginationInfo represented info for shared pagination component.
type PaginationInfo struct {
	SearchQuery  string
	BaseEndpoint string
	TargetID     string
	TotalItems   int
	Offset       int
	Limit        int
	CurrentPage  int
	TotalPages   int
	PrevOffset   int
	NextOffset   int
	HasPrev      bool
	HasNext      bool
}

// StreamEventRow represents a single stream event for dashboard display.
type StreamEventRow struct {
	EventID   string
	EventName string // INSERT, MODIFY, REMOVE
	Timestamp int64  // Unix seconds
}

// IndexInfo represents index information.
type IndexInfo struct {
	IndexName        string
	PartitionKey     string
	PartitionKeyType string
	SortKey          string
	SortKeyType      string
	ProjectionType   string
}

// QueryResult represents query results.
type QueryResult struct {
	LastEvaluatedKey map[string]types.AttributeValue
	Items            []map[string]types.AttributeValue
	Count            int32
	ScannedCount     int32
}

// QueryParams represents query parameters.
type QueryParams struct {
	IndexName         string
	PartitionKeyValue string
	SortKeyOperator   string
	SortKeyValue      string
	SortKeyValue2     string
	FilterExp         string
	LimitStr          string
	ExclusiveStartKey string
}
