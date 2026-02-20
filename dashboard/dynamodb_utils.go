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
	TableName              string
	PartitionKey           string
	PartitionKeyType       string
	SortKey                string
	SortKeyType            string
	GlobalSecondaryIndexes []IndexInfo
	LocalSecondaryIndexes  []IndexInfo
	ItemCount              int64
	GSICount               int
	LSICount               int
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
