package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// SearchVectors performs a vector similarity search (see
// [dynamodb.Client.SearchVectors]).
//
// gopherstack doesn't model vector indexes (CreateTable/UpdateTable can't
// attach one, see PARITY.md), so fabricating similarity scores would violate
// the no-fabricated-data rule. Instead this validates the request and real
// table state, then honestly reports the named index as not found.
func (db *InMemoryDB) SearchVectors(
	ctx context.Context,
	input *dynamodb.SearchVectorsInput,
) (*dynamodb.SearchVectorsOutput, error) {
	if err := validateSearchVectorsInput(input); err != nil {
		return nil, err
	}

	tableName := aws.ToString(input.TableName)
	region := getRegionFromContext(ctx, db)

	table := db.getTableInRegionRLocked(region, tableName, "SearchVectors")
	if table == nil {
		return nil, NewResourceNotFoundException("table not found: " + tableName)
	}

	indexName := aws.ToString(input.IndexName)

	return nil, NewResourceNotFoundException(fmt.Sprintf("Index: %s not found", indexName))
}

// validateSearchVectorsInput enforces SearchVectorsInput's required fields,
// matching aws-sdk-go-v2's validateOpSearchVectorsInput (TableName, IndexName,
// SearchVector, TopK are all required).
func validateSearchVectorsInput(input *dynamodb.SearchVectorsInput) error {
	if aws.ToString(input.TableName) == "" {
		return NewValidationException("Table name is required")
	}

	if aws.ToString(input.IndexName) == "" {
		return NewValidationException("IndexName is required")
	}

	if len(input.SearchVector) == 0 {
		return NewValidationException("SearchVector is required")
	}

	if input.TopK == nil {
		return NewValidationException("TopK is required")
	}

	return nil
}
