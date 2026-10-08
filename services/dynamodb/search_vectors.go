package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// SearchVectors performs a brute-force similarity search over the items that
// hold a well-formed vector in the index's attribute (see [dynamodb.Client.SearchVectors]).
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

	query, err := newVectorQuery(input)
	if err != nil {
		return nil, err
	}

	table.mu.RLock("SearchVectors")
	defer table.mu.RUnlock()

	desc, found := findVectorIndex(table, aws.ToString(input.IndexName))
	if !found {
		return nil, NewResourceNotFoundException(fmt.Sprintf("Index: %s not found", aws.ToString(input.IndexName)))
	}

	if err = query.bind(desc); err != nil {
		return nil, err
	}

	hits := query.rank(table, desc)
	results := make([]types.SearchResultItem, 0, len(hits))

	for _, hit := range hits {
		projected, projErr := query.project(table, desc, hit.item)
		if projErr != nil {
			return nil, projErr
		}

		item, convErr := models.ToSDKItem(projected)
		if convErr != nil {
			return nil, NewValidationException(convErr.Error())
		}

		results = append(results, types.SearchResultItem{Item: item, Score: hit.score})
	}

	return &dynamodb.SearchVectorsOutput{SearchResults: results}, nil
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
