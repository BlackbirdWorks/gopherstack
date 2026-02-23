package dynamodb

import (
	"context"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// tableNameFromARN extracts the table name from a DynamoDB table ARN.
// ARN format: arn:aws:dynamodb:<region>:<account>:table/<table-name>
// Falls back to treating the input as a plain table name if it's not an ARN.
func tableNameFromARN(resourceARN string) string {
	const tablePrefix = "table/"

	if _, after, ok := strings.Cut(resourceARN, tablePrefix); ok {
		return after
	}

	return resourceARN
}

// TagResource attaches tags to a DynamoDB table identified by its ARN.
func (db *InMemoryDB) TagResource(
	_ context.Context,
	input *dynamodb.TagResourceInput,
) (*dynamodb.TagResourceOutput, error) {
	tableName := tableNameFromARN(aws.ToString(input.ResourceArn))

	db.mu.RLock("TagResource")
	var table *Table

	for _, regionTables := range db.Tables {
		if t, ok := regionTables[tableName]; ok {
			table = t

			break
		}
	}

	db.mu.RUnlock()

	if table == nil {
		return nil, NewResourceNotFoundException("table not found: " + tableName)
	}

	table.mu.Lock("TagResource")
	defer table.mu.Unlock()

	if table.Tags == nil {
		table.Tags = make(map[string]string)
	}

	for _, tag := range input.Tags {
		table.Tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}

	return &dynamodb.TagResourceOutput{}, nil
}

// UntagResource removes tags from a DynamoDB table identified by its ARN.
func (db *InMemoryDB) UntagResource(
	_ context.Context,
	input *dynamodb.UntagResourceInput,
) (*dynamodb.UntagResourceOutput, error) {
	tableName := tableNameFromARN(aws.ToString(input.ResourceArn))

	db.mu.RLock("UntagResource")
	var table *Table

	for _, regionTables := range db.Tables {
		if t, ok := regionTables[tableName]; ok {
			table = t

			break
		}
	}

	db.mu.RUnlock()

	if table == nil {
		return nil, NewResourceNotFoundException("table not found: " + tableName)
	}

	table.mu.Lock("UntagResource")
	defer table.mu.Unlock()

	for _, key := range input.TagKeys {
		delete(table.Tags, key)
	}

	return &dynamodb.UntagResourceOutput{}, nil
}

// ListTagsOfResource returns the tags attached to a DynamoDB table identified by its ARN.
func (db *InMemoryDB) ListTagsOfResource(
	_ context.Context,
	input *dynamodb.ListTagsOfResourceInput,
) (*dynamodb.ListTagsOfResourceOutput, error) {
	tableName := tableNameFromARN(aws.ToString(input.ResourceArn))

	db.mu.RLock("ListTagsOfResource")
	var table *Table

	for _, regionTables := range db.Tables {
		if t, ok := regionTables[tableName]; ok {
			table = t

			break
		}
	}

	db.mu.RUnlock()

	if table == nil {
		return nil, NewResourceNotFoundException("table not found: " + tableName)
	}

	table.mu.RLock("ListTagsOfResource")

	keys := make([]string, 0, len(table.Tags))
	for k := range table.Tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	sdkTags := make([]types.Tag, 0, len(table.Tags))
	for _, k := range keys {
		key, val := k, table.Tags[k]
		sdkTags = append(sdkTags, types.Tag{Key: &key, Value: &val})
	}

	table.mu.RUnlock()

	return &dynamodb.ListTagsOfResourceOutput{Tags: sdkTags}, nil
}
