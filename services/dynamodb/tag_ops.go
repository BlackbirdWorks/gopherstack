package dynamodb

import (
	"context"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
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
	table := findTableByName(db.Tables, tableName)
	db.mu.RUnlock()

	if table == nil {
		return nil, NewResourceNotFoundException("table not found: " + tableName)
	}

	table.mu.Lock("TagResource")
	defer table.mu.Unlock()

	if table.Tags == nil {
		table.Tags = tags.New("dynamodb.table." + tableName + ".tags")
	}

	for _, tag := range input.Tags {
		table.Tags.Set(aws.ToString(tag.Key), aws.ToString(tag.Value))
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
	table := findTableByName(db.Tables, tableName)
	db.mu.RUnlock()

	if table == nil {
		return nil, NewResourceNotFoundException("table not found: " + tableName)
	}

	table.mu.Lock("UntagResource")
	defer table.mu.Unlock()

	if table.Tags != nil {
		table.Tags.DeleteKeys(input.TagKeys)
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
	table := findTableByName(db.Tables, tableName)
	db.mu.RUnlock()

	if table == nil {
		return nil, NewResourceNotFoundException("table not found: " + tableName)
	}

	table.mu.RLock("ListTagsOfResource")

	var tagMap map[string]string
	if table.Tags != nil {
		tagMap = table.Tags.Clone()
	}

	table.mu.RUnlock()

	keys := make([]string, 0, len(tagMap))
	for k := range tagMap {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	sdkTags := make([]types.Tag, 0, len(tagMap))
	for _, k := range keys {
		key, val := k, tagMap[k]
		sdkTags = append(sdkTags, types.Tag{Key: &key, Value: &val})
	}

	return &dynamodb.ListTagsOfResourceOutput{Tags: sdkTags}, nil
}

// findTableByName searches all region-keyed table maps for a table with the given name.
// Returns nil if not found. Must be called with db.mu held.
func findTableByName(tables map[string]map[string]*Table, name string) *Table {
	for _, regionTables := range tables {
		if t, ok := regionTables[name]; ok {
			return t
		}
	}

	return nil
}
