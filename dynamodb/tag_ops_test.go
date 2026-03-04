package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddb "github.com/blackbirdworks/gopherstack/dynamodb"
)

// tableARN returns a fake DynamoDB table ARN used in tag tests.
func tableARN(name string) string {
	return "arn:aws:dynamodb:us-east-1:000000000000:table/" + name
}

func TestTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		tableName       string
		createTable     bool
		tagBatches      [][]types.Tag
		wantErr         bool
		wantErrContains string
		wantTagCount    int
		wantTagValues   map[string]string
	}{
		{
			name:        "add_and_list",
			tableName:   "tag-table",
			createTable: true,
			tagBatches: [][]types.Tag{
				{
					{Key: aws.String("env"), Value: aws.String("test")},
					{Key: aws.String("team"), Value: aws.String("platform")},
				},
			},
			wantTagCount:  2,
			wantTagValues: map[string]string{"env": "test", "team": "platform"},
		},
		{
			name:      "not_found",
			tableName: "nonexistent",
			tagBatches: [][]types.Tag{
				{{Key: aws.String("k"), Value: aws.String("v")}},
			},
			wantErr:         true,
			wantErrContains: "ResourceNotFoundException",
		},
		{
			name:        "overwrite_value",
			tableName:   "overwrite-tag-table",
			createTable: true,
			tagBatches: [][]types.Tag{
				{{Key: aws.String("env"), Value: aws.String("staging")}},
				{{Key: aws.String("env"), Value: aws.String("prod")}},
			},
			wantTagCount:  1,
			wantTagValues: map[string]string{"env": "prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := ddb.NewInMemoryDB()
			ctx := t.Context()

			if tt.createTable {
				createTableHelper(t, db, tt.tableName, "pk")
			}

			var err error
			for _, batch := range tt.tagBatches {
				_, err = db.TagResource(ctx, &dynamodb.TagResourceInput{
					ResourceArn: aws.String(tableARN(tt.tableName)),
					Tags:        batch,
				})
			}

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErrContains)

				return
			}

			require.NoError(t, err)

			out, err := db.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{
				ResourceArn: aws.String(tableARN(tt.tableName)),
			})
			require.NoError(t, err)
			require.Len(t, out.Tags, tt.wantTagCount)

			tagMap := make(map[string]string, len(out.Tags))
			for _, tag := range out.Tags {
				tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
			}

			for k, v := range tt.wantTagValues {
				assert.Equal(t, v, tagMap[k])
			}
		})
	}
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tableName    string
		initialTags  []types.Tag
		keysToRemove []string
		wantTagKey   string
		wantTagValue string
	}{
		{
			name:      "removes_one_tag",
			tableName: "untag-table",
			initialTags: []types.Tag{
				{Key: aws.String("keep"), Value: aws.String("yes")},
				{Key: aws.String("remove"), Value: aws.String("no")},
			},
			keysToRemove: []string{"remove"},
			wantTagKey:   "keep",
			wantTagValue: "yes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := ddb.NewInMemoryDB()
			ctx := t.Context()

			createTableHelper(t, db, tt.tableName, "pk")

			_, err := db.TagResource(ctx, &dynamodb.TagResourceInput{
				ResourceArn: aws.String(tableARN(tt.tableName)),
				Tags:        tt.initialTags,
			})
			require.NoError(t, err)

			_, err = db.UntagResource(ctx, &dynamodb.UntagResourceInput{
				ResourceArn: aws.String(tableARN(tt.tableName)),
				TagKeys:     tt.keysToRemove,
			})
			require.NoError(t, err)

			out, err := db.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{
				ResourceArn: aws.String(tableARN(tt.tableName)),
			})
			require.NoError(t, err)
			require.Len(t, out.Tags, 1)
			assert.Equal(t, tt.wantTagKey, aws.ToString(out.Tags[0].Key))
			assert.Equal(t, tt.wantTagValue, aws.ToString(out.Tags[0].Value))
		})
	}
}

func TestListTagsOfResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableName string
		wantEmpty bool
	}{
		{
			name:      "empty",
			tableName: "empty-tag-table",
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := ddb.NewInMemoryDB()
			ctx := t.Context()

			createTableHelper(t, db, tt.tableName, "pk")

			out, err := db.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{
				ResourceArn: aws.String(tableARN(tt.tableName)),
			})
			require.NoError(t, err)

			if tt.wantEmpty {
				assert.Empty(t, out.Tags)
			}
		})
	}
}
