package models_test

import (
	"encoding/base64"
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToSDKAttributeValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err      error
		expected types.AttributeValue
		input    any
		name     string
	}{
		{
			name:     "String",
			input:    map[string]any{"S": "val"},
			expected: &types.AttributeValueMemberS{Value: "val"},
		},
		{
			name:     "Number",
			input:    map[string]any{"N": "123"},
			expected: &types.AttributeValueMemberN{Value: "123"},
		},
		{
			name:     "Binary-String",
			input:    map[string]any{"B": base64.StdEncoding.EncodeToString([]byte("bin"))},
			expected: &types.AttributeValueMemberB{Value: []byte("bin")},
		},
		{
			name:     "Binary-Bytes",
			input:    map[string]any{"B": []byte("bin")},
			expected: &types.AttributeValueMemberB{Value: []byte("bin")},
		},
		{
			name:     "Bool",
			input:    map[string]any{"BOOL": true},
			expected: &types.AttributeValueMemberBOOL{Value: true},
		},
		{
			name:     "Null",
			input:    map[string]any{"NULL": true},
			expected: &types.AttributeValueMemberNULL{Value: true},
		},
		{
			name: "Map",
			input: map[string]any{
				"M": map[string]any{
					"k": map[string]any{"S": "v"},
				},
			},
			expected: &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"k": &types.AttributeValueMemberS{Value: "v"},
				},
			},
		},
		{
			name:  "List",
			input: map[string]any{"L": []any{map[string]any{"S": "v"}}},
			expected: &types.AttributeValueMemberL{
				Value: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: "v"},
				},
			},
		},
		{
			name:     "StringSet-Any",
			input:    map[string]any{"SS": []any{"a", "b"}},
			expected: &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
		},
		{
			name:     "StringSet-String",
			input:    map[string]any{"SS": []string{"a", "b"}},
			expected: &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
		},
		{
			name:     "NumberSet",
			input:    map[string]any{"NS": []any{"1", "2"}},
			expected: &types.AttributeValueMemberNS{Value: []string{"1", "2"}},
		},
		{
			name:     "BinarySet-Any",
			input:    map[string]any{"BS": []any{base64.StdEncoding.EncodeToString([]byte("a"))}},
			expected: &types.AttributeValueMemberBS{Value: [][]byte{[]byte("a")}},
		},
		{
			name:     "BinarySet-Bytes",
			input:    map[string]any{"BS": [][]byte{[]byte("a")}},
			expected: &types.AttributeValueMemberBS{Value: [][]byte{[]byte("a")}},
		},
		{
			name:  "InvalidMap",
			input: "not a map",
			err:   models.ErrNotMap,
		},
		{
			name:  "WrongKeyCount",
			input: map[string]any{"S": "v", "N": "1"},
			err:   models.ErrInvalidTypeKeyCount,
		},
		{
			name:  "InvalidSS",
			input: map[string]any{"SS": []any{1, 2}},
			err:   models.ErrInvalidStringInSS,
		},
		{
			name:  "InvalidBS",
			input: map[string]any{"BS": []any{1, 2}},
			err:   models.ErrInvalidStringInBS,
		},
		{
			name:  "UnknownType",
			input: map[string]any{"X": "v"},
			err:   models.ErrUnknownAttributeType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := models.ToSDKAttributeValue(tt.input)
			if tt.err != nil {
				assert.ErrorIs(t, err, tt.err)

				return
			}
			require.NoError(t, err)

			opts := []cmp.Option{
				cmpopts.IgnoreUnexported(
					types.AttributeValueMemberS{},
					types.AttributeValueMemberN{},
					types.AttributeValueMemberB{},
					types.AttributeValueMemberBOOL{},
					types.AttributeValueMemberNULL{},
					types.AttributeValueMemberM{},
					types.AttributeValueMemberL{},
					types.AttributeValueMemberSS{},
					types.AttributeValueMemberNS{},
					types.AttributeValueMemberBS{},
				),
			}
			if diff := cmp.Diff(tt.expected, got, opts...); diff != "" {
				t.Errorf("ToSDKAttributeValue() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFromSDKAttributeValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    types.AttributeValue
		expected any
		name     string
	}{
		{
			name:     "String",
			input:    &types.AttributeValueMemberS{Value: "val"},
			expected: map[string]any{"S": "val"},
		},
		{
			name:     "Number",
			input:    &types.AttributeValueMemberN{Value: "123"},
			expected: map[string]any{"N": "123"},
		},
		{
			name:     "Binary",
			input:    &types.AttributeValueMemberB{Value: []byte("bin")},
			expected: map[string]any{"B": base64.StdEncoding.EncodeToString([]byte("bin"))},
		},
		{
			name:     "Bool",
			input:    &types.AttributeValueMemberBOOL{Value: true},
			expected: map[string]any{"BOOL": true},
		},
		{
			name:     "Null",
			input:    &types.AttributeValueMemberNULL{Value: true},
			expected: map[string]any{"NULL": true},
		},
		{
			name: "Map",
			input: &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"k": &types.AttributeValueMemberS{Value: "v"},
				},
			},
			expected: map[string]any{"M": map[string]any{"k": map[string]any{"S": "v"}}},
		},
		{
			name: "List",
			input: &types.AttributeValueMemberL{
				Value: []types.AttributeValue{&types.AttributeValueMemberS{Value: "v"}},
			},
			expected: map[string]any{
				"L": []any{
					map[string]any{"S": "v"},
				},
			},
		},
		{
			name:     "StringSet",
			input:    &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
			expected: map[string]any{"SS": []string{"a", "b"}},
		},
		{
			name:     "NumberSet",
			input:    &types.AttributeValueMemberNS{Value: []string{"1", "2"}},
			expected: map[string]any{"NS": []string{"1", "2"}},
		},
		{
			name:     "BinarySet",
			input:    &types.AttributeValueMemberBS{Value: [][]byte{[]byte("a")}},
			expected: map[string]any{"BS": []any{base64.StdEncoding.EncodeToString([]byte("a"))}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := models.FromSDKAttributeValue(tt.input)
			if diff := cmp.Diff(tt.expected, got); diff != "" {
				t.Errorf("FromSDKAttributeValue() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSchemaConversion(t *testing.T) {
	t.Parallel()

	schema := []models.KeySchemaElement{
		{AttributeName: "pk", KeyType: "HASH"},
		{AttributeName: "sk", KeyType: "RANGE"},
	}

	sdkSchema := models.ToSDKKeySchema(schema)
	assert.Len(t, sdkSchema, 2)
	assert.Equal(t, "pk", *sdkSchema[0].AttributeName)
	assert.Equal(t, types.KeyTypeHash, sdkSchema[0].KeyType)

	back := models.FromSDKKeySchema(sdkSchema)
	assert.Equal(t, schema, back)
}

func TestAttributeDefinitionConversion(t *testing.T) {
	t.Parallel()

	defs := []models.AttributeDefinition{
		{AttributeName: "pk", AttributeType: "S"},
	}

	sdkDefs := models.ToSDKAttributeDefinitions(defs)
	assert.Len(t, sdkDefs, 1)
	assert.Equal(t, "pk", *sdkDefs[0].AttributeName)
	assert.Equal(t, types.ScalarAttributeTypeS, sdkDefs[0].AttributeType)

	back := models.FromSDKAttributeDefinitions(sdkDefs)
	assert.Equal(t, defs, back)
}

func TestSecondaryIndexConversion(t *testing.T) {
	t.Parallel()

	gsis := []models.GlobalSecondaryIndex{
		{
			IndexName:             "GSI1",
			KeySchema:             []models.KeySchemaElement{{AttributeName: "k", KeyType: "HASH"}},
			Projection:            models.Projection{ProjectionType: "ALL"},
			ProvisionedThroughput: models.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(1)},
		},
	}

	sdkGsis := models.ToSDKGlobalSecondaryIndexes(gsis)
	assert.Len(t, sdkGsis, 1)
	assert.Equal(t, "GSI1", *sdkGsis[0].IndexName)

	back := models.FromSDKGlobalSecondaryIndexes(sdkGsis)
	assert.Equal(t, gsis[0].IndexName, back[0].IndexName)

	lsis := []models.LocalSecondaryIndex{
		{
			IndexName:  "LSI1",
			KeySchema:  []models.KeySchemaElement{{AttributeName: "k", KeyType: "HASH"}},
			Projection: models.Projection{ProjectionType: "ALL"},
		},
	}

	sdkLsis := models.ToSDKLocalSecondaryIndexes(lsis)
	assert.Len(t, sdkLsis, 1)
	assert.Equal(t, "LSI1", *sdkLsis[0].IndexName)

	backLsis := models.FromSDKLocalSecondaryIndexes(sdkLsis)
	assert.Equal(t, lsis[0].IndexName, backLsis[0].IndexName)
}

func TestFromSDKProjection(t *testing.T) {
	t.Parallel()

	sdkP := &types.Projection{
		ProjectionType:   types.ProjectionTypeInclude,
		NonKeyAttributes: []string{"a"},
	}

	got := models.FromSDKProjection(sdkP)
	assert.Equal(t, "INCLUDE", got.ProjectionType)
	assert.Equal(t, []string{"a"}, got.NonKeyAttributes)

	assert.Equal(t, models.Projection{}, models.FromSDKProjection(nil))
}

func TestToSDKMetadataDescriptions(t *testing.T) {
	t.Parallel()

	t.Run("GSI", func(t *testing.T) {
		t.Parallel()
		gsis := []models.GlobalSecondaryIndexDescription{
			{
				IndexName:   "GSI1",
				IndexStatus: "ACTIVE",
				ItemCount:   100,
				ProvisionedThroughput: models.ProvisionedThroughputDescription{
					ReadCapacityUnits:  5,
					WriteCapacityUnits: 5,
				},
			},
		}
		sdkGsis := models.ToSDKGlobalSecondaryIndexDescriptions(gsis)
		assert.Len(t, sdkGsis, 1)
		assert.Equal(t, "GSI1", *sdkGsis[0].IndexName)
	})

	t.Run("LSI", func(t *testing.T) {
		t.Parallel()
		lsis := []models.LocalSecondaryIndexDescription{
			{
				IndexName:      "LSI1",
				ItemCount:      50,
				IndexSizeBytes: 1024,
			},
		}
		sdkLsis := models.ToSDKLocalSecondaryIndexDescriptions(lsis)
		assert.Len(t, sdkLsis, 1)
		assert.Equal(t, "LSI1", *sdkLsis[0].IndexName)
	})
	t.Run("TTL", func(t *testing.T) {
		t.Parallel()
		sdkTTL := &dynamodb.DescribeTimeToLiveOutput{
			TimeToLiveDescription: &types.TimeToLiveDescription{
				AttributeName:    aws.String("ttl"),
				TimeToLiveStatus: types.TimeToLiveStatusEnabled,
			},
		}
		got := models.FromSDKDescribeTimeToLiveOutput(sdkTTL)
		assert.Equal(t, "ttl", got.TimeToLiveDescription.AttributeName)
		assert.Equal(t, "ENABLED", got.TimeToLiveDescription.TimeToLiveStatus)

		assert.Equal(
			t,
			models.DescribeTimeToLiveOutput{},
			*models.FromSDKDescribeTimeToLiveOutput(nil),
		)
	})
}
