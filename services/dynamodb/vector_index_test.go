package dynamodb_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func vecList(vals ...float64) types.AttributeValue {
	l := make([]types.AttributeValue, len(vals))
	for i, v := range vals {
		l[i] = &types.AttributeValueMemberN{Value: strconv.FormatFloat(v, 'f', -1, 64)}
	}

	return &types.AttributeValueMemberL{Value: l}
}

func vecIndex(name, fn string, dims int64, schema ...types.SearchSchemaElement) types.VectorIndex {
	return types.VectorIndex{
		IndexName:        aws.String(name),
		VectorAttribute:  &types.VectorAttributeDefinition{AttributeName: aws.String("emb")},
		Dimensions:       aws.Int64(dims),
		DistanceFunction: types.VectorDistanceFunction(fn),
		Projection:       &types.Projection{ProjectionType: types.ProjectionTypeAll},
		SearchSchema:     schema,
	}
}

func newVectorTable(t *testing.T, indexes ...types.VectorIndex) *sdk.Client {
	t.Helper()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

	_, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String("docs"),
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode:   types.BillingModePayPerRequest,
		VectorIndexes: indexes,
	})
	require.NoError(t, err)

	rows := []struct {
		pk     string
		tenant string
		vec    []float64
	}{
		{"a", "t1", []float64{1, 0}},
		{"b", "t1", []float64{0, 1}},
		{"c", "t2", []float64{1, 1}},
	}

	for _, r := range rows {
		_, err = client.PutItem(
			t.Context(),
			&sdk.PutItemInput{TableName: aws.String("docs"), Item: map[string]types.AttributeValue{
				"pk":     &types.AttributeValueMemberS{Value: r.pk},
				"tenant": &types.AttributeValueMemberS{Value: r.tenant},
				"emb":    vecList(r.vec...),
			}},
		)
		require.NoError(t, err)
	}

	// Malformed vectors are not indexed.
	_, err = client.PutItem(
		t.Context(),
		&sdk.PutItemInput{TableName: aws.String("docs"), Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "bad"}, "emb": vecList(1, 2, 3),
		}},
	)
	require.NoError(t, err)

	return client
}

func TestSearchVectors_Ranking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fn        string
		wantOrder []string
		wantFirst float64
	}{
		{name: "cosine", fn: "COSINE", wantOrder: []string{"a", "c", "b"}, wantFirst: 0},
		{name: "euclidean", fn: "EUCLIDEAN", wantOrder: []string{"a", "c", "b"}, wantFirst: 0},
		{name: "dot product", fn: "DOT_PRODUCT", wantOrder: []string{"a", "c", "b"}, wantFirst: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newVectorTable(t, vecIndex("by-emb", tt.fn, 2))

			out, err := client.SearchVectors(t.Context(), &sdk.SearchVectorsInput{
				TableName: aws.String("docs"), IndexName: aws.String("by-emb"),
				SearchVector: []types.AttributeValue{
					&types.AttributeValueMemberN{Value: "1"}, &types.AttributeValueMemberN{Value: "0"},
				},
				TopK: aws.Int32(10),
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.SearchResults))
			for _, r := range out.SearchResults {
				got = append(got, r.Item["pk"].(*types.AttributeValueMemberS).Value)
			}

			assert.Equal(t, tt.wantOrder, got)
			assert.InDelta(t, tt.wantFirst, out.SearchResults[0].Score, 1e-9)
		})
	}
}

func TestSearchVectors_TopKAndFilter(t *testing.T) {
	t.Parallel()

	client := newVectorTable(t, vecIndex(
		"by-emb",
		"COSINE",
		2,
		types.SearchSchemaElement{
			AttributeName:           aws.String("tenant"),
			SearchSchemaElementType: types.SearchSchemaElementTypeHash,
		},
	))

	search := func(topK int32, cond string) (*sdk.SearchVectorsOutput, error) {
		in := &sdk.SearchVectorsInput{
			TableName: aws.String("docs"), IndexName: aws.String("by-emb"),
			SearchVector: []types.AttributeValue{
				&types.AttributeValueMemberN{Value: "1"}, &types.AttributeValueMemberN{Value: "0"},
			},
			TopK: aws.Int32(topK),
		}
		if cond != "" {
			in.SearchConditionExpression = aws.String(cond)
			in.ExpressionAttributeValues = map[string]types.AttributeValue{
				":t": &types.AttributeValueMemberS{Value: "t1"},
			}
		}

		return client.SearchVectors(t.Context(), in)
	}

	out, err := search(1, "")
	require.Error(t, err, "HASH attribute must be provided")
	assert.Nil(t, out)

	out, err = search(1, "tenant = :t")
	require.NoError(t, err)
	require.Len(t, out.SearchResults, 1)
	assert.Equal(t, "a", out.SearchResults[0].Item["pk"].(*types.AttributeValueMemberS).Value)

	out, err = search(5, "tenant = :t")
	require.NoError(t, err)
	assert.Len(t, out.SearchResults, 2)
}

func TestSearchVectors_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		index  string
		want   string
		vector []types.AttributeValue
		topK   int32
	}{
		{
			name:   "unknown index",
			index:  "nope",
			vector: []types.AttributeValue{&types.AttributeValueMemberN{Value: "1"}},
			topK:   1,
			want:   "ResourceNotFoundException",
		},
		{
			name:   "wrong dimensions",
			index:  "by-emb",
			vector: []types.AttributeValue{&types.AttributeValueMemberN{Value: "1"}},
			topK:   1,
			want:   "ValidationException",
		},
		{
			name:  "zero topk",
			index: "by-emb",
			vector: []types.AttributeValue{
				&types.AttributeValueMemberN{Value: "1"},
				&types.AttributeValueMemberN{Value: "1"},
			},
			topK: 0,
			want: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newVectorTable(t, vecIndex("by-emb", "COSINE", 2))

			_, err := client.SearchVectors(t.Context(), &sdk.SearchVectorsInput{
				TableName: aws.String("docs"), IndexName: aws.String(tt.index),
				SearchVector: tt.vector, TopK: aws.Int32(tt.topK),
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestVectorIndex_CreateValidation(t *testing.T) {
	t.Parallel()

	bad := vecIndex("x", "MANHATTAN", 2)
	zeroDim := vecIndex("y", "COSINE", 0)
	dupDims := []types.VectorIndex{vecIndex("p", "COSINE", 2), vecIndex("q", "COSINE", 3)}
	dupName := []types.VectorIndex{vecIndex("p", "COSINE", 2), vecIndex("p", "EUCLIDEAN", 2)}

	tests := []struct {
		name    string
		indexes []types.VectorIndex
	}{
		{name: "bad distance", indexes: []types.VectorIndex{bad}},
		{name: "zero dimensions", indexes: []types.VectorIndex{zeroDim}},
		{name: "same attribute different dimensions", indexes: dupDims},
		{name: "duplicate name", indexes: dupName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
			_, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
				TableName: aws.String("docs"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
				BillingMode:   types.BillingModePayPerRequest,
				VectorIndexes: tt.indexes,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "ValidationException")
		})
	}
}

func TestVectorIndex_DescribeUpdateRestore(t *testing.T) {
	t.Parallel()

	client := newVectorTable(t, vecIndex("by-emb", "COSINE", 2))
	ctx := t.Context()

	desc, err := client.DescribeTable(ctx, &sdk.DescribeTableInput{TableName: aws.String("docs")})
	require.NoError(t, err)
	require.Len(t, desc.Table.VectorIndexes, 1)

	vi := desc.Table.VectorIndexes[0]
	assert.Equal(t, "by-emb", aws.ToString(vi.IndexName))
	assert.Equal(t, types.IndexStatusActive, vi.IndexStatus)
	assert.Equal(t, types.VectorDistanceFunctionCosine, vi.DistanceFunction)
	assert.EqualValues(t, 2, aws.ToInt64(vi.Dimensions))
	assert.EqualValues(t, 3, aws.ToInt64(vi.ItemCount), "malformed vector is not counted")
	assert.Contains(t, aws.ToString(vi.IndexArn), "table/docs/index/by-emb")

	_, err = client.UpdateTable(ctx, &sdk.UpdateTableInput{
		TableName: aws.String("docs"),
		VectorIndexUpdates: []types.VectorIndexUpdate{{Create: &types.CreateVectorIndexAction{
			IndexName:        aws.String("by-dot"),
			VectorAttribute:  &types.VectorAttributeDefinition{AttributeName: aws.String("emb")},
			Dimensions:       aws.Int64(2),
			DistanceFunction: types.VectorDistanceFunctionDotProduct,
			Projection:       &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
		}}},
	})
	require.NoError(t, err)

	got, err := client.SearchVectors(ctx, &sdk.SearchVectorsInput{
		TableName: aws.String("docs"), IndexName: aws.String("by-dot"),
		SearchVector: []types.AttributeValue{
			&types.AttributeValueMemberN{Value: "1"}, &types.AttributeValueMemberN{Value: "1"},
		},
		TopK: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, got.SearchResults, 1)
	assert.NotContains(t, got.SearchResults[0].Item, "tenant", "KEYS_ONLY drops non-key attributes")
	assert.Contains(t, got.SearchResults[0].Item, "emb")

	_, err = client.UpdateTable(ctx, &sdk.UpdateTableInput{
		TableName: aws.String("docs"),
		VectorIndexUpdates: []types.VectorIndexUpdate{
			{Delete: &types.DeleteVectorIndexAction{IndexName: aws.String("by-emb")}},
			{Delete: &types.DeleteVectorIndexAction{IndexName: aws.String("by-dot")}},
		},
	})
	require.Error(t, err, "one vector index change per UpdateTable")

	_, err = client.UpdateTable(ctx, &sdk.UpdateTableInput{
		TableName: aws.String("docs"),
		VectorIndexUpdates: []types.VectorIndexUpdate{
			{Delete: &types.DeleteVectorIndexAction{IndexName: aws.String("by-emb")}},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateTable(ctx, &sdk.UpdateTableInput{
		TableName: aws.String("docs"),
		VectorIndexUpdates: []types.VectorIndexUpdate{
			{Delete: &types.DeleteVectorIndexAction{IndexName: aws.String("by-emb")}},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")

	bk, err := client.CreateBackup(
		ctx,
		&sdk.CreateBackupInput{TableName: aws.String("docs"), BackupName: aws.String("bk")},
	)
	require.NoError(t, err)

	kept, err := client.RestoreTableFromBackup(ctx, &sdk.RestoreTableFromBackupInput{
		BackupArn: bk.BackupDetails.BackupArn, TargetTableName: aws.String("docs-kept"),
	})
	require.NoError(t, err)
	require.Len(t, kept.TableDescription.VectorIndexes, 1)
	assert.Contains(t, aws.ToString(kept.TableDescription.VectorIndexes[0].IndexArn), "table/docs-kept/index/by-dot")

	dropped, err := client.RestoreTableFromBackup(ctx, &sdk.RestoreTableFromBackupInput{
		BackupArn: bk.BackupDetails.BackupArn, TargetTableName: aws.String("docs-dropped"),
		VectorIndexOverride: []types.VectorIndex{},
	})
	require.NoError(t, err)
	assert.Empty(t, dropped.TableDescription.VectorIndexes)
}
