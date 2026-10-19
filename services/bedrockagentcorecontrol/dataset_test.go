package bedrockagentcorecontrol_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func inlineSource(examples ...map[string]any) types.DataSourceType {
	docs := make([]document.Interface, 0, len(examples))
	for _, e := range examples {
		docs = append(docs, document.NewLazyDocument(e))
	}

	return &types.DataSourceTypeMemberInlineExamples{Value: types.InlineExamplesSource{Examples: docs}}
}

func TestDatasetExamplesAndVersions(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	created, err := f.client.CreateDataset(ctx, &sdk.CreateDatasetInput{
		DatasetName: aws.String("evals"), SchemaType: types.DatasetSchemaTypeThirdPartyEvaluationV1,
		Source: inlineSource(map[string]any{"input": "a"}, map[string]any{"exampleId": "fixed", "input": "b"}),
	})
	require.NoError(t, err)

	id := created.DatasetId

	got, err := f.client.GetDataset(ctx, &sdk.GetDatasetInput{DatasetId: id})
	require.NoError(t, err)
	assert.Equal(t, types.DatasetStatusActive, got.Status)
	assert.Equal(t, int64(2), aws.ToInt64(got.ExampleCount))
	assert.Equal(t, types.DraftStatusUnmodified, got.DraftStatus)

	added, err := f.client.AddDatasetExamples(ctx, &sdk.AddDatasetExamplesInput{
		DatasetId: id, Source: inlineSource(map[string]any{"input": "c"}),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), aws.ToInt64(added.AddedCount))
	assert.Len(t, added.ExampleIds, 1)

	_, err = f.client.UpdateDatasetExamples(ctx, &sdk.UpdateDatasetExamplesInput{
		DatasetId: id,
		Examples:  []document.Interface{document.NewLazyDocument(map[string]any{"exampleId": "fixed", "input": "B"})},
	})
	require.NoError(t, err)

	_, err = f.client.UpdateDatasetExamples(ctx, &sdk.UpdateDatasetExamplesInput{
		DatasetId: id, Examples: []document.Interface{document.NewLazyDocument(map[string]any{"exampleId": "ghost"})},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	ver, err := f.client.CreateDatasetVersion(ctx, &sdk.CreateDatasetVersionInput{DatasetId: id})
	require.NoError(t, err)
	assert.Equal(t, "1", aws.ToString(ver.DatasetVersion))

	del, err := f.client.DeleteDatasetExamples(
		ctx,
		&sdk.DeleteDatasetExamplesInput{DatasetId: id, ExampleIds: []string{"fixed"}},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), aws.ToInt64(del.DeletedCount))

	draft, err := f.client.ListDatasetExamples(ctx, &sdk.ListDatasetExamplesInput{DatasetId: id})
	require.NoError(t, err)
	assert.Len(t, draft.Examples, 2)
	assert.Equal(t, "DRAFT", aws.ToString(draft.DatasetVersion))

	v1, err := f.client.ListDatasetExamples(
		ctx,
		&sdk.ListDatasetExamplesInput{DatasetId: id, DatasetVersion: aws.String("1")},
	)
	require.NoError(t, err)
	assert.Len(t, v1.Examples, 3, "the version snapshot is unaffected by later draft edits")

	page, err := f.client.ListDatasetExamples(
		ctx,
		&sdk.ListDatasetExamplesInput{DatasetId: id, MaxResults: aws.Int32(2), DatasetVersion: aws.String("1")},
	)
	require.NoError(t, err)
	assert.Len(t, page.Examples, 2)
	assert.NotNil(t, page.NextToken)

	versions, err := f.client.ListDatasetVersions(ctx, &sdk.ListDatasetVersionsInput{DatasetId: id})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 1)
	assert.Equal(t, int64(3), aws.ToInt64(versions.Versions[0].ExampleCount))

	snap, err := f.client.GetDataset(ctx, &sdk.GetDatasetInput{DatasetId: id, DatasetVersion: aws.String("1")})
	require.NoError(t, err)
	assert.Equal(t, "1", aws.ToString(snap.DatasetVersion))

	_, err = f.client.DeleteDataset(ctx, &sdk.DeleteDatasetInput{DatasetId: id, DatasetVersion: aws.String("1")})
	require.NoError(t, err)

	versions, err = f.client.ListDatasetVersions(ctx, &sdk.ListDatasetVersionsInput{DatasetId: id})
	require.NoError(t, err)
	assert.Empty(t, versions.Versions)

	_, err = f.client.DeleteDataset(ctx, &sdk.DeleteDatasetInput{DatasetId: id})
	require.NoError(t, err)
}

func TestDatasetS3Source(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetReferences(
		fakeRefs{objects: map[string][]byte{"data/ex.jsonl": []byte("{\"input\":\"a\"}\n\n{\"input\":\"b\"}\n")}},
	)

	tests := []struct {
		name       string
		uri        string
		wantStatus types.DatasetStatus
		wantCount  int64
	}{
		{"jsonl", "s3://data/ex.jsonl", types.DatasetStatusActive, 2},
		{"missing", "s3://data/missing.jsonl", types.DatasetStatusCreateFailed, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := f.client.CreateDataset(t.Context(), &sdk.CreateDatasetInput{
				DatasetName: aws.String("s3_" + tt.name), SchemaType: types.DatasetSchemaTypeThirdPartyEvaluationV1,
				Source: &types.DataSourceTypeMemberS3Source{Value: types.S3Source{S3Uri: aws.String(tt.uri)}},
			})
			require.NoError(t, err)

			got, err := f.client.GetDataset(t.Context(), &sdk.GetDatasetInput{DatasetId: out.DatasetId})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.Status)
			assert.Equal(t, tt.wantCount, aws.ToInt64(got.ExampleCount))

			if tt.wantStatus == types.DatasetStatusCreateFailed {
				assert.NotEmpty(t, aws.ToString(got.FailureReason))
			}
		})
	}

	_, err := f.client.CreateDataset(t.Context(), &sdk.CreateDatasetInput{
		DatasetName: aws.String("bad_uri"), SchemaType: types.DatasetSchemaTypeThirdPartyEvaluationV1,
		Source: &types.DataSourceTypeMemberS3Source{Value: types.S3Source{S3Uri: aws.String("http://x")}},
	})
	assert.Equal(t, "ValidationException", apiCode(err))
}
