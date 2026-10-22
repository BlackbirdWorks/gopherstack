package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const viewTemplate = `{"Head":{"Title":"Hi"},"Body":[{"Type":"Text","Value":"$.Name"}]}`

func TestViewCreateValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "viewvalid"))

	tests := []struct {
		mutate func(in *sdk.CreateViewInput)
		name   string
		code   string
	}{
		{
			name:   "template not json",
			mutate: func(in *sdk.CreateViewInput) { in.Content.Template = aws.String("{") },
			code:   "InvalidParameterException",
		},
		{
			name:   "empty template",
			mutate: func(in *sdk.CreateViewInput) { in.Content.Template = aws.String(" ") },
			code:   "InvalidParameterException",
		},
		{
			name:   "bad status",
			mutate: func(in *sdk.CreateViewInput) { in.Status = "DRAFT" },
			code:   "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &sdk.CreateViewInput{
				InstanceId: inst, Name: aws.String("v-" + tt.name), Status: types.ViewStatusPublished,
				Content: &types.ViewInputContent{Template: aws.String(viewTemplate), Actions: []string{"Go"}},
			}
			tt.mutate(in)
			_, err := f.client.CreateView(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestViewLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "views"))

	created, err := f.client.CreateView(t.Context(), &sdk.CreateViewInput{
		InstanceId: inst, Name: aws.String("greeting"), Status: types.ViewStatusPublished,
		Content: &types.ViewInputContent{Template: aws.String(viewTemplate), Actions: []string{"Go"}},
		Tags:    map[string]string{"k": "v"},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), created.View.Version)
	assert.Equal(t, types.ViewTypeCustomerManaged, created.View.Type)
	assert.Contains(t, aws.ToString(created.View.Content.InputSchema), "Name")

	_, err = f.client.CreateView(t.Context(), &sdk.CreateViewInput{
		InstanceId: inst, Name: aws.String("greeting"), Status: types.ViewStatusSaved,
		Content: &types.ViewInputContent{Template: aws.String(viewTemplate)},
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	_, err = f.client.CreateViewVersion(t.Context(), &sdk.CreateViewVersionInput{
		InstanceId: inst, ViewId: created.View.Id, ViewContentSha256: aws.String("stale"),
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	v2, err := f.client.CreateViewVersion(t.Context(), &sdk.CreateViewVersionInput{
		InstanceId: inst, ViewId: created.View.Id, VersionDescription: aws.String("two"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), v2.View.Version)

	saved, err := f.client.UpdateViewContent(t.Context(), &sdk.UpdateViewContentInput{
		InstanceId: inst, ViewId: created.View.Id, Status: types.ViewStatusSaved,
		Content: &types.ViewInputContent{Template: aws.String(`{"Body":[]}`), Actions: []string{}},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), saved.View.Version, "saving does not publish")

	published, err := f.client.UpdateViewContent(t.Context(), &sdk.UpdateViewContentInput{
		InstanceId: inst, ViewId: created.View.Id, Status: types.ViewStatusPublished,
		Content: &types.ViewInputContent{Template: aws.String(`{"Body":[1]}`), Actions: []string{}},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(3), published.View.Version)

	_, err = f.client.UpdateViewMetadata(t.Context(), &sdk.UpdateViewMetadataInput{
		InstanceId: inst, ViewId: created.View.Id, Name: aws.String("renamed"), Description: aws.String("d"),
	})
	require.NoError(t, err)

	got, err := f.client.DescribeView(t.Context(), &sdk.DescribeViewInput{InstanceId: inst, ViewId: created.View.Id})
	require.NoError(t, err)
	assert.Equal(t, "renamed", aws.ToString(got.View.Name))
	assert.Equal(t, "v", got.View.Tags["k"])

	old, err := f.client.DescribeView(t.Context(), &sdk.DescribeViewInput{
		InstanceId: inst, ViewId: aws.String(aws.ToString(created.View.Id) + ":1"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), old.View.Version)
	assert.JSONEq(t, viewTemplate, aws.ToString(old.View.Content.Template))

	versions, err := f.client.ListViewVersions(
		t.Context(),
		&sdk.ListViewVersionsInput{InstanceId: inst, ViewId: created.View.Id},
	)
	require.NoError(t, err)
	assert.Len(t, versions.ViewVersionSummaryList, 3)

	list, err := f.client.ListViews(t.Context(), &sdk.ListViewsInput{InstanceId: inst, Type: types.ViewTypeAwsManaged})
	require.NoError(t, err)
	assert.Empty(t, list.ViewsSummaryList)

	list, err = f.client.ListViews(
		t.Context(),
		&sdk.ListViewsInput{InstanceId: inst, Type: types.ViewTypeCustomerManaged},
	)
	require.NoError(t, err)
	assert.Len(t, list.ViewsSummaryList, 1)

	dst := f.restored(t)
	got, err = dst.client.DescribeView(t.Context(), &sdk.DescribeViewInput{InstanceId: inst, ViewId: created.View.Id})
	require.NoError(t, err)
	assert.Equal(t, int32(3), got.View.Version)

	_, err = f.client.DeleteViewVersion(
		t.Context(),
		&sdk.DeleteViewVersionInput{InstanceId: inst, ViewId: created.View.Id, ViewVersion: aws.Int32(1)},
	)
	require.NoError(t, err)
	_, err = f.client.DeleteViewVersion(
		t.Context(),
		&sdk.DeleteViewVersionInput{InstanceId: inst, ViewId: created.View.Id, ViewVersion: aws.Int32(1)},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DeleteView(t.Context(), &sdk.DeleteViewInput{InstanceId: inst, ViewId: created.View.Id})
	require.NoError(t, err)
	_, err = f.client.DescribeView(t.Context(), &sdk.DescribeViewInput{InstanceId: inst, ViewId: created.View.Id})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
