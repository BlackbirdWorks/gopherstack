package resourceexplorer2_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	rtypes "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestViewCRUD(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	ctx := t.Context()

	_, err := c.CreateIndex(ctx, &sdk.CreateIndexInput{})
	require.NoError(t, err)

	created, err := c.CreateView(ctx, &sdk.CreateViewInput{
		ViewName:           aws.String("prod-only"),
		Filters:            &rtypes.SearchFilter{FilterString: aws.String("tag:env=prod")},
		IncludedProperties: []rtypes.IncludedProperty{{Name: aws.String("tags")}},
		Tags:               map[string]string{"owner": "me"},
	})
	require.NoError(t, err)

	view := created.View
	assert.Contains(t, aws.ToString(view.ViewArn), ":view/prod-only/")
	assert.Equal(t, testAccount, aws.ToString(view.Owner))
	assert.Equal(t, "tag:env=prod", aws.ToString(view.Filters.FilterString))
	assert.Equal(t, "arn:aws:iam::"+testAccount+":root", aws.ToString(view.Scope))

	got, err := c.GetView(ctx, &sdk.GetViewInput{ViewArn: view.ViewArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"owner": "me"}, got.Tags)

	upd, err := c.UpdateView(ctx, &sdk.UpdateViewInput{
		ViewArn: view.ViewArn, Filters: &rtypes.SearchFilter{FilterString: aws.String("service:s3")},
	})
	require.NoError(t, err)
	assert.Equal(t, "service:s3", aws.ToString(upd.View.Filters.FilterString))
	assert.Len(t, upd.View.IncludedProperties, 1, "properties untouched when omitted")

	upd, err = c.UpdateView(
		ctx,
		&sdk.UpdateViewInput{ViewArn: view.ViewArn, IncludedProperties: []rtypes.IncludedProperty{}},
	)
	require.NoError(t, err)
	assert.Empty(t, upd.View.IncludedProperties, "an explicit empty list clears properties")

	list, err := c.ListViews(ctx, &sdk.ListViewsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{aws.ToString(view.ViewArn)}, list.Views)

	batch, err := c.BatchGetView(ctx, &sdk.BatchGetViewInput{
		ViewArns: []string{
			aws.ToString(view.ViewArn),
			"arn:aws:resource-explorer-2:us-east-1:" + testAccount + ":view/x/1",
		},
	})
	require.NoError(t, err)
	require.Len(t, batch.Views, 1)
	require.Len(t, batch.Errors, 1)
	assert.Contains(t, aws.ToString(batch.Errors[0].ViewArn), "view/x/1")

	_, err = c.DeleteView(ctx, &sdk.DeleteViewInput{ViewArn: view.ViewArn})
	require.NoError(t, err)

	_, err = c.GetView(ctx, &sdk.GetViewInput{ViewArn: view.ViewArn})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestViewErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		in    sdk.CreateViewInput
		code  string
		noIdx bool
	}{
		{name: "bad_name", in: sdk.CreateViewInput{ViewName: aws.String("bad name!")}, code: "ValidationException"},
		{
			name: "name_too_long",
			in:   sdk.CreateViewInput{ViewName: aws.String(strings.Repeat("a", 65))},
			code: "ValidationException",
		},
		{
			name: "bad_filter",
			in: sdk.CreateViewInput{
				ViewName: aws.String("v"), Filters: &rtypes.SearchFilter{FilterString: aws.String("service:")},
			},
			code: "ValidationException",
		},
		{
			name: "bad_property",
			in: sdk.CreateViewInput{
				ViewName: aws.String("v"), IncludedProperties: []rtypes.IncludedProperty{{Name: aws.String("nope")}},
			},
			code: "ValidationException",
		},
		{
			name: "bad_scope",
			in:   sdk.CreateViewInput{ViewName: aws.String("v"), Scope: aws.String("nope")},
			code: "ValidationException",
		},
		{
			name:  "no_index",
			in:    sdk.CreateViewInput{ViewName: aws.String("v")},
			code:  "ResourceNotFoundException",
			noIdx: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newFixture(t).client(t)

			if !tc.noIdx {
				_, err := c.CreateIndex(t.Context(), &sdk.CreateIndexInput{})
				require.NoError(t, err)
			}

			_, err := c.CreateView(t.Context(), &tc.in)
			assert.Equal(t, tc.code, apiCode(err))
		})
	}
}

func TestViewDuplicateAndQuota(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	_, err := c.CreateIndex(t.Context(), &sdk.CreateIndexInput{})
	require.NoError(t, err)

	_, err = c.CreateView(t.Context(), &sdk.CreateViewInput{ViewName: aws.String("dup")})
	require.NoError(t, err)

	_, err = c.CreateView(t.Context(), &sdk.CreateViewInput{ViewName: aws.String("DUP")})
	assert.Equal(t, "ConflictException", apiCode(err))

	for i := 1; i < 50; i++ {
		_, err = c.CreateView(t.Context(), &sdk.CreateViewInput{ViewName: aws.String("v" + strings.Repeat("x", i))})
		require.NoError(t, err)
	}

	_, err = c.CreateView(t.Context(), &sdk.CreateViewInput{ViewName: aws.String("overflow")})
	assert.Equal(t, "ServiceQuotaExceededException", apiCode(err))
}

func TestDefaultView(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	ctx := t.Context()

	_, err := c.CreateIndex(ctx, &sdk.CreateIndexInput{})
	require.NoError(t, err)

	none, err := c.GetDefaultView(ctx, &sdk.GetDefaultViewInput{})
	require.NoError(t, err)
	assert.Nil(t, none.ViewArn)

	_, err = c.Search(ctx, &sdk.SearchInput{QueryString: aws.String("")})
	assert.Equal(t, "UnauthorizedException", apiCode(err), "search without a default view")

	v := createView(t, c, "main", "")

	got, err := c.GetDefaultView(ctx, &sdk.GetDefaultViewInput{})
	require.NoError(t, err)
	assert.Equal(t, v, aws.ToString(got.ViewArn))

	_, err = c.DeleteView(ctx, &sdk.DeleteViewInput{ViewArn: aws.String(v)})
	require.NoError(t, err)

	got, err = c.GetDefaultView(ctx, &sdk.GetDefaultViewInput{})
	require.NoError(t, err)
	assert.Nil(t, got.ViewArn, "deleting the default view clears it")

	v = createView(t, c, "again", "")

	_, err = c.DisassociateDefaultView(ctx, &sdk.DisassociateDefaultViewInput{})
	require.NoError(t, err)

	got, err = c.GetDefaultView(ctx, &sdk.GetDefaultViewInput{})
	require.NoError(t, err)
	assert.Nil(t, got.ViewArn)

	_, err = c.AssociateDefaultView(ctx, &sdk.AssociateDefaultViewInput{ViewArn: aws.String(v + "x")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestIndexDeletionRemovesViews(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	ctx := t.Context()

	idx, err := c.CreateIndex(ctx, &sdk.CreateIndexInput{})
	require.NoError(t, err)

	v := createView(t, c, "main", "")

	_, err = c.DeleteIndex(ctx, &sdk.DeleteIndexInput{Arn: idx.Arn})
	require.NoError(t, err)

	_, err = c.GetView(ctx, &sdk.GetViewInput{ViewArn: aws.String(v)})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	list, err := c.ListViews(ctx, &sdk.ListViewsInput{})
	require.NoError(t, err)
	assert.Empty(t, list.Views)
}

func TestViewsAreRegionalAndPerAccount(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	east, west := f.client(t), f.clientIn(t, "us-west-2", "test")
	other := f.clientIn(t, "us-east-1", "111111111111")
	ctx := t.Context()

	v := setupViewed(t, east)

	_, err := west.GetView(ctx, &sdk.GetViewInput{ViewArn: aws.String(v)})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "other Region")

	_, err = other.GetView(ctx, &sdk.GetViewInput{ViewArn: aws.String(v)})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "other account")

	_, err = other.GetIndex(ctx, &sdk.GetIndexInput{})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
