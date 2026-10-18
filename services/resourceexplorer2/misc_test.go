package resourceexplorer2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	rtypes "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourceexplorer2"
)

func TestTags(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	ctx := t.Context()

	idx, err := c.CreateIndex(ctx, &sdk.CreateIndexInput{Tags: map[string]string{"a": "1"}})
	require.NoError(t, err)

	view := createView(t, c, "v", "")

	tests := []struct {
		name string
		arn  string
	}{
		{name: "index", arn: aws.ToString(idx.Arn)},
		{name: "view", arn: view},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, terr := c.TagResource(ctx, &sdk.TagResourceInput{
				ResourceArn: aws.String(tc.arn), Tags: map[string]string{"k" + tc.name: "v", "x": "y"},
			})
			require.NoError(t, terr)

			got, terr := c.ListTagsForResource(ctx, &sdk.ListTagsForResourceInput{ResourceArn: aws.String(tc.arn)})
			require.NoError(t, terr)
			assert.Equal(t, "v", got.Tags["k"+tc.name])

			_, terr = c.UntagResource(ctx, &sdk.UntagResourceInput{
				ResourceArn: aws.String(tc.arn), TagKeys: []string{"k" + tc.name, "x"},
			})
			require.NoError(t, terr)

			got, terr = c.ListTagsForResource(ctx, &sdk.ListTagsForResourceInput{ResourceArn: aws.String(tc.arn)})
			require.NoError(t, terr)
			assert.NotContains(t, got.Tags, "k"+tc.name)
		})
	}

	_, err = c.TagResource(ctx, &sdk.TagResourceInput{
		ResourceArn: aws.String(
			"arn:aws:resource-explorer-2:us-east-1:1:index/none",
		),
		Tags: map[string]string{"a": "b"},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = c.TagResource(ctx, &sdk.TagResourceInput{ResourceArn: idx.Arn, Tags: map[string]string{"aws:x": "b"}})
	assert.Equal(t, "ValidationException", apiCode(err))

	got, err := c.GetIndex(ctx, &sdk.GetIndexInput{})
	require.NoError(t, err)
	assert.Equal(t, "1", got.Tags["a"])
}

func TestSupportedResourceTypes(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)

	var (
		token *string
		total int
		seen  = map[string]bool{}
	)

	for {
		out, err := c.ListSupportedResourceTypes(t.Context(), &sdk.ListSupportedResourceTypesInput{
			MaxResults: aws.Int32(20), NextToken: token,
		})
		require.NoError(t, err)

		for _, rt := range out.ResourceTypes {
			total++

			seen[aws.ToString(rt.ResourceType)] = true

			assert.Equal(
				t,
				aws.ToString(rt.Service)+":",
				aws.ToString(rt.ResourceType)[:len(aws.ToString(rt.Service))+1],
			)
			assert.NotEmpty(t, rt.CFNResourceTypes)
		}

		if token = out.NextToken; token == nil {
			break
		}
	}

	assert.Len(t, seen, total)
	assert.True(t, seen["ec2:instance"])
	assert.True(t, seen["s3:bucket"])
}

func TestManagedViews(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	ctx := t.Context()

	all, err := c.ListManagedViews(ctx, &sdk.ListManagedViewsInput{})
	require.NoError(t, err)
	require.Len(t, all.ManagedViews, 2)

	filtered, err := c.ListManagedViews(
		ctx,
		&sdk.ListManagedViewsInput{ServicePrincipal: aws.String("ssm.amazonaws.com")},
	)
	require.NoError(t, err)
	require.Len(t, filtered.ManagedViews, 1)

	got, err := c.GetManagedView(ctx, &sdk.GetManagedViewInput{ManagedViewArn: aws.String(filtered.ManagedViews[0])})
	require.NoError(t, err)
	assert.Equal(t, "ssm.amazonaws.com", aws.ToString(got.ManagedView.TrustedService))
	assert.Equal(t, filtered.ManagedViews[0], aws.ToString(got.ManagedView.ManagedViewArn))
	assert.Contains(t, aws.ToString(got.ManagedView.ResourcePolicy), "resource-explorer-2:Search")

	_, err = c.GetManagedView(
		ctx,
		&sdk.GetManagedViewInput{
			ManagedViewArn: aws.String("arn:aws:resource-explorer-2:us-east-1:aws:managed-view/none/1"),
		},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestAccountLevelServiceConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus rtypes.AWSServiceAccessStatus
		enabled    bool
		wantRole   bool
	}{
		{name: "disabled", wantStatus: rtypes.AWSServiceAccessStatusDisabled},
		{name: "enabled", enabled: true, wantStatus: rtypes.AWSServiceAccessStatusEnabled, wantRole: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetOrgAccess(fakeOrg{enabled: tc.enabled})

			out, err := f.client(t).
				GetAccountLevelServiceConfiguration(t.Context(), &sdk.GetAccountLevelServiceConfigurationInput{})
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, out.OrgConfiguration.AWSServiceAccessStatus)
			assert.Equal(t, tc.wantRole, out.OrgConfiguration.ServiceLinkedRole != nil)
		})
	}
}

func TestListIndexesForMembers(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	mine := f.client(t)
	other := f.clientIn(t, "us-east-1", "111111111111")

	_, err := mine.CreateIndex(t.Context(), &sdk.CreateIndexInput{})
	require.NoError(t, err)

	_, err = other.CreateIndex(t.Context(), &sdk.CreateIndexInput{})
	require.NoError(t, err)

	tests := []struct {
		name     string
		wantCode string
		accounts []string
		want     int
	}{
		{name: "one_member", accounts: []string{"111111111111"}, want: 1},
		{name: "two_members", accounts: []string{"111111111111", testAccount}, want: 2},
		{name: "unknown_member", accounts: []string{"222222222222"}, want: 0},
		{name: "empty_list", accounts: []string{}, wantCode: "ValidationException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, lerr := mine.ListIndexesForMembers(
				t.Context(),
				&sdk.ListIndexesForMembersInput{AccountIdList: tc.accounts},
			)
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, apiCode(lerr))

				return
			}

			require.NoError(t, lerr)
			assert.Len(t, out.Indexes, tc.want)
		})
	}
}

func TestResourceExplorerSetup(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetLifecycleDelay(time.Minute)

	c := f.client(t)
	ctx := t.Context()

	task, err := c.CreateResourceExplorerSetup(ctx, &sdk.CreateResourceExplorerSetupInput{
		RegionList: []string{
			"us-east-1",
			"us-west-2",
		},
		AggregatorRegions: []string{"us-east-1"},
		ViewName:          aws.String("setup-view"),
	})
	require.NoError(t, err)

	status, err := c.GetResourceExplorerSetup(ctx, &sdk.GetResourceExplorerSetupInput{TaskId: task.TaskId})
	require.NoError(t, err)
	require.Len(t, status.Regions, 2)

	for _, r := range status.Regions {
		assert.Equal(t, rtypes.OperationStatusInProgress, r.Index.Status, "index still CREATING")
		assert.Equal(t, rtypes.OperationStatusSucceeded, r.View.Status)
	}

	f.clock.advance(2 * time.Minute)

	status, err = c.GetResourceExplorerSetup(ctx, &sdk.GetResourceExplorerSetupInput{TaskId: task.TaskId})
	require.NoError(t, err)

	byRegion := map[string]rtypes.RegionStatus{}
	for _, r := range status.Regions {
		byRegion[aws.ToString(r.Region)] = r
	}

	assert.Equal(t, rtypes.OperationStatusSucceeded, byRegion["us-east-1"].Index.Status)
	assert.Equal(t, rtypes.IndexTypeAggregator, byRegion["us-east-1"].Index.Index.Type)
	assert.Equal(t, rtypes.IndexTypeLocal, byRegion["us-west-2"].Index.Index.Type)
	assert.Equal(t, "setup-view", aws.ToString(byRegion["us-west-2"].View.View.ViewName))

	def, err := c.GetDefaultView(ctx, &sdk.GetDefaultViewInput{})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(byRegion["us-east-1"].View.View.ViewArn), aws.ToString(def.ViewArn))

	again, err := c.CreateResourceExplorerSetup(
		ctx,
		&sdk.CreateResourceExplorerSetupInput{RegionList: []string{"us-east-1"}, ViewName: aws.String("again")},
	)
	require.NoError(t, err)

	st2, err := c.GetResourceExplorerSetup(ctx, &sdk.GetResourceExplorerSetupInput{TaskId: again.TaskId})
	require.NoError(t, err)
	assert.Equal(t, rtypes.OperationStatusSkipped, st2.Regions[0].Index.Status, "index already exists")

	del, err := c.DeleteResourceExplorerSetup(
		ctx,
		&sdk.DeleteResourceExplorerSetupInput{DeleteInAllRegions: aws.Bool(true)},
	)
	require.NoError(t, err)

	st3, err := c.GetResourceExplorerSetup(ctx, &sdk.GetResourceExplorerSetupInput{TaskId: del.TaskId})
	require.NoError(t, err)
	require.Len(t, st3.Regions, 2)
	assert.Equal(t, rtypes.OperationStatusInProgress, st3.Regions[0].Index.Status)

	f.clock.advance(2 * time.Minute)

	st3, err = c.GetResourceExplorerSetup(ctx, &sdk.GetResourceExplorerSetupInput{TaskId: del.TaskId})
	require.NoError(t, err)
	assert.Equal(t, rtypes.OperationStatusSucceeded, st3.Regions[0].Index.Status)

	list, err := c.ListIndexes(ctx, &sdk.ListIndexesInput{})
	require.NoError(t, err)
	assert.Empty(t, list.Indexes)
}

func TestResourceExplorerSetupValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   sdk.CreateResourceExplorerSetupInput
	}{
		{
			name: "no_regions",
			in:   sdk.CreateResourceExplorerSetupInput{RegionList: []string{}, ViewName: aws.String("v")},
		},
		{
			name: "bad_region",
			in:   sdk.CreateResourceExplorerSetupInput{RegionList: []string{"nowhere"}, ViewName: aws.String("v")},
		},
		{
			name: "aggregator_not_listed",
			in: sdk.CreateResourceExplorerSetupInput{
				RegionList: []string{"us-east-1"}, AggregatorRegions: []string{"us-west-2"}, ViewName: aws.String("v"),
			},
		},
		{
			name: "two_aggregators",
			in: sdk.CreateResourceExplorerSetupInput{
				RegionList: []string{"us-east-1", "us-west-2"}, AggregatorRegions: []string{"us-east-1", "us-west-2"},
				ViewName: aws.String("v"),
			},
		},
		{
			name: "bad_view",
			in: sdk.CreateResourceExplorerSetupInput{
				RegionList: []string{"us-east-1"},
				ViewName:   aws.String("bad name"),
			},
		},
	}

	c := newFixture(t).client(t)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := c.CreateResourceExplorerSetup(t.Context(), &tc.in)
			assert.Equal(t, "ValidationException", apiCode(err))
		})
	}

	_, err := c.GetResourceExplorerSetup(t.Context(), &sdk.GetResourceExplorerSetupInput{TaskId: aws.String("missing")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	ctx := t.Context()

	idx, err := c.CreateIndex(ctx, &sdk.CreateIndexInput{Tags: map[string]string{"k": "v"}})
	require.NoError(t, err)

	view := createView(t, c, "persisted", "service:s3", "tags")

	snap := f.backend.Snapshot(ctx)
	require.NotEmpty(t, snap)

	restored := resourceexplorer2.NewInMemoryBackend("000000000000", "us-east-1")
	restored.SetResourceSource(defaultResources())
	require.NoError(t, restored.Restore(ctx, snap))

	r := &fixture{backend: restored, clock: f.clock, url: newServedURL(t, restored)}
	rc := r.client(t)

	got, err := rc.GetIndex(ctx, &sdk.GetIndexInput{})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(idx.Arn), aws.ToString(got.Arn))
	assert.Equal(t, map[string]string{"k": "v"}, got.Tags)

	out, err := rc.Search(ctx, &sdk.SearchInput{QueryString: aws.String("")})
	require.NoError(t, err)
	assert.Equal(t, view, aws.ToString(out.ViewArn), "default view survives")
	assert.Len(t, out.Resources, 1)
}

func TestUnroutedOperations(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)

	tests := []struct {
		call func() error
		name string
	}{
		{name: "get_service_index", call: func() error {
			_, err := c.GetServiceIndex(t.Context(), &sdk.GetServiceIndexInput{})

			return err
		}},
		{name: "list_service_views", call: func() error {
			_, err := c.ListServiceViews(t.Context(), &sdk.ListServiceViewsInput{})

			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tc.call())
		})
	}
}

func TestResetClearsState(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)

	setupViewed(t, c)
	f.backend.Reset()

	_, err := c.GetIndex(t.Context(), &sdk.GetIndexInput{})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
