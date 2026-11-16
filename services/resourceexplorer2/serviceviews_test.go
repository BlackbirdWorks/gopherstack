package resourceexplorer2_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourceexplorer2"
)

func TestServiceViews(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)

	viewARN, err := f.backend.PutServiceView(resourceexplorer2.ServiceView{
		Name:         "obs-view",
		AccountID:    testAccount,
		Region:       "us-east-1",
		FilterString: "service:ec2",
		IncludedProperties: []string{
			"tags",
		},
		ScopeType:         "ACCOUNT",
		StreamingService:  "observabilityadmin.amazonaws.com",
		RecorderName:      "rec",
		RecorderType:      "AWS",
		RecorderPrincipal: "observabilityadmin.amazonaws.com",
	})
	require.NoError(t, err)

	_, err = f.backend.PutServiceView(resourceexplorer2.ServiceView{
		Name: "west", AccountID: testAccount, Region: "us-west-2",
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		arn      string
		wantCode string
	}{
		{name: "found", arn: viewARN},
		{name: "unknown", arn: viewARN + "x", wantCode: "ResourceNotFoundException"},
		{
			name:     "other_region",
			arn:      "arn:aws:resource-explorer-2:us-west-2:" + testAccount + ":service-view/west",
			wantCode: "ResourceNotFoundException",
		},
		{name: "not_arn", arn: "nope", wantCode: "ValidationException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, gerr := c.GetServiceView(t.Context(), &sdk.GetServiceViewInput{ServiceViewArn: aws.String(tc.arn)})
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, apiCode(gerr))

				return
			}

			require.NoError(t, gerr)
			assert.Equal(t, "obs-view", aws.ToString(out.View.ServiceViewName))
			assert.Equal(t, "service:ec2", aws.ToString(out.View.Filters.FilterString))
			assert.Equal(t, "observabilityadmin.amazonaws.com", aws.ToString(out.View.StreamingAccessForService))
			assert.Equal(t, "rec", aws.ToString(out.View.ServiceLinkedRecorder.RecorderName))
			assert.Equal(t, "ACCOUNT", aws.ToString(out.View.ScopeType))
			require.Len(t, out.View.IncludedProperties, 1)
		})
	}

	list, err := c.ListServiceViews(t.Context(), &sdk.ListServiceViewsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{viewARN}, list.ServiceViews)

	other := f.clientIn(t, "us-east-1", "111111111111")

	_, err = other.GetServiceView(t.Context(), &sdk.GetServiceViewInput{ServiceViewArn: aws.String(viewARN)})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	otherList, err := other.ListServiceViews(t.Context(), &sdk.ListServiceViewsInput{})
	require.NoError(t, err)
	assert.Empty(t, otherList.ServiceViews)

	_, err = c.ListServiceViews(t.Context(), &sdk.ListServiceViewsInput{MaxResults: aws.Int32(51)})
	assert.Equal(t, "ValidationException", apiCode(err))
}

func TestServiceViewListPagination(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)

	for i := range 3 {
		_, err := f.backend.PutServiceView(resourceexplorer2.ServiceView{
			Name: fmt.Sprintf("v%d", i), AccountID: testAccount, Region: "us-east-1",
		})
		require.NoError(t, err)
	}

	first, err := c.ListServiceViews(t.Context(), &sdk.ListServiceViewsInput{MaxResults: aws.Int32(2)})
	require.NoError(t, err)
	require.Len(t, first.ServiceViews, 2)
	require.NotNil(t, first.NextToken)

	second, err := c.ListServiceViews(t.Context(), &sdk.ListServiceViewsInput{NextToken: first.NextToken})
	require.NoError(t, err)
	assert.Len(t, second.ServiceViews, 1)
}

func TestServiceViewQuota(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	var err error

	for i := range 51 {
		_, err = f.backend.PutServiceView(resourceexplorer2.ServiceView{
			Name: fmt.Sprintf("v%d", i), AccountID: testAccount, Region: "us-east-1",
		})
	}

	require.Error(t, err)
	assert.Contains(t, err.Error(), "ServiceQuotaExceededException")
}

func TestPutServiceViewRejectsInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		view resourceexplorer2.ServiceView
	}{
		{name: "no_name", view: resourceexplorer2.ServiceView{AccountID: testAccount, Region: "us-east-1"}},
		{name: "keyword_filter", view: resourceexplorer2.ServiceView{
			Name: "x", AccountID: testAccount, Region: "us-east-1", FilterString: "free text",
		}},
		{name: "bad_property", view: resourceexplorer2.ServiceView{
			Name: "x", AccountID: testAccount, Region: "us-east-1", IncludedProperties: []string{"bogus"},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := newFixture(t).backend.PutServiceView(tc.view)
			require.Error(t, err)
		})
	}
}

func TestStreamingAccess(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)

	empty, err := c.ListStreamingAccessForServices(t.Context(), &sdk.ListStreamingAccessForServicesInput{})
	require.NoError(t, err)
	assert.Empty(t, empty.StreamingAccessForServices)

	require.NoError(t, f.backend.GrantStreamingAccess(testAccount, "b.amazonaws.com"))
	require.NoError(t, f.backend.GrantStreamingAccess(testAccount, "a.amazonaws.com"))
	require.NoError(t, f.backend.GrantStreamingAccess(testAccount, "a.amazonaws.com"))
	require.NoError(t, f.backend.GrantStreamingAccess("111111111111", "other.amazonaws.com"))

	out, err := c.ListStreamingAccessForServices(t.Context(), &sdk.ListStreamingAccessForServicesInput{})
	require.NoError(t, err)
	require.Len(t, out.StreamingAccessForServices, 2)
	assert.Equal(t, "a.amazonaws.com", aws.ToString(out.StreamingAccessForServices[0].ServicePrincipal))
	assert.Equal(t, f.clock.t, aws.ToTime(out.StreamingAccessForServices[0].CreatedAt))

	assert.True(t, f.backend.RevokeStreamingAccess(testAccount, "a.amazonaws.com"))
	assert.False(t, f.backend.RevokeStreamingAccess(testAccount, "a.amazonaws.com"))

	require.Error(t, f.backend.GrantStreamingAccess("", "x"))
}

func TestServiceViewSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	viewARN, err := f.backend.PutServiceView(resourceexplorer2.ServiceView{
		Name: "v", AccountID: testAccount, Region: "us-east-1", ScopeType: "ACCOUNT",
	})
	require.NoError(t, err)
	require.NoError(t, f.backend.GrantStreamingAccess(testAccount, "a.amazonaws.com"))

	snap := f.backend.Snapshot(t.Context())

	g := newFixture(t)
	require.NoError(t, g.backend.Restore(t.Context(), snap))

	c := g.client(t)
	got, err := c.GetServiceView(t.Context(), &sdk.GetServiceViewInput{ServiceViewArn: aws.String(viewARN)})
	require.NoError(t, err)
	assert.Equal(t, "v", aws.ToString(got.View.ServiceViewName))

	out, err := c.ListStreamingAccessForServices(t.Context(), &sdk.ListStreamingAccessForServicesInput{})
	require.NoError(t, err)
	assert.Len(t, out.StreamingAccessForServices, 1)
}
