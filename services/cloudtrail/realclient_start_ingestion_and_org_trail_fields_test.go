package cloudtrail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestRealClient_CreateEventDataStoreStartIngestion covers gopherstack-xhu2t:
// CreateEventDataStoreInput.StartIngestion (default true per
// cloudtrail@v1.58.4 api_op_CreateEventDataStore.go:130-132) was undeclared,
// so an event data store created with StartIngestion=false always came up
// ENABLED instead of STOPPED_INGESTION.
func TestRealClient_CreateEventDataStoreStartIngestion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		startIngestion *bool
		wantStatus     cttypes.EventDataStoreStatus
		name           string
	}{
		{name: "explicit_false", startIngestion: aws.Bool(false), wantStatus: "STOPPED_INGESTION"},
		{name: "explicit_true", startIngestion: aws.Bool(true), wantStatus: "ENABLED"},
		{name: "omitted_defaults_true", wantStatus: "ENABLED"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
			client := newTestCloudTrailClient(t, cloudtrail.NewHandler(b))

			out, err := client.CreateEventDataStore(t.Context(), &cloudtrailsdk.CreateEventDataStoreInput{
				Name:           aws.String("slice7-eds-" + tc.name),
				StartIngestion: tc.startIngestion,
			})
			require.NoError(t, err)
			assert.Equal(t, string(tc.wantStatus), string(out.Status))
		})
	}
}

// TestRealClient_TrailIsOrganizationTrail covers gopherstack-xhu2t:
// CreateTrailInput.IsOrganizationTrail and UpdateTrailInput.IsOrganizationTrail
// (cloudtrail@v1.58.4 serializers.go:4510-4512/5688-5690) were undeclared;
// GetTrail/DescribeTrails always reported false regardless of what was sent.
func TestRealClient_TrailIsOrganizationTrail(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
	client := newTestCloudTrailClient(t, cloudtrail.NewHandler(b))

	created, err := client.CreateTrail(t.Context(), &cloudtrailsdk.CreateTrailInput{
		Name:                aws.String("slice7-org-trail"),
		S3BucketName:        aws.String("slice7-bucket"),
		IsOrganizationTrail: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(created.IsOrganizationTrail))

	got, err := client.GetTrail(t.Context(), &cloudtrailsdk.GetTrailInput{Name: aws.String("slice7-org-trail")})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(got.Trail.IsOrganizationTrail))

	updated, err := client.UpdateTrail(t.Context(), &cloudtrailsdk.UpdateTrailInput{
		Name:                aws.String("slice7-org-trail"),
		IsOrganizationTrail: aws.Bool(false),
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(updated.IsOrganizationTrail))

	got, err = client.GetTrail(t.Context(), &cloudtrailsdk.GetTrailInput{Name: aws.String("slice7-org-trail")})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(got.Trail.IsOrganizationTrail))
}
