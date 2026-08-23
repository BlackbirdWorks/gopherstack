package resiliencehub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListAppVersionResources_ResolutionID_RealClient covers gopherstack-r80d
// (required-output-member sweep). ListAppVersionResourcesOutput requires
// ResolutionId (resiliencehub@v1.38.3 api_op_ListAppVersionResources.go:67-70)
// even for a freshly created app version that has never gone through
// ResolveAppVersionResources -- the handler only set ResolutionID when
// AppVersion.Resolution was non-nil, and the wire struct's omitempty tag
// dropped the key entirely when it stayed empty, so a real client's required
// ResolutionId decoded as an empty string it could not distinguish from "the
// key was never sent".
func TestListAppVersionResources_ResolutionID_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	appOut, err := client.CreateApp(
		t.Context(), &resiliencehubsdk.CreateAppInput{Name: aws.String("r80d-unresolved-app")},
	)
	require.NoError(t, err)

	got, err := client.ListAppVersionResources(t.Context(), &resiliencehubsdk.ListAppVersionResourcesInput{
		AppArn:     appOut.App.AppArn,
		AppVersion: aws.String("draft"),
	})
	require.NoError(t, err)
	require.NotNil(
		t, got.ResolutionId, "resolutionId must be present even when empty, per the real required output shape",
	)
	assert.Empty(t, aws.ToString(got.ResolutionId))
}

// TestListUnsupportedAppVersionResources_ResolutionID_RealClient covers
// gopherstack-r80d. Same bug and fix as
// TestListAppVersionResources_ResolutionID_RealClient, for the sibling op
// (resiliencehub@v1.38.3 api_op_ListUnsupportedAppVersionResources.go:62-67).
func TestListUnsupportedAppVersionResources_ResolutionID_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	appOut, err := client.CreateApp(
		t.Context(), &resiliencehubsdk.CreateAppInput{Name: aws.String("r80d-unresolved-app-2")},
	)
	require.NoError(t, err)

	got, err := client.ListUnsupportedAppVersionResources(
		t.Context(), &resiliencehubsdk.ListUnsupportedAppVersionResourcesInput{
			AppArn:     appOut.App.AppArn,
			AppVersion: aws.String("draft"),
		},
	)
	require.NoError(t, err)
	require.NotNil(
		t, got.ResolutionId, "resolutionId must be present even when empty, per the real required output shape",
	)
	assert.Empty(t, aws.ToString(got.ResolutionId))
}
