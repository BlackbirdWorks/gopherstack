package appstream_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestCreateUser_UserNameIsEmailRealClient covers gopherstack-wksweep-as-1:
// real CreateUserInput (appstream@v1.64.5 api_op_CreateUser.go) has no Email
// member at all -- UserName IS documented as "The email address of the
// user", so the Go SDK struct structurally cannot carry a separate Email
// field. This proves the real, sole identity member (UserName) round-trips
// end to end through DescribeUsers.
func TestCreateUser_UserNameIsEmailRealClient(t *testing.T) {
	t.Parallel()

	backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestAppStreamClient(t, appstream.NewHandler(backend))
	ctx := t.Context()

	const email = "alice@example.com"

	_, err := client.CreateUser(ctx, &appstreamsdk.CreateUserInput{
		UserName:           aws.String(email),
		AuthenticationType: types.AuthenticationTypeUserpool,
	})
	require.NoError(t, err)

	desc, err := client.DescribeUsers(ctx, &appstreamsdk.DescribeUsersInput{
		AuthenticationType: types.AuthenticationTypeUserpool,
	})
	require.NoError(t, err)
	require.Len(t, desc.Users, 1)
	assert.Equal(t, email, aws.ToString(desc.Users[0].UserName),
		"UserName is the user's email address on real AppStream; it must round-trip unchanged")
}

// TestCreateUsageReportSubscription_NoInputRealClient covers
// gopherstack-wksweep-as-2: real CreateUsageReportSubscriptionInput
// (appstream@v1.64.5 api_op_CreateUsageReportSubscription.go) takes no
// parameters at all -- the Go SDK struct is empty, so a real client
// structurally cannot supply S3BucketName/Schedule. Before the fix,
// gopherstack read those from a fabricated request struct that a real
// client's marshaled (empty) body could never populate, so the returned
// S3BucketName was always empty; AWS actually derives both server-side.
func TestCreateUsageReportSubscription_NoInputRealClient(t *testing.T) {
	t.Parallel()

	backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestAppStreamClient(t, appstream.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateUsageReportSubscription(ctx, &appstreamsdk.CreateUsageReportSubscriptionInput{})
	require.NoError(t, err)
	assert.Equal(t, types.UsageReportScheduleDaily, created.Schedule)
	assert.NotEmpty(t, aws.ToString(created.S3BucketName),
		"S3BucketName must be derived server-side; pre-fix a real client always got back empty")

	desc, err := client.DescribeUsageReportSubscriptions(ctx, &appstreamsdk.DescribeUsageReportSubscriptionsInput{})
	require.NoError(t, err)
	require.Len(t, desc.UsageReportSubscriptions, 1)
	assert.Equal(t, aws.ToString(created.S3BucketName), aws.ToString(desc.UsageReportSubscriptions[0].S3BucketName))
	assert.Equal(t, types.UsageReportScheduleDaily, desc.UsageReportSubscriptions[0].Schedule)
}

// TestDescribeImages_TypeFilterRealClient covers wrapper-key-sweep-appstream-1:
// real DescribeImagesInput (appstream@v1.64.5 api_op_DescribeImages.go) carries
// a Type field (types.VisibilityType, wire key "Type" -- confirmed against
// serializeCBOR_DescribeImagesInput in the pinned SDK's serializers.go) that
// gopherstack's handler never read at all. Every image this backend creates
// is Visibility "PRIVATE" (images.go), so filtering by Type=PUBLIC must return
// an empty list; before the fix the dropped filter meant a PUBLIC-only request
// got back every private image instead.
func TestDescribeImages_TypeFilterRealClient(t *testing.T) {
	t.Parallel()

	backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestAppStreamClient(t, appstream.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateImportedImage(ctx, &appstreamsdk.CreateImportedImageInput{
		Name:        aws.String("my-private-image"),
		SourceAmiId: aws.String("ami-0123456789abcdef0"),
		IamRoleArn:  aws.String("arn:aws:iam::000000000000:role/import"),
	})
	require.NoError(t, err)

	priv, err := client.DescribeImages(ctx, &appstreamsdk.DescribeImagesInput{
		Type: types.VisibilityTypePrivate,
	})
	require.NoError(t, err)
	assert.Len(t, priv.Images, 1, "Type=PRIVATE must return the private image")

	pub, err := client.DescribeImages(ctx, &appstreamsdk.DescribeImagesInput{
		Type: types.VisibilityTypePublic,
	})
	require.NoError(t, err)
	assert.Empty(t, pub.Images,
		"Type=PUBLIC must return no images -- this backend never creates any; "+
			"pre-fix the Type filter was dropped and every private image came back instead")
}

// TestDescribeSessions_AuthenticationTypeFilterRealClient covers
// wrapper-key-sweep-appstream-2: real DescribeSessionsInput
// (appstream@v1.64.5 api_op_DescribeSessions.go) carries an
// AuthenticationType field (wire key "AuthenticationType") that
// gopherstack's handler never read at all. Every session this backend
// creates (CreateStreamingURL) has AuthenticationType "API"; before the
// fix a USERPOOL-filtered request got back every API session instead of
// an empty list.
func TestDescribeSessions_AuthenticationTypeFilterRealClient(t *testing.T) {
	t.Parallel()

	backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestAppStreamClient(t, appstream.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateStack(ctx, &appstreamsdk.CreateStackInput{Name: aws.String("stack1")})
	require.NoError(t, err)
	_, err = client.CreateFleet(ctx, &appstreamsdk.CreateFleetInput{
		Name:         aws.String("fleet1"),
		InstanceType: aws.String("stream.standard.medium"),
		ImageName:    aws.String("some-image"),
	})
	require.NoError(t, err)

	_, err = client.CreateStreamingURL(ctx, &appstreamsdk.CreateStreamingURLInput{
		StackName: aws.String("stack1"),
		FleetName: aws.String("fleet1"),
		UserId:    aws.String("user1"),
	})
	require.NoError(t, err)

	api, err := client.DescribeSessions(ctx, &appstreamsdk.DescribeSessionsInput{
		StackName:          aws.String("stack1"),
		FleetName:          aws.String("fleet1"),
		AuthenticationType: types.AuthenticationTypeApi,
	})
	require.NoError(t, err)
	assert.Len(t, api.Sessions, 1, "AuthenticationType=API must return the API session")

	userpool, err := client.DescribeSessions(ctx, &appstreamsdk.DescribeSessionsInput{
		StackName:          aws.String("stack1"),
		FleetName:          aws.String("fleet1"),
		AuthenticationType: types.AuthenticationTypeUserpool,
	})
	require.NoError(t, err)
	assert.Empty(t, userpool.Sessions,
		"AuthenticationType=USERPOOL must return no sessions -- this backend only ever creates API "+
			"sessions; pre-fix the filter was dropped and the API session came back instead")
}

// TestDescribeImagePermissions_SharedAwsAccountIdsFilterRealClient covers
// wrapper-key-sweep-appstream-3: real DescribeImagePermissionsInput
// (appstream@v1.64.5 api_op_DescribeImagePermissions.go) carries a
// SharedAwsAccountIds field (wire key "SharedAwsAccountIds") that
// gopherstack's handler never read at all. Before the fix, filtering by an
// account the image was never shared with returned every shared account
// instead of an empty list.
func TestDescribeImagePermissions_SharedAwsAccountIdsFilterRealClient(t *testing.T) {
	t.Parallel()

	backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestAppStreamClient(t, appstream.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateImportedImage(ctx, &appstreamsdk.CreateImportedImageInput{
		Name:        aws.String("my-image"),
		SourceAmiId: aws.String("ami-0123456789abcdef0"),
		IamRoleArn:  aws.String("arn:aws:iam::000000000000:role/import"),
	})
	require.NoError(t, err)

	_, err = client.UpdateImagePermissions(ctx, &appstreamsdk.UpdateImagePermissionsInput{
		Name:            aws.String("my-image"),
		SharedAccountId: aws.String("111111111111"),
		ImagePermissions: &types.ImagePermissions{
			AllowFleet:        aws.Bool(true),
			AllowImageBuilder: aws.Bool(false),
		},
	})
	require.NoError(t, err)

	matching, err := client.DescribeImagePermissions(ctx, &appstreamsdk.DescribeImagePermissionsInput{
		Name:                aws.String("my-image"),
		SharedAwsAccountIds: []string{"111111111111"},
	})
	require.NoError(t, err)
	assert.Len(t, matching.SharedImagePermissionsList, 1, "filtering by the account it IS shared with must return it")

	nonMatching, err := client.DescribeImagePermissions(ctx, &appstreamsdk.DescribeImagePermissionsInput{
		Name:                aws.String("my-image"),
		SharedAwsAccountIds: []string{"222222222222"},
	})
	require.NoError(t, err)
	assert.Empty(t, nonMatching.SharedImagePermissionsList,
		"filtering by an account the image was never shared with must return no results -- "+
			"pre-fix the SharedAwsAccountIds filter was dropped and every shared account came back instead")
}
