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
