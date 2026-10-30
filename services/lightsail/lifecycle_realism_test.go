package lightsail_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lightsail"
)

func newDelayedClient(t *testing.T, d time.Duration) *lightsailsdk.Client {
	t.Helper()

	backend := lightsail.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)
	backend.SetTransitionDelay(d)

	return newRoundTripClient(t, lightsail.NewHandler(backend))
}

func createInstance(t *testing.T, c *lightsailsdk.Client, name string) {
	t.Helper()

	_, err := c.CreateInstances(t.Context(), &lightsailsdk.CreateInstancesInput{
		AvailabilityZone: aws.String("us-east-1a"), BlueprintId: aws.String("amazon_linux_2023"),
		BundleId: aws.String("nano_3_0"), InstanceNames: []string{name},
	})
	require.NoError(t, err)
}

func instanceStateName(t *testing.T, c *lightsailsdk.Client, name string) string {
	t.Helper()

	out, err := c.GetInstanceState(t.Context(), &lightsailsdk.GetInstanceStateInput{InstanceName: aws.String(name)})
	require.NoError(t, err)

	return aws.ToString(out.State.Name)
}

func TestInstanceStopStartTransitions(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := newDelayedClient(t, time.Hour)
		createInstance(t, c, "web-1")
		assert.Equal(t, "pending", instanceStateName(t, c, "web-1"))

		_, err := c.StopInstance(t.Context(), &lightsailsdk.StopInstanceInput{InstanceName: aws.String("web-1")})

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidInputException", apiErr.ErrorCode())

		fast := newDelayedClient(t, time.Millisecond)
		createInstance(t, fast, "web-2")
		require.Eventually(t, func() bool { return instanceStateName(t, fast, "web-2") == "running" },
			defaultAsyncWait, defaultAsyncPoll)

		_, err = fast.StartInstance(t.Context(), &lightsailsdk.StartInstanceInput{InstanceName: aws.String("web-2")})
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidInputException", apiErr.ErrorCode())
	})
}

func TestStopShowsStoppingFirst(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		backend := lightsail.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
		t.Cleanup(backend.Close)
		backend.SetTransitionDelay(time.Millisecond)
		c := newRoundTripClient(t, lightsail.NewHandler(backend))

		createInstance(t, c, "web-3")
		require.Eventually(t, func() bool { return instanceStateName(t, c, "web-3") == "running" },
			defaultAsyncWait, defaultAsyncPoll)

		backend.SetTransitionDelay(time.Hour)

		_, err := c.StopInstance(t.Context(), &lightsailsdk.StopInstanceInput{InstanceName: aws.String("web-3")})
		require.NoError(t, err)
		assert.Equal(t, "stopping", instanceStateName(t, c, "web-3"))
	})
}

func TestCreateInstances_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		names  []string
		az     string
		reuse  []string
		wantOK bool
	}{
		{name: "valid", names: []string{"good-name"}, az: "us-east-1a", wantOK: true},
		{name: "bad characters", names: []string{"bad name"}, az: "us-east-1a"},
		{name: "single char", names: []string{"x"}, az: "us-east-1a"},
		{name: "foreign zone", names: []string{"good-name"}, az: "eu-west-1a"},
		{
			name:   "duplicate in request releases names",
			names:  []string{"dup", "other", "dup"},
			az:     "us-east-1a",
			reuse:  []string{"other"},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestClient(t)
			_, err := c.CreateInstances(t.Context(), &lightsailsdk.CreateInstancesInput{
				AvailabilityZone: aws.String(tt.az), BlueprintId: aws.String("amazon_linux_2023"),
				BundleId: aws.String("nano_3_0"), InstanceNames: tt.names,
			})

			if tt.wantOK {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidInputException", apiErr.ErrorCode())

			for _, n := range tt.reuse {
				createInstance(t, c, n)
			}
		})
	}
}

func TestGetInstances_BadPageToken(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	_, err := c.GetInstances(t.Context(), &lightsailsdk.GetInstancesInput{PageToken: aws.String("!!garbage")})

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidInputException", apiErr.ErrorCode())
	assert.Equal(t, "Invalid pageToken", apiErr.ErrorMessage())
}

func TestCreateBucket_ReturnsBucketAndValidatesName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		bucket  string
		wantErr bool
	}{
		{name: "valid", bucket: "my-bucket-1"},
		{name: "uppercase", bucket: "My-Bucket", wantErr: true},
		{name: "trailing hyphen", bucket: "bucket-", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestClient(t)
			out, err := c.CreateBucket(t.Context(), &lightsailsdk.CreateBucketInput{
				BucketName: aws.String(tt.bucket), BundleId: aws.String("small_1_0"),
			})

			if tt.wantErr {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "InvalidInputException", apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.Bucket)
			assert.Equal(t, tt.bucket, aws.ToString(out.Bucket.Name))
			assert.NotEmpty(t, out.Operations)
		})
	}
}
