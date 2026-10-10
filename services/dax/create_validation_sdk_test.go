package dax_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	daxsdk "github.com/aws/aws-sdk-go-v2/service/dax"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_CreateClusterValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		roleARN  string
		window   string
		wantCode string
	}{
		{name: "valid", roleARN: "arn:aws:iam::000000000000:role/dax", window: "sun:05:00-sun:09:00"},
		{name: "wrapping_window", roleARN: "arn:aws:iam::000000000000:role/dax", window: "sat:23:00-sun:03:00"},
		{name: "bad_role", roleARN: "notarn", wantCode: "InvalidARNFault"},
		{name: "user_arn", roleARN: "arn:aws:iam::000000000000:user/u", wantCode: "InvalidARNFault"},
		{
			name:     "junk_window",
			roleARN:  "arn:aws:iam::000000000000:role/dax",
			window:   "junk",
			wantCode: "InvalidParameterValueException",
		},
		{
			name:     "short_window",
			roleARN:  "arn:aws:iam::000000000000:role/dax",
			window:   "sun:05:00-sun:05:30",
			wantCode: "InvalidParameterValueException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDAXSDKClient(t, newTestHandler())
			in := &daxsdk.CreateClusterInput{
				ClusterName:       aws.String("c1"),
				NodeType:          aws.String("dax.r4.large"),
				ReplicationFactor: 1,
				IamRoleArn:        aws.String(tc.roleARN),
			}
			if tc.window != "" {
				in.PreferredMaintenanceWindow = aws.String(tc.window)
			}

			_, err := client.CreateCluster(t.Context(), in)
			if tc.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tc.wantCode, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), tc.wantCode+":")
		})
	}
}

func TestBackend_LifecycleDelayOverride(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := newTestBackend()
		b.SetLifecycleDelay(time.Minute)

		created, err := b.CreateCluster(validCreateInput("slow"))
		require.NoError(t, err)
		assert.Equal(t, "creating", created.Status)

		time.Sleep(30 * time.Second)

		got, _, err := b.DescribeClusters([]string{"slow"}, 0, "")
		require.NoError(t, err)
		assert.Equal(t, "creating", got[0].Status)

		time.Sleep(31 * time.Second)

		got, _, err = b.DescribeClusters([]string{"slow"}, 0, "")
		require.NoError(t, err)
		assert.Equal(t, "available", got[0].Status)
	})
}
