package kinesisanalyticsv2_test

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisanalyticsv2sdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2"
	kav2types "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func TestLifecycleDelay_TransitionalStatuses(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		b := kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1")
		b.SetLifecycleDelay(time.Minute)

		_, err := b.CreateApplication(ctx, "lc-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		status := func() string {
			app, descErr := b.DescribeApplication(ctx, "lc-app")
			require.NoError(t, descErr)

			return app.ApplicationStatus
		}

		_, err = b.StartApplication(ctx, "lc-app", nil, nil)
		require.NoError(t, err)
		assert.Equal(t, "STARTING", status())

		_, err = b.StopApplication(ctx, "lc-app", false)
		require.Error(t, err, "cannot stop while STARTING")

		time.Sleep(time.Minute)
		assert.Equal(t, "RUNNING", status())

		_, err = b.StopApplication(ctx, "lc-app", false)
		require.NoError(t, err)
		assert.Equal(t, "STOPPING", status())

		_, err = b.StartApplication(ctx, "lc-app", nil, nil)
		require.Error(t, err, "cannot start while STOPPING")

		time.Sleep(time.Minute)
		assert.Equal(t, "READY", status())
	})
}

func TestCreateApplication_InputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		runtime kav2types.RuntimeEnvironment
		wantErr bool
	}{
		{name: "valid", appName: "ok.app_1-x", runtime: kav2types.RuntimeEnvironmentFlink118},
		{name: "space in name", appName: "bad name", runtime: kav2types.RuntimeEnvironmentFlink118, wantErr: true},
		{
			name:    "name too long",
			appName: strings.Repeat("a", 129),
			runtime: kav2types.RuntimeEnvironmentFlink118,
			wantErr: true,
		},
		{name: "unknown runtime", appName: "rt-app", runtime: "BOGUS-1_0", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := kinesisanalyticsv2.NewHandler(kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1"))
			client := newTestKAV2SDKClient(t, h)

			_, err := client.CreateApplication(t.Context(), &kinesisanalyticsv2sdk.CreateApplicationInput{
				ApplicationName:      aws.String(tt.appName),
				RuntimeEnvironment:   tt.runtime,
				ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/r"),
			})
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var invalid *kav2types.InvalidArgumentException
			require.ErrorAs(t, err, &invalid)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.NotContains(t, apiErr.ErrorMessage(), "InvalidArgumentException")
		})
	}
}
