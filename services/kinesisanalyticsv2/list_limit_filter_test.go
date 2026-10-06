package kinesisanalyticsv2_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisanalyticsv2sdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2"
	kav2types "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

// ListApplications.Limit (api_op_ListApplications.go:35) and
// ListApplicationVersions.Limit (api_op_ListApplicationVersions.go:43).
func TestListApplications_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		limit    *int32
		name     string
		wantLen  int
		wantNext bool
	}{
		{name: "limit 2", limit: aws.Int32(2), wantLen: 2, wantNext: true},
		{name: "limit above total", limit: aws.Int32(10), wantLen: 5},
		{name: "no limit", wantLen: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := kinesisanalyticsv2.NewInMemoryBackend(kav2RTAccountID, kav2RTRegion)
			client := newTestKAV2SDKClient(t, kinesisanalyticsv2.NewHandler(backend))
			ctx := t.Context()

			for i := range 5 {
				_, err := client.CreateApplication(ctx, &kinesisanalyticsv2sdk.CreateApplicationInput{
					ApplicationName:      aws.String(fmt.Sprintf("app-%d", i)),
					RuntimeEnvironment:   kav2types.RuntimeEnvironmentFlink118,
					ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/r"),
				})
				require.NoError(t, err)
			}

			out, err := client.ListApplications(ctx, &kinesisanalyticsv2sdk.ListApplicationsInput{Limit: tt.limit})
			require.NoError(t, err)
			assert.Len(t, out.ApplicationSummaries, tt.wantLen)
			assert.Equal(t, tt.wantNext, out.NextToken != nil)
		})
	}
}

// ListApplicationOperations Operation / OperationStatus filters and Limit.
func TestListApplicationOperations_FilterAndLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   kinesisanalyticsv2sdk.ListApplicationOperationsInput
		wantOps []string
	}{
		{name: "all", wantOps: []string{"StartApplication", "StopApplication", "StartApplication"}},
		{
			name:    "operation filter",
			input:   kinesisanalyticsv2sdk.ListApplicationOperationsInput{Operation: aws.String("StartApplication")},
			wantOps: []string{"StartApplication", "StartApplication"},
		},
		{
			name:    "operation filter no match",
			input:   kinesisanalyticsv2sdk.ListApplicationOperationsInput{Operation: aws.String("RollbackApplication")},
			wantOps: []string{},
		},
		{
			name: "status filter no match",
			input: kinesisanalyticsv2sdk.ListApplicationOperationsInput{
				OperationStatus: kav2types.OperationStatusFailed,
			},
			wantOps: []string{},
		},
		{
			name:    "limit",
			input:   kinesisanalyticsv2sdk.ListApplicationOperationsInput{Limit: aws.Int32(2)},
			wantOps: []string{"StartApplication", "StopApplication"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := kinesisanalyticsv2.NewInMemoryBackend(kav2RTAccountID, kav2RTRegion)
			client := newTestKAV2SDKClient(t, kinesisanalyticsv2.NewHandler(backend))
			ctx := t.Context()

			_, err := client.CreateApplication(ctx, &kinesisanalyticsv2sdk.CreateApplicationInput{
				ApplicationName:      aws.String("ops-app"),
				RuntimeEnvironment:   kav2types.RuntimeEnvironmentFlink118,
				ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/r"),
			})
			require.NoError(t, err)

			for _, op := range []string{"start", "stop", "start"} {
				if op == "start" {
					_, err = client.StartApplication(
						ctx,
						&kinesisanalyticsv2sdk.StartApplicationInput{ApplicationName: aws.String("ops-app")},
					)
				} else {
					_, err = client.StopApplication(
						ctx,
						&kinesisanalyticsv2sdk.StopApplicationInput{ApplicationName: aws.String("ops-app")},
					)
				}

				require.NoError(t, err)
			}

			in := tt.input
			in.ApplicationName = aws.String("ops-app")

			out, err := client.ListApplicationOperations(ctx, &in)
			require.NoError(t, err)

			got := make([]string, 0, len(out.ApplicationOperationInfoList))
			for _, o := range out.ApplicationOperationInfoList {
				got = append(got, aws.ToString(o.Operation))
			}

			assert.Equal(t, tt.wantOps, got)
		})
	}
}
