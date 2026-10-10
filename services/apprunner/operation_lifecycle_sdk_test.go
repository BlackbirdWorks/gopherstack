package apprunner_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	apprunnersdk "github.com/aws/aws-sdk-go-v2/service/apprunner"
	"github.com/aws/aws-sdk-go-v2/service/apprunner/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

func TestOperationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus types.ServiceStatus
		wantOp     types.OperationStatus
		delay      time.Duration
		advance    time.Duration
		wantUpdate bool
	}{
		{
			name:       "instant",
			wantStatus: types.ServiceStatusRunning,
			wantOp:     types.OperationStatusSucceeded,
			wantUpdate: true,
		},
		{
			name: "in progress", delay: time.Minute, advance: 30 * time.Second,
			wantStatus: types.ServiceStatusOperationInProgress, wantOp: types.OperationStatusInProgress,
		},
		{
			name: "settled", delay: time.Minute, advance: time.Minute,
			wantStatus: types.ServiceStatusRunning, wantOp: types.OperationStatusSucceeded, wantUpdate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Unix(1_700_000_000, 0)
			backend := apprunner.NewInMemoryBackend("000000000000", "us-east-1")
			backend.SetClock(func() time.Time { return now })
			backend.SetOperationDelay(tt.delay)
			c := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
			ctx := t.Context()

			created, err := c.CreateService(ctx, &apprunnersdk.CreateServiceInput{
				ServiceName: aws.String("svc-one"),
				SourceConfiguration: &types.SourceConfiguration{ImageRepository: &types.ImageRepository{
					ImageIdentifier: aws.String(
						"public.ecr.aws/x/y:1",
					),
					ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
				}},
			})
			require.NoError(t, err)

			now = now.Add(tt.advance)

			desc, err := c.DescribeService(
				ctx,
				&apprunnersdk.DescribeServiceInput{ServiceArn: created.Service.ServiceArn},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, desc.Service.Status)

			list, err := c.ListServices(ctx, &apprunnersdk.ListServicesInput{})
			require.NoError(t, err)
			require.Len(t, list.ServiceSummaryList, 1)
			assert.Equal(t, tt.wantStatus, list.ServiceSummaryList[0].Status)

			ops, err := c.ListOperations(ctx, &apprunnersdk.ListOperationsInput{ServiceArn: created.Service.ServiceArn})
			require.NoError(t, err)
			require.Len(t, ops.OperationSummaryList, 1)
			assert.Equal(t, tt.wantOp, ops.OperationSummaryList[0].Status)

			_, err = c.PauseService(ctx, &apprunnersdk.PauseServiceInput{ServiceArn: created.Service.ServiceArn})
			if tt.wantUpdate {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidStateException", apiErr.ErrorCode())
			assert.False(t, strings.HasSuffix(apiErr.ErrorMessage(), "InvalidStateException"), apiErr.ErrorMessage())
		})
	}
}

func TestNotFoundMessage(t *testing.T) {
	t.Parallel()

	c := newTestAppRunnerClient(t, newTestHandler(t))
	_, err := c.DescribeService(t.Context(), &apprunnersdk.DescribeServiceInput{
		ServiceArn: aws.String("arn:aws:apprunner:us-east-1:000000000000:service/nope"),
	})

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
	assert.NotContains(t, apiErr.ErrorMessage(), "ResourceNotFoundException")
}

func TestCreateService_NameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		svcName string
		wantErr bool
	}{
		{name: "valid", svcName: "my_svc-1"},
		{name: "too short", svcName: "abc", wantErr: true},
		{name: "too long", svcName: strings.Repeat("a", 41), wantErr: true},
		{name: "bad char", svcName: "bad.name", wantErr: true},
		{name: "leading dash", svcName: "-bad-name", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestAppRunnerClient(t, newTestHandler(t))
			_, err := c.CreateService(t.Context(), &apprunnersdk.CreateServiceInput{
				ServiceName: aws.String(tt.svcName),
				SourceConfiguration: &types.SourceConfiguration{ImageRepository: &types.ImageRepository{
					ImageIdentifier: aws.String(
						"public.ecr.aws/x/y:1",
					),
					ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
				}},
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidRequestException", apiErr.ErrorCode())
		})
	}
}
