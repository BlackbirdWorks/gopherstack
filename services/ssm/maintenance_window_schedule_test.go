package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestMaintenanceWindow_ScheduleExpressionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		schedule string
		wantErr  bool
	}{
		{name: "cron", schedule: "cron(0 2 ? * * *)"},
		{name: "rate", schedule: "rate(1 hour)"},
		{name: "at", schedule: "at(2030-01-01T00:00:00)"},
		{name: "garbage", schedule: "garbage", wantErr: true},
		{name: "unterminated", schedule: "cron(0 2 ? * * *", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))

			out, err := client.CreateMaintenanceWindow(t.Context(), &ssmsdk.CreateMaintenanceWindowInput{
				Name: aws.String("mw1"), Schedule: aws.String(tt.schedule),
				Duration: aws.Int32(3), Cutoff: 1, AllowUnassociatedTargets: true,
			})
			if tt.wantErr {
				require.ErrorContains(t, err, "ValidationException")

				return
			}

			require.NoError(t, err)

			_, err = client.UpdateMaintenanceWindow(t.Context(), &ssmsdk.UpdateMaintenanceWindowInput{
				WindowId: out.WindowId, Schedule: aws.String("nonsense"),
			})
			require.ErrorContains(t, err, "ValidationException")
		})
	}
}
