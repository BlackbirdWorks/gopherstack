package scheduler_test

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

func TestRunnerClassicInspectorTargetLogsWouldStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want bool
	}{
		{
			name: "classic template",
			arn:  "arn:aws:inspector:us-east-1:000000000000:target/0-AbCdEfGh/template/0-IjKlMnOp",
			want: true,
		},
		{name: "inspector2 is not the classic target", arn: "arn:aws:inspector2:us-east-1:000000000000:x", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newRunnerTestBackend(t)
			_, err := backend.CreateSchedule(
				t.Context(), "insp", "", "rate(1 minute)", "", "",
				scheduler.Target{ARN: tt.arn, RoleARN: "arn:aws:iam::000000000000:role/r"},
				"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
			)
			require.NoError(t, err)

			var buf bytes.Buffer

			ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, nil)))
			scheduler.CheckAndFireSchedules(ctx, scheduler.NewRunner(backend), time.Now().Add(time.Hour))

			if tt.want {
				assert.Contains(t, buf.String(), "would start Inspector assessment run")
				assert.Contains(t, buf.String(), "template")

				return
			}

			assert.NotContains(t, buf.String(), "would start Inspector")
		})
	}
}
