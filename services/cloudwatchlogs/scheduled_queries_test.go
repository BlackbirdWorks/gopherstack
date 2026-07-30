package cloudwatchlogs_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testScheduledQueryParams returns a valid ScheduledQueryCreateParams for
// tests that don't care about the specific field values, just that Create
// succeeds -- ExecutionRoleArn and QueryLanguage are both required by the
// real API (see ScheduledQueryCreateParams's doc comment).
func testScheduledQueryParams(name string) cloudwatchlogs.ScheduledQueryCreateParams {
	return cloudwatchlogs.ScheduledQueryCreateParams{
		Name:               name,
		QueryString:        "fields @message",
		QueryLanguage:      "CWLI",
		ScheduleExpression: "cron(0 * * * ? *)",
		ExecutionRoleArn:   "arn:aws:iam::123456789012:role/scheduled-query-role",
		State:              "ENABLED",
	}
}

func TestCloudWatchLogsBackend_ScheduledQueryLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(b *cloudwatchlogs.InMemoryBackend)
		name     string
		arn      string
		newState string
		op       string
	}{
		{
			name: "create_and_list",
			op:   "list",
		},
		{
			name: "delete_existing",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateScheduledQuery(testScheduledQueryParams("q1"))
			},
			op: "delete_first",
		},
		{
			name:    "delete_missing",
			op:      "delete_direct",
			arn:     "arn:nonexistent",
			wantErr: cloudwatchlogs.ErrScheduledQueryNotFound,
		},
		{
			name: "update_state",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateScheduledQuery(testScheduledQueryParams("q1"))
			},
			op:       "update_first",
			newState: "DISABLED",
		},
		{
			name: "update_invalid_state",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.CreateScheduledQuery(testScheduledQueryParams("q1"))
			},
			op:       "update_first",
			newState: "INVALID",
			wantErr:  cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			var err error
			switch tt.op {
			case "list":
				_, _ = b.CreateScheduledQuery(testScheduledQueryParams("q1"))
				var queries []cloudwatchlogs.ScheduledQuery
				queries, _, err = b.ListScheduledQueries(50, "")
				require.NoError(t, err)
				assert.Len(t, queries, 1)

				return
			case "delete_first":
				var queries []cloudwatchlogs.ScheduledQuery
				queries, _, err = b.ListScheduledQueries(50, "")
				require.NoError(t, err)
				require.Len(t, queries, 1)
				err = b.DeleteScheduledQuery(queries[0].ScheduledQueryArn)
			case "delete_direct":
				err = b.DeleteScheduledQuery(tt.arn)
			case "update_first":
				var queries []cloudwatchlogs.ScheduledQuery
				queries, _, err = b.ListScheduledQueries(50, "")
				require.NoError(t, err)
				require.Len(t, queries, 1)
				err = b.UpdateScheduledQuery(queries[0].ScheduledQueryArn, tt.newState)
			}

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCloudWatchLogsBackend_GetScheduledQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name     string
		queryArn string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				arn, err := b.CreateScheduledQuery(testScheduledQueryParams("q1"))
				require.NoError(t, err)

				return arn
			},
		},
		{
			name:     "not_found",
			queryArn: "arn:aws:logs:us-east-1:123:scheduled-query:nonexistent",
			wantErr:  cloudwatchlogs.ErrScheduledQueryNotFound,
		},
		{
			name:    "empty_arn",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			arn := tt.queryArn
			if tt.setup != nil {
				arn = tt.setup(t, b)
			}

			sq, err := b.GetScheduledQuery(arn)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, sq)
			assert.Equal(t, arn, sq.ScheduledQueryArn)
		})
	}
}

func TestCloudWatchLogsBackend_GetScheduledQueryHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		name       string
		queryArn   string
		wantMinLen int
	}{
		{
			name: "returns_initial_run_from_create",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				arn, err := b.CreateScheduledQuery(testScheduledQueryParams("q1"))
				require.NoError(t, err)

				return arn
			},
			wantMinLen: 1,
		},
		{
			name: "returns_seeded_runs",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				arn, err := b.CreateScheduledQuery(testScheduledQueryParams("q2"))
				require.NoError(t, err)
				cloudwatchlogs.AddScheduledQueryRunInternal(
					b,
					arn,
					cloudwatchlogs.ScheduledQueryRunSummary{
						Arn:            arn,
						RunStatus:      "FAILED",
						ExecutionTime:  500,
						InvocationTime: 400,
					},
				)

				return arn
			},
			wantMinLen: 2,
		},
		{
			name:     "not_found",
			queryArn: "arn:aws:logs:us-east-1:123:scheduled-query:nonexistent",
			wantErr:  cloudwatchlogs.ErrScheduledQueryNotFound,
		},
		{
			name:    "empty_arn",
			wantErr: cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			arn := tt.queryArn
			if tt.setup != nil {
				arn = tt.setup(t, b)
			}

			summaries, next, err := b.GetScheduledQueryHistory(arn, "", 0)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(summaries), tt.wantMinLen)
			assert.Empty(t, next)
		})
	}
}
