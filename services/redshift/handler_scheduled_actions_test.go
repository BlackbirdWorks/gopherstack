package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateScheduledAction ----

func TestHandler_CreateScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CreateScheduledAction&Version=2012-12-01" +
				"&ScheduledActionName=my-action" +
				"&Schedule=cron(0+12+*+*+?+*)" +
				"&IamRole=arn:aws:iam::123456789012:role/MyRole" +
				"&ScheduledActionDescription=Daily+resize",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateScheduledActionResponse", "my-action", "ACTIVE"},
		},
		{
			name:         "missing_name",
			body:         "Action=CreateScheduledAction&Version=2012-12-01&Schedule=cron(0+12+*+*+?+*)",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "duplicate",
			body: "Action=CreateScheduledAction&Version=2012-12-01" +
				"&ScheduledActionName=dup-action" +
				"&Schedule=cron(0+12+*+*+?+*)",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ScheduledActionAlreadyExists"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "duplicate" {
				postRedshiftForm(t, h, "Action=CreateScheduledAction&Version=2012-12-01"+
					"&ScheduledActionName=dup-action&Schedule=cron(0+12+*+*+?+*)")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteScheduledAction ----

func TestHandler_DeleteScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DeleteScheduledAction&Version=2012-12-01&ScheduledActionName=my-action",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteScheduledActionResponse"},
		},
		{
			name:         "not_found",
			body:         "Action=DeleteScheduledAction&Version=2012-12-01&ScheduledActionName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ScheduledActionNotFound"},
		},
		{
			name:         "missing_name",
			body:         "Action=DeleteScheduledAction&Version=2012-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "success" {
				postRedshiftForm(t, h, "Action=CreateScheduledAction&Version=2012-12-01"+
					"&ScheduledActionName=my-action&Schedule=cron(0+12+*+*+?+*)")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeScheduledActions ----

func TestHandler_DescribeScheduledActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeScheduledActions&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeScheduledActionsResponse"},
		},
		{
			name:         "with_data",
			body:         "Action=DescribeScheduledActions&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeScheduledActionsResponse", "test-action"},
		},
		{
			name:         "filter_by_name",
			body:         "Action=DescribeScheduledActions&Version=2012-12-01&ScheduledActionName=test-action",
			wantCode:     http.StatusOK,
			wantContains: []string{"test-action", "ACTIVE"},
		},
		{
			name:         "filter_not_found",
			body:         "Action=DescribeScheduledActions&Version=2012-12-01&ScheduledActionName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ScheduledActionNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "with_data" || tt.name == "filter_by_name" {
				postRedshiftForm(t, h, "Action=CreateScheduledAction&Version=2012-12-01"+
					"&ScheduledActionName=test-action&Schedule=cron(0+12+*+*+?+*)")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- ModifyScheduledAction ----

func TestHandler_ModifyScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success_update_schedule",
			body: "Action=ModifyScheduledAction&Version=2012-12-01" +
				"&ScheduledActionName=my-action" +
				"&Schedule=cron(0+6+*+*+?+*)",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyScheduledActionResponse", "my-action"},
		},
		{
			name: "success_update_description",
			body: "Action=ModifyScheduledAction&Version=2012-12-01" +
				"&ScheduledActionName=my-action" +
				"&ScheduledActionDescription=Updated+description",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyScheduledActionResponse", "my-action"},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyScheduledAction&Version=2012-12-01&ScheduledActionName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ScheduledActionNotFound"},
		},
		{
			name:         "missing_name",
			body:         "Action=ModifyScheduledAction&Version=2012-12-01&Schedule=cron(0+6+*+*+?+*)",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "success_update_schedule" || tt.name == "success_update_description" {
				postRedshiftForm(t, h, "Action=CreateScheduledAction&Version=2012-12-01"+
					"&ScheduledActionName=my-action&Schedule=cron(0+12+*+*+?+*)")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- Backend tests for ScheduledAction ----

func TestBackend_ScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *redshift.InMemoryBackend)
		name string
	}{
		{
			name: "create_increments_count",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction(
					"action-1", "cron(0 12 * * ? *)", "arn:aws:iam::123:role/R", "desc", "",
				)
				require.NoError(t, err)
				assert.Equal(t, 1, redshift.ScheduledActionCount(b))
			},
		},
		{
			name: "delete_decrements_count",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction("action-del", "cron(0 12 * * ? *)", "", "", "")
				require.NoError(t, err)
				err = b.DeleteScheduledAction("action-del")
				require.NoError(t, err)
				assert.Equal(t, 0, redshift.ScheduledActionCount(b))
			},
		},
		{
			name: "describe_all_returns_all",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction("a1", "cron(0 12 * * ? *)", "", "", "")
				require.NoError(t, err)
				_, err = b.CreateScheduledAction("a2", "rate(1 day)", "", "", "")
				require.NoError(t, err)
				actions, err := b.DescribeScheduledActions("")
				require.NoError(t, err)
				assert.Len(t, actions, 2)
			},
		},
		{
			name: "modify_updates_schedule",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction("action-mod", "cron(0 12 * * ? *)", "", "", "")
				require.NoError(t, err)
				updated, err := b.ModifyScheduledAction("action-mod", "rate(1 hour)", "", "")
				require.NoError(t, err)
				assert.Equal(t, "rate(1 hour)", updated.Schedule)
			},
		},
		{
			name: "modify_updates_description",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction("action-desc", "cron(0 12 * * ? *)", "", "old desc", "")
				require.NoError(t, err)
				updated, err := b.ModifyScheduledAction("action-desc", "", "", "new desc")
				require.NoError(t, err)
				assert.Equal(t, "new desc", updated.ScheduledActionDescription)
			},
		},
		{
			name: "modify_not_found_returns_error",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.ModifyScheduledAction("nonexistent", "rate(1 day)", "", "")
				require.Error(t, err)
				assert.ErrorIs(t, err, redshift.ErrScheduledActionNotFound)
			},
		},
		{
			name: "duplicate_create_returns_error",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction("action-dup", "cron(0 12 * * ? *)", "", "", "")
				require.NoError(t, err)
				_, err = b.CreateScheduledAction("action-dup", "rate(1 day)", "", "", "")
				require.Error(t, err)
				assert.ErrorIs(t, err, redshift.ErrScheduledActionAlreadyExists)
			},
		},
		{
			name: "delete_not_found_returns_error",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				err := b.DeleteScheduledAction("nonexistent")
				require.Error(t, err)
				assert.ErrorIs(t, err, redshift.ErrScheduledActionNotFound)
			},
		},
		{
			name: "state_is_active_on_create",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				a, err := b.CreateScheduledAction("action-state", "cron(0 12 * * ? *)", "", "", "")
				require.NoError(t, err)
				assert.Equal(t, "ACTIVE", a.State)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}
