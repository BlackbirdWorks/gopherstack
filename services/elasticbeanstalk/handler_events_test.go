package elasticbeanstalk_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DescribeEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "returns empty events list",
			body:       "Version=2010-12-01&Action=DescribeEvents&ApplicationName=my-app",
			wantStatus: http.StatusOK,
			wantXML:    "DescribeEventsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}
		})
	}
}

// TestHandler_DescribeEvents_FilterByEnvironmentName verifies that DescribeEvents returns a
// launch event and filters correctly by EnvironmentName.
func TestHandler_DescribeEvents_FilterByEnvironmentName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filter   string
		contains []string
		absent   []string
	}{
		{
			name:     "no filter returns all events",
			filter:   "",
			contains: []string{"Successfully launched environment: env1.", "<Severity>INFO</Severity>"},
		},
		{
			name:     "filter by matching EnvironmentName",
			filter:   "&EnvironmentName=env1",
			contains: []string{"Successfully launched environment: env1."},
		},
		{
			name:   "filter by non-matching EnvironmentName returns empty",
			filter: "&EnvironmentName=other",
			absent: []string{"Successfully launched environment"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")

			rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents"+tt.filter)
			require.Equal(t, http.StatusOK, rec.Code)

			for _, s := range tt.contains {
				assert.Contains(t, rec.Body.String(), s)
			}

			for _, s := range tt.absent {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestHandler_DescribeEvents_AfterTerminate verifies that DescribeEvents returns a termination event.
func TestHandler_DescribeEvents_AfterTerminate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")
	postEBForm(t, h, "Version=2010-12-01&Action=TerminateEnvironment&ApplicationName=app&EnvironmentName=env1")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "terminateEnvironment completed successfully.")
}

// TestHandler_DescribeEvents_AfterUpdate verifies that DescribeEvents returns an update event.
func TestHandler_DescribeEvents_AfterUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")
	postEBForm(t, h,
		"Version=2010-12-01&Action=UpdateEnvironment&ApplicationName=app&EnvironmentName=env1&Description=updated")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Environment update completed successfully.")
}

// TestHandler_DescribeEvents_MostRecentFirst verifies that newer events appear before older ones.
func TestHandler_DescribeEvents_MostRecentFirst(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")
	postEBForm(t, h, "Version=2010-12-01&Action=TerminateEnvironment&ApplicationName=app&EnvironmentName=env1")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	posTerminate := indexOfFirst(body, "terminateEnvironment")
	posLaunch := indexOfFirst(body, "Successfully launched")

	assert.Less(t, posTerminate, posLaunch, "terminate event should appear before launch event (most recent first)")
}

// TestHandler_DescribeEvents_SeverityFilter verifies that Severity=ERROR filters out INFO events.
// The Terraform provider sends Severity=ERROR when polling for errors after environment creation;
// returning INFO events (like "Successfully launched environment") as if they were errors was
// the root cause of the chronic terraform EB test flake on PRs #2106 and #2122.
func TestHandler_DescribeEvents_SeverityFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filter   string
		contains []string
		absent   []string
	}{
		{
			name:   "Severity=ERROR excludes INFO launch event",
			filter: "&Severity=ERROR",
			absent: []string{"Successfully launched environment"},
		},
		{
			name:     "Severity=INFO returns INFO launch event",
			filter:   "&Severity=INFO",
			contains: []string{"Successfully launched environment: env1."},
		},
		{
			name:     "no Severity filter returns all events",
			filter:   "",
			contains: []string{"Successfully launched environment: env1."},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")

			rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents"+tt.filter)
			require.Equal(t, http.StatusOK, rec.Code)

			for _, s := range tt.contains {
				assert.Contains(t, rec.Body.String(), s)
			}

			for _, s := range tt.absent {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestHandler_DescribeEvents_StartTimeFilter verifies that StartTime filters out older events.
func TestHandler_DescribeEvents_StartTimeFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")

	// StartTime far in the future should exclude the event.
	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents&StartTime=2099-01-01T00:00:00Z")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "Successfully launched environment")

	// StartTime in the past should include the event.
	rec = postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents&StartTime=2000-01-01T00:00:00Z")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Successfully launched environment: env1.")
}

// TestHandler_DescribeEvents_EnvironmentIdFilter verifies that EnvironmentId filters events
// to the correct environment, matching how the Terraform provider queries for errors by ID.
func TestHandler_DescribeEvents_EnvironmentIdFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env2")

	// env1 gets e-00000001, env2 gets e-00000002.
	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents&EnvironmentId=e-00000001")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Successfully launched environment: env1.")
	assert.NotContains(t, body, "env2")

	rec = postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents&EnvironmentId=e-00000002")
	require.Equal(t, http.StatusOK, rec.Code)
	body = rec.Body.String()
	assert.Contains(t, body, "Successfully launched environment: env2.")
	assert.NotContains(t, body, "env1.")
}

// TestHandler_DescribeEvents_PersistenceRoundTrip verifies that events survive snapshot/restore.
func TestHandler_DescribeEvents_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")

	snap := h.Backend.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestHandler()
	require.NoError(t, h2.Backend.Restore(t.Context(), snap))

	rec := postEBForm(t, h2, "Version=2010-12-01&Action=DescribeEvents")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Successfully launched environment: env1.")
}

// TestHandler_DescribeEvents_RealTimestamp verifies events carry real timestamps, not hardcoded ones.
func TestHandler_DescribeEvents_RealTimestamp(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	postEBForm(t, h, "Version=2010-12-01&Action=CreateEnvironment&ApplicationName=app&EnvironmentName=env1")

	rec := postEBForm(t, h, "Version=2010-12-01&Action=DescribeEvents")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<EventDate>")
	assert.NotContains(t, body, "<EventDate>2026-01-01T00:00:00Z</EventDate>", "event date should not be hardcoded")
	assert.Regexp(t, iso8601Re, body)
}
