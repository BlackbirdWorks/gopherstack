package cloudwatch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// Dashboard: CRUD round-trip
// ---------------------------------------------------------------------------

func TestBackend_Dashboard_CRUD(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	body := `{"widgets":[]}`
	require.NoError(t, b.PutDashboard("dash1", body))

	entry, got, err := b.GetDashboard("dash1")
	require.NoError(t, err)
	assert.Equal(t, "dash1", entry.DashboardName)
	assert.Equal(t, body, got)
	assert.NotEmpty(t, entry.DashboardArn)

	p, err := b.ListDashboards("", "")
	require.NoError(t, err)
	assert.Len(t, p.Data, 1)

	require.NoError(t, b.DeleteDashboards([]string{"dash1"}))
	_, _, err = b.GetDashboard("dash1")
	assert.Error(t, err)
}

func TestCloudWatchBackend_Dashboards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *cloudwatch.InMemoryBackend)
		name         string
		putName      string
		putBody      string
		wantNames    []string
		wantNotNames []string
		wantPutErr   bool
	}{
		{
			name:      "PutDashboard/creates",
			putName:   "MyDash",
			putBody:   `{"widgets":[]}`,
			wantNames: []string{"MyDash"},
		},
		{
			name: "PutDashboard/updates existing",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutDashboard("UpdateDash", `{"widgets":[]}`))
			},
			putName:   "UpdateDash",
			putBody:   `{"widgets":[{"type":"text"}]}`,
			wantNames: []string{"UpdateDash"},
		},
		{
			name:       "PutDashboard/missing name returns error",
			putName:    "",
			putBody:    `{}`,
			wantPutErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.PutDashboard(tt.putName, tt.putBody)

			if tt.wantPutErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			page, listErr := b.ListDashboards("", "")
			require.NoError(t, listErr)

			names := make([]string, 0, len(page.Data))
			for _, e := range page.Data {
				names = append(names, e.DashboardName)
			}

			for _, wn := range tt.wantNames {
				assert.Contains(t, names, wn)
			}
			for _, wn := range tt.wantNotNames {
				assert.NotContains(t, names, wn)
			}
		})
	}
}

func TestCloudWatchBackend_GetDashboard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		dashName  string
		body      string
		fetchName string
		wantErr   bool
	}{
		{
			name:      "found",
			dashName:  "MyDash",
			body:      `{"widgets":[]}`,
			fetchName: "MyDash",
		},
		{
			name:      "not found",
			dashName:  "MyDash",
			body:      `{"widgets":[]}`,
			fetchName: "OtherDash",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			require.NoError(t, b.PutDashboard(tt.dashName, tt.body))

			entry, body, err := b.GetDashboard(tt.fetchName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.fetchName, entry.DashboardName)
			assert.Equal(t, tt.body, body)
			assert.NotEmpty(t, entry.DashboardArn)
			assert.Equal(t, int64(len(tt.body)), entry.Size)
		})
	}
}

func TestCloudWatchBackend_ListDashboards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prefix    string
		dashNames []string
		wantCount int
	}{
		{
			name:      "list all",
			dashNames: []string{"alpha", "beta", "gamma"},
			wantCount: 3,
		},
		{
			name:      "filter by prefix",
			dashNames: []string{"prod-web", "prod-api", "staging-web"},
			prefix:    "prod-",
			wantCount: 2,
		},
		{
			name:      "no match prefix",
			dashNames: []string{"alpha", "beta"},
			prefix:    "xyz-",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			for _, n := range tt.dashNames {
				require.NoError(t, b.PutDashboard(n, `{}`))
			}

			page, err := b.ListDashboards(tt.prefix, "")
			require.NoError(t, err)
			assert.Len(t, page.Data, tt.wantCount)
		})
	}
}

func TestCloudWatchBackend_DeleteDashboards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		create     []string
		deleteName []string
		wantCount  int
	}{
		{
			name:       "delete existing",
			create:     []string{"dash1", "dash2", "dash3"},
			deleteName: []string{"dash1", "dash3"},
			wantCount:  1,
		},
		{
			name:       "delete non-existent is no-op",
			create:     []string{"dash1"},
			deleteName: []string{"nonexistent"},
			wantCount:  1,
		},
		{
			name:       "delete all",
			create:     []string{"a", "b"},
			deleteName: []string{"a", "b"},
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			for _, n := range tt.create {
				require.NoError(t, b.PutDashboard(n, `{}`))
			}

			require.NoError(t, b.DeleteDashboards(tt.deleteName))

			page, err := b.ListDashboards("", "")
			require.NoError(t, err)
			assert.Len(t, page.Data, tt.wantCount)
		})
	}
}

func TestCloudWatchBackend_GetDashboardARNs(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutDashboard("my-dash", `{"widgets":[]}`))

	arns := b.GetDashboardARNs([]string{"my-dash", "nonexistent"})
	require.Len(t, arns, 1)
	assert.Contains(t, arns[0], "my-dash")
}
