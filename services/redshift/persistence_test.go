package redshift_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *redshift.InMemoryBackend) string
		verify func(t *testing.T, b *redshift.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *redshift.InMemoryBackend) string {
				cluster, err := b.CreateCluster("test-cluster", "ra3.xlplus", "admin", "Pass1234!")
				if err != nil {
					return ""
				}

				return cluster.ClusterIdentifier
			},
			verify: func(t *testing.T, b *redshift.InMemoryBackend, id string) {
				t.Helper()

				clusters, err := b.DescribeClusters(id)
				require.NoError(t, err)
				require.Len(t, clusters, 1)
				assert.Equal(t, id, clusters[0].ClusterIdentifier)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *redshift.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *redshift.InMemoryBackend, _ string) {
				t.Helper()

				clusters, err := b.DescribeClusters("")
				require.NoError(t, err)
				assert.Empty(t, clusters)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

func TestRedshiftHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(backend)

	_, err := backend.CreateCluster("snap-cluster", "ra3.xlplus", "admin", "Pass1234!")
	require.NoError(t, err)

	snap := h.Snapshot()
	require.NotNil(t, snap)

	fresh := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	freshH := redshift.NewHandler(fresh)
	require.NoError(t, freshH.Restore(snap))

	clusters, err := fresh.DescribeClusters("")
	require.NoError(t, err)
	assert.Len(t, clusters, 1)
}

func TestRedshiftHandler_Routing(t *testing.T) {
	t.Parallel()

	h := redshift.NewHandler(redshift.NewInMemoryBackend("000000000000", "us-east-1"))

	assert.Equal(t, "Redshift", h.Name())
	assert.Positive(t, h.MatchPriority())

	e := echo.New()

	tests := []struct {
		name      string
		ct        string
		wantMatch bool
	}{
		{"redshift form", "application/x-www-form-urlencoded", true},
		{"json type", "application/json", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(
				http.MethodPost,
				"/",
				strings.NewReader("Action=CreateCluster&Version=2012-12-01"),
			)
			req.Header.Set("Content-Type", tt.ct)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}

	// Test ExtractOperation
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DescribeClusters"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "DescribeClusters", h.ExtractOperation(c))

	// Test ExtractResource
	req2 := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader("Action=DescribeClusters&ClusterIdentifier=my-cluster"),
	)
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.Equal(t, "my-cluster", h.ExtractResource(c2))
}
