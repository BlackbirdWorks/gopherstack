package bedrockagentcorecontrol_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestSuccessStatusMatchesAPIReference(t *testing.T) {
	t.Parallel()

	const (
		createWI = "/identities/CreateWorkloadIdentity"
		deleteWI = "/identities/DeleteWorkloadIdentity"
		wiBody   = `{"name":"wi"}`
	)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		status int
	}{
		{"workload identity create", http.MethodPost, createWI, wiBody, http.StatusCreated},
		{"workload identity delete", http.MethodPost, deleteWI, wiBody, http.StatusNoContent},
		{"browser profile create", http.MethodPut, "/browser-profiles", `{"name":"bp"}`, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)

			if tt.status == http.StatusNoContent {
				do(t, f.url, http.MethodPost, createWI, tt.body)
			}

			status, body := do(t, f.url, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.status, status, body)

			if tt.status == http.StatusNoContent {
				assert.Empty(t, body)
			}
		})
	}
}

func do(t *testing.T, base, method, path, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, base+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "aws-sdk-go-v2/1.41.0 api/bedrockagentcorecontrol#1.0.0")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(raw)
}
