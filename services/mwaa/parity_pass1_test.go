package mwaa_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

// ─────────────────────────────────────────────────────────────
// WorkerReplacementStrategy — create, update, validate
// ─────────────────────────────────────────────────────────────

func TestParity_WorkerReplacementStrategy_CreateValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
	}{
		{name: "FORCED", strategy: "FORCED"},
		{name: "TERMINATION_WITH_DRAIN", strategy: "TERMINATION_WITH_DRAIN"},
		{name: "empty_defaults_ok", strategy: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.WorkerReplacementStrategy = tt.strategy
			env, err := b.CreateEnvironment(context.Background(), "wrs-create-"+tt.name, req)
			require.NoError(t, err)

			if tt.strategy != "" {
				assert.Equal(t, tt.strategy, env.WorkerReplacementStrategy)
			}
		})
	}
}

func TestParity_WorkerReplacementStrategy_CreateInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
	}{
		{name: "lowercase", strategy: "forced"},
		{name: "bogus", strategy: "ROLLING"},
		{name: "random", strategy: "BEST_EFFORT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.WorkerReplacementStrategy = tt.strategy
			_, err := b.CreateEnvironment(context.Background(), "wrs-inv-"+tt.name, req)
			require.Error(t, err)
		})
	}
}

func TestParity_WorkerReplacementStrategy_UpdatePersists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		initial   string
		updated   string
		wantFinal string
	}{
		{
			name:      "set_FORCED",
			initial:   "TERMINATION_WITH_DRAIN",
			updated:   "FORCED",
			wantFinal: "FORCED",
		},
		{
			name:      "set_TERMINATION_WITH_DRAIN",
			initial:   "FORCED",
			updated:   "TERMINATION_WITH_DRAIN",
			wantFinal: "TERMINATION_WITH_DRAIN",
		},
		{
			name:      "empty_update_keeps_current",
			initial:   "FORCED",
			updated:   "",
			wantFinal: "FORCED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			req := newCreateReq()
			req.WorkerReplacementStrategy = tt.initial
			envName := "wrs-upd-" + tt.name
			_, err := b.CreateEnvironment(context.Background(), envName, req)
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), envName) // promote to AVAILABLE

			_, err = b.UpdateEnvironment(context.Background(), envName, &mwaa.ExportedUpdateEnvironmentRequest{
				WorkerReplacementStrategy: tt.updated,
			})
			require.NoError(t, err)

			env, err := b.GetEnvironment(context.Background(), envName)
			require.NoError(t, err)
			assert.Equal(t, tt.wantFinal, env.WorkerReplacementStrategy)
		})
	}
}

func TestParity_WorkerReplacementStrategy_UpdateInvalid(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	envName := "wrs-upd-invalid"
	_, err := b.CreateEnvironment(context.Background(), envName, newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), envName) // promote to AVAILABLE

	_, err = b.UpdateEnvironment(context.Background(), envName, &mwaa.ExportedUpdateEnvironmentRequest{
		WorkerReplacementStrategy: "PREFERRED",
	})
	require.Error(t, err)
}

// ─────────────────────────────────────────────────────────────
// Token hostname accuracy — CreateCliToken / CreateWebLoginToken
// ─────────────────────────────────────────────────────────────

func TestParity_CreateCliToken_HostnameMatchesWebserverURL(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	envName := "cli-hostname-env"
	_, err := b.CreateEnvironment(context.Background(), envName, newCreateReq())
	require.NoError(t, err)
	env, _ := b.GetEnvironment(context.Background(), envName) // promote + capture URL

	_, hostname, err := b.CreateCliToken(context.Background(), envName)
	require.NoError(t, err)

	wantHostname := strings.TrimPrefix(env.WebserverURL, "https://")
	assert.Equal(t, wantHostname, hostname)
}

func TestParity_CreateWebLoginToken_HostnameMatchesWebserverURL(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	envName := "web-hostname-env"
	_, err := b.CreateEnvironment(context.Background(), envName, newCreateReq())
	require.NoError(t, err)
	env, _ := b.GetEnvironment(context.Background(), envName)

	_, hostname, err := b.CreateWebLoginToken(context.Background(), envName)
	require.NoError(t, err)

	wantHostname := strings.TrimPrefix(env.WebserverURL, "https://")
	assert.Equal(t, wantHostname, hostname)
}

func TestParity_TokenHostname_ContainsAirflowRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		region    string
		wantInfix string
	}{
		{name: "us_east_1", region: "us-east-1", wantInfix: ".airflow.us-east-1.amazonaws.com"},
		{name: "eu_west_1", region: "eu-west-1", wantInfix: ".airflow.eu-west-1.amazonaws.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(tt.region, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "tok-region-env", newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "tok-region-env")

			_, hostname, err := b.CreateCliToken(context.Background(), "tok-region-env")
			require.NoError(t, err)
			assert.Contains(t, hostname, tt.wantInfix)
		})
	}
}

func TestParity_TokenHostname_NotSameAcrossEnvironments(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	for _, name := range []string{"env-tok-a", "env-tok-b"} {
		_, err := b.CreateEnvironment(context.Background(), name, newCreateReq())
		require.NoError(t, err)
		_, _ = b.GetEnvironment(context.Background(), name)
	}

	_, hostA, err := b.CreateCliToken(context.Background(), "env-tok-a")
	require.NoError(t, err)
	_, hostB, err := b.CreateCliToken(context.Background(), "env-tok-b")
	require.NoError(t, err)

	assert.NotEqual(t, hostA, hostB, "different environments should have different hostnames")
}

func TestParity_CreateCliToken_NotAvailable_ReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
	}{
		{name: "creating", status: "CREATING"},
		{name: "updating", status: "UPDATING"},
		{name: "deleting", status: "DELETING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			env := b.AddEnvironmentInternal("cli-guard-" + tt.name)
			env.Status = tt.status

			_, _, err := b.CreateCliToken(context.Background(), "cli-guard-"+tt.name)
			require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
		})
	}
}

func TestParity_CreateWebLoginToken_NotAvailable_ReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
	}{
		{name: "creating", status: "CREATING"},
		{name: "updating", status: "UPDATING"},
		{name: "deleting", status: "DELETING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			env := b.AddEnvironmentInternal("web-guard-" + tt.name)
			env.Status = tt.status

			_, _, err := b.CreateWebLoginToken(context.Background(), "web-guard-"+tt.name)
			require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
		})
	}
}
