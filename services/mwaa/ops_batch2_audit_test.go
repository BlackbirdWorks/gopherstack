package mwaa_test

// AWS-accuracy audit batch-2 ops tests (go-1fus).
//
// Covers: CreateCliToken / CreateWebLoginToken missing AVAILABLE-state guard
// (AWS returns ResourceNotFoundException when env is not AVAILABLE); and
// UpdateEnvironment missing AVAILABLE-state guard (AWS returns
// ValidationException when env is in a transient state such as CREATING).

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

// ─────────────────────────────────────────────────────────────
// 1. CreateCliToken – AVAILABLE-state guard
// ─────────────────────────────────────────────────────────────

func TestOpsB2_CreateCliToken_RequiresAvailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  string
		wantErr bool
	}{
		{name: "creating_rejected", status: "CREATING", wantErr: true},
		{name: "updating_rejected", status: "UPDATING", wantErr: true},
		{name: "deleting_rejected", status: "DELETING", wantErr: true},
		{name: "available_ok", status: "AVAILABLE", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			env := b.AddEnvironmentInternal("cli-state-env-" + tt.name)
			env.Status = tt.status

			_, err := b.CreateCliToken(context.Background(), "cli-state-env-"+tt.name)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound,
					"non-AVAILABLE env must return not-found sentinel")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestOpsB2_CreateCliToken_HTTP_NonAvailable_Returns404(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	// Create env – it lands in CREATING state; first GET is not called, so it stays CREATING.
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/cli-http-env", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":  "arn:aws:s3:::b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doMWAARequest(t, h, http.MethodPost, "/clitoken/cli-http-env", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// 2. CreateWebLoginToken – AVAILABLE-state guard
// ─────────────────────────────────────────────────────────────

func TestOpsB2_CreateWebLoginToken_RequiresAvailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  string
		wantErr bool
	}{
		{name: "creating_rejected", status: "CREATING", wantErr: true},
		{name: "updating_rejected", status: "UPDATING", wantErr: true},
		{name: "deleting_rejected", status: "DELETING", wantErr: true},
		{name: "available_ok", status: "AVAILABLE", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			env := b.AddEnvironmentInternal("web-state-env-" + tt.name)
			env.Status = tt.status

			_, err := b.CreateWebLoginToken(context.Background(), "web-state-env-"+tt.name)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound,
					"non-AVAILABLE env must return not-found sentinel")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestOpsB2_CreateWebLoginToken_HTTP_NonAvailable_Returns404(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/web-http-env", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":  "arn:aws:s3:::b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doMWAARequest(t, h, http.MethodPost, "/webtoken/web-http-env", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// 3. UpdateEnvironment – AVAILABLE-state guard
// ─────────────────────────────────────────────────────────────

func TestOpsB2_UpdateEnvironment_RequiresAvailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  string
		wantErr bool
	}{
		{name: "creating_rejected", status: "CREATING", wantErr: true},
		{name: "updating_rejected", status: "UPDATING", wantErr: true},
		{name: "deleting_rejected", status: "DELETING", wantErr: true},
		{name: "available_ok", status: "AVAILABLE", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			env := b.AddEnvironmentInternal("upd-state-env-" + tt.name)
			env.Status = tt.status

			_, err := b.UpdateEnvironment(
				context.Background(),
				"upd-state-env-"+tt.name,
				&mwaa.ExportedUpdateEnvironmentRequest{
					DagS3Path: "new-dags/",
				},
			)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, mwaa.ErrInvalidParameter,
					"update on non-AVAILABLE env must return invalid-parameter sentinel")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestOpsB2_UpdateEnvironment_HTTP_NonAvailable_Returns400(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	// Create env – stays in CREATING until first GET promotes it.
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/upd-http-env", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":  "arn:aws:s3:::b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// PATCH while env is still in CREATING state.
	rec = doMWAARequest(t, h, http.MethodPatch, "/environments/upd-http-env", map[string]any{
		"DagS3Path": "new-dags/",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
