package efs_test

import (
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestAudit2_CreationTokenMaxLength verifies CreationToken is rejected when longer than 64 chars.
func TestAudit2_CreationTokenMaxLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		token      string
		wantStatus int
	}{
		{
			name:       "token_exactly_64_chars_accepted",
			token:      strings.Repeat("a", 64),
			wantStatus: http.StatusCreated,
		},
		{
			name:       "token_65_chars_rejected",
			token:      strings.Repeat("a", 65),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "token_100_chars_rejected",
			token:      strings.Repeat("x", 100),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "token_1_char_accepted",
			token:      "a",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
				"CreationToken": tt.token,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				resp := parseRefinementResp(t, rec)
				assert.Contains(t, resp["ErrorCode"], "ValidationException")
			}
		})
	}
}

// TestAudit2_DescribeFileSystems_NotFound verifies DescribeFileSystems returns 404
// when a specific FileSystemId is requested but does not exist.
func TestAudit2_DescribeFileSystems_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		fsID       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "nonexistent_id_returns_404",
			fsID:       "fs-notfound",
			wantStatus: http.StatusNotFound,
			wantErr:    "FileSystemNotFound",
		},
		{
			name:       "garbage_id_returns_404",
			fsID:       "not-a-real-id",
			wantStatus: http.StatusNotFound,
			wantErr:    "FileSystemNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			rec := doRESTRefinement(t, h, http.MethodGet, "/2015-02-01/file-systems/"+tt.fsID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseRefinementResp(t, rec)
			assert.Equal(t, tt.wantErr, resp["ErrorCode"])
		})
	}
}

// TestAudit2_DescribeFileSystems_NotFound_Backend verifies the backend returns ErrNotFound
// when a specific FileSystemId is queried but does not exist.
func TestAudit2_DescribeFileSystems_NotFound_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "nonexistent_id", id: "fs-notfound"},
		{name: "garbage_id", id: "not-a-real-id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			_, _, err := b.DescribeFileSystems(tt.id, "", "", 0)
			require.ErrorIs(t, err, efs.ErrNotFound)
		})
	}
}

// TestAudit2_CreateTags_Validates verifies that the legacy CreateTags operation
// enforces the same tag constraints as TagResource.
func TestAudit2_CreateTags_Validates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		tags      map[string]string
		name      string
		wantErr   bool
	}{
		{
			name:    "valid_tags_accepted",
			tags:    map[string]string{"env": "prod"},
			wantErr: false,
		},
		{
			name:      "empty_key_rejected",
			tags:      map[string]string{"": "value"},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name:      "aws_prefix_rejected",
			tags:      map[string]string{"aws:reserved": "value"},
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
		{
			name: "too_many_tags_rejected",
			tags: func() map[string]string {
				m := make(map[string]string, 51)
				for i := range 51 {
					m[strings.Repeat("k", i+1)] = "v"
				}

				return m
			}(),
			wantErr:   true,
			wantErrIs: efs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(efs.CreateFileSystemRequest{CreationToken: "tags-" + tt.name})
			require.NoError(t, err)

			err = b.CreateTags(fs.FileSystemID, tt.tags)
			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAudit2_CreateReplicationConfiguration_DestinationCount verifies exactly one destination
// is required, matching AWS EFS behavior.
func TestAudit2_CreateReplicationConfiguration_DestinationCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		destinations []map[string]any
		wantStatus   int
	}{
		{
			name:         "one_destination_accepted",
			destinations: []map[string]any{{"Region": "us-west-2"}},
			wantStatus:   http.StatusOK,
		},
		{
			name:         "zero_destinations_rejected",
			destinations: []map[string]any{},
			wantStatus:   http.StatusBadRequest,
		},
		{
			name: "two_destinations_rejected",
			destinations: []map[string]any{
				{"Region": "us-west-2"},
				{"Region": "eu-west-1"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			fsID := createFS(t, h, "repl-dest-"+tt.name)

			rec := doRESTRefinement(t, h, http.MethodPost,
				"/2015-02-01/file-systems/"+fsID+"/replication-configuration",
				map[string]any{"Destinations": tt.destinations},
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				resp := parseRefinementResp(t, rec)
				assert.Contains(t, resp["ErrorCode"], "ValidationException")
			}
		})
	}
}

// TestAudit2_CreateMountTarget_SubnetIDRequired verifies SubnetId is required,
// matching AWS EFS behavior.
func TestAudit2_CreateMountTarget_SubnetIDRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "with_subnet_id_accepted",
			body:       map[string]any{"SubnetId": "subnet-abc123"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "without_subnet_id_rejected",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_subnet_id_rejected",
			body:       map[string]any{"SubnetId": ""},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			fsID := createFS(t, h, "mt-subnet-"+tt.name)

			body := map[string]any{"FileSystemId": fsID}
			maps.Copy(body, tt.body)

			rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/mount-targets", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				resp := parseRefinementResp(t, rec)
				assert.Contains(t, resp["ErrorCode"], "BadRequest")
			}
		})
	}
}
