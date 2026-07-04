package fsx_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_CreateFileSystem_FileSystemTypeValidation verifies that
// CreateFileSystem rejects unknown FileSystemType values with a 400 error.
// Real AWS FSx accepts only LUSTRE, WINDOWS, ONTAP, and OPENZFS; the emulator
// previously accepted any non-empty string.
func TestParity_CreateFileSystem_FileSystemTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fsType   string
		capacity int
		wantCode int
	}{
		{
			name:     "invalid_type_rejected",
			fsType:   "INVALID",
			capacity: 1200,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "lowercase_rejected",
			fsType:   "lustre",
			capacity: 1200,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "LUSTRE_accepted",
			fsType:   "LUSTRE",
			capacity: 1200,
			wantCode: http.StatusOK,
		},
		{
			name:     "WINDOWS_accepted",
			fsType:   "WINDOWS",
			capacity: 32,
			wantCode: http.StatusOK,
		},
		{
			name:     "ONTAP_accepted",
			fsType:   "ONTAP",
			capacity: 1024,
			wantCode: http.StatusOK,
		},
		{
			name:     "OPENZFS_accepted",
			fsType:   "OPENZFS",
			capacity: 64,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{
				"FileSystemType":  tt.fsType,
				"StorageCapacity": tt.capacity,
			})

			assert.Equal(t, tt.wantCode, rec.Code, "FileSystemType=%q", tt.fsType)
		})
	}
}

// TestParity_CreateFileSystem_StorageCapacityMinimum verifies that CreateFileSystem
// enforces minimum storage capacity per file system type.
// Real AWS FSx rejects below-minimum values with a ValidationError.
func TestParity_CreateFileSystem_StorageCapacityMinimum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fsType   string
		capacity int
		wantCode int
	}{
		{
			name:     "lustre_below_minimum_rejected",
			fsType:   "LUSTRE",
			capacity: 1199,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "lustre_at_minimum_accepted",
			fsType:   "LUSTRE",
			capacity: 1200,
			wantCode: http.StatusOK,
		},
		{
			name:     "windows_below_minimum_rejected",
			fsType:   "WINDOWS",
			capacity: 31,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "windows_at_minimum_accepted",
			fsType:   "WINDOWS",
			capacity: 32,
			wantCode: http.StatusOK,
		},
		{
			name:     "openzfs_below_minimum_rejected",
			fsType:   "OPENZFS",
			capacity: 63,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "openzfs_at_minimum_accepted",
			fsType:   "OPENZFS",
			capacity: 64,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doFSxRequest(t, h, "CreateFileSystem", map[string]any{
				"FileSystemType":  tt.fsType,
				"StorageCapacity": tt.capacity,
			})

			assert.Equal(t, tt.wantCode, rec.Code,
				"FileSystemType=%q StorageCapacity=%d", tt.fsType, tt.capacity)
		})
	}
}
