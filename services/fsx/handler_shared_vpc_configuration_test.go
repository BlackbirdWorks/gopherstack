package fsx_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSx_SharedVpcConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		enable      string
		wantEnabled string
		wantCode    int
	}{
		{
			name:        "default is false",
			enable:      "",
			wantEnabled: "false",
			wantCode:    http.StatusOK,
		},
		{
			name:        "update to true",
			enable:      "true",
			wantEnabled: "true",
			wantCode:    http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			if tc.enable != "" {
				rec := doFSxRequest(t, h, "UpdateSharedVpcConfiguration", map[string]any{
					"EnableSharedVpcOnFileSystemCreation": tc.enable,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doFSxRequest(t, h, "DescribeSharedVpcConfiguration", map[string]any{})
			require.Equal(t, tc.wantCode, rec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tc.wantEnabled, out["EnableSharedVpcOnFileSystemCreation"])
		})
	}
}
