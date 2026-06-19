package resourcegroups_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateGroup_NameValidation asserts 400 for invalid or empty group names.
func TestCreateGroup_NameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		groupName string
		wantCode  int
	}{
		{name: "valid_name", groupName: "my-group", wantCode: http.StatusOK},
		{name: "empty_name", groupName: "", wantCode: http.StatusBadRequest},
		{name: "too_long_name", groupName: string(make([]byte, 129)), wantCode: http.StatusBadRequest},
		{name: "invalid_chars", groupName: "bad name!", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": tt.groupName})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				require.NotEmpty(t, rec.Body.String())
			}
		})
	}
}
