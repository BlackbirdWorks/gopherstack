package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDomainControllers_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		desired int32
		wantLen int
	}{
		{name: "scale up to 3", desired: 3, wantLen: 3},
		{name: "scale up to 1", desired: 1, wantLen: 1},
		{name: "desired 0 removes all", desired: 0, wantLen: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateMicrosoftAD(t, h, "corp.example.com")

			rec := doRequest(t, h, "UpdateNumberOfDomainControllers", map[string]any{
				"DirectoryId":   dirID,
				"DesiredNumber": tt.desired,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			listRec := doRequest(
				t,
				h,
				"DescribeDomainControllers",
				map[string]any{"DirectoryId": dirID},
			)
			require.Equal(t, http.StatusOK, listRec.Code)
			body := respBody(t, listRec)
			controllers, _ := body["DomainControllers"].([]any)
			assert.Len(t, controllers, tt.wantLen)
		})
	}
}

// --- CreateDirectory/ConnectDirectory returns DirectoryId shape ---

func TestDomainControllers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update to 2 then describe"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Describe empty
			rec1 := doRequest(t, h, "DescribeDomainControllers", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			dcs, _ := r1["DomainControllers"].([]any)
			assert.Empty(t, dcs)

			// Update to 2
			rec2 := doRequest(t, h, "UpdateNumberOfDomainControllers", map[string]any{
				"DirectoryId":   dirID,
				"DesiredNumber": 2,
			})
			assert.Equal(t, http.StatusOK, rec2.Code)

			// Describe again
			rec3 := doRequest(t, h, "DescribeDomainControllers", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			dcs2, _ := r3["DomainControllers"].([]any)
			assert.Len(t, dcs2, 2)

			_ = tc
		})
	}
}
