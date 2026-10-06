package swf_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

func TestHandler_ListTypes_NameFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		action       string
		typeKey      string
		filter       string
		wantVersions []string
	}{
		{
			name: "activity by name", action: "ListActivityTypes", typeKey: "activityType",
			filter: "a", wantVersions: []string{"1", "2"},
		},
		{
			name: "activity unfiltered", action: "ListActivityTypes", typeKey: "activityType",
			wantVersions: []string{"1", "2", "1"},
		},
		{
			name: "workflow by name", action: "ListWorkflowTypes", typeKey: "workflowType",
			filter: "b", wantVersions: []string{"1"},
		},
		{name: "workflow no match", action: "ListWorkflowTypes", typeKey: "workflowType", filter: "zzz"},
		{
			name: "workflow same name orders by version", action: "ListWorkflowTypes", typeKey: "workflowType",
			filter: "a", wantVersions: []string{"1", "2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			for _, v := range []string{"2", "1"} {
				b.AddActivityTypeInternal("d1", "a", v, "REGISTERED")
				b.AddWorkflowTypeInternal("d1", "a", v, "REGISTERED")
			}
			b.AddActivityTypeInternal("d1", "b", "1", "REGISTERED")
			b.AddWorkflowTypeInternal("d1", "b", "1", "REGISTERED")

			body := map[string]any{"domain": "d1", "registrationStatus": "REGISTERED"}
			if tt.filter != "" {
				body["name"] = tt.filter
			}

			rec := doSWFRequest(t, swf.NewHandler(b), tt.action, body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			infos, _ := parseSWFResp(t, rec)["typeInfos"].([]any)

			var got []string

			for _, it := range infos {
				ref := it.(map[string]any)[tt.typeKey].(map[string]any)
				if tt.filter != "" {
					assert.Equal(t, tt.filter, ref["name"])
				}

				got = append(got, ref["version"].(string))
			}

			assert.Equal(t, tt.wantVersions, got)
		})
	}
}
