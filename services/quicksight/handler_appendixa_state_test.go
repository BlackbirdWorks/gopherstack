package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// roundTripStep is one HTTP call in a stateful round-trip scenario.
type roundTripStep struct {
	body         any
	name         string
	method       string
	path         string
	wantContains string // top-level key expected in the response body
	wantStatus   int
}

func runRoundTrip(t *testing.T, h *quicksight.Handler, steps []roundTripStep) {
	t.Helper()

	for _, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			rec := doRequest(t, h, st.method, st.path, st.body)
			assert.Equal(t, st.wantStatus, rec.Code, "status for %s", st.name)
			if st.wantContains != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, st.wantContains, "key for %s", st.name)
			}
		})
	}
}

func TestQuickSight_FolderRoundTrip(t *testing.T) { //nolint:paralleltest // shared handler state.
	b := newTestBackend(t)
	h := quicksight.NewHandler(b)

	steps := []roundTripStep{
		{
			name:         "describe missing folder returns not found",
			method:       http.MethodGet,
			path:         accountPath("/folders/rtf1"),
			wantStatus:   http.StatusNotFound,
			wantContains: "Message",
		},
		{
			name:         "create folder",
			method:       http.MethodPost,
			path:         accountPath("/folders/rtf1"),
			body:         map[string]any{"Name": "RT Folder"},
			wantStatus:   http.StatusOK,
			wantContains: "FolderId",
		},
		{
			name:         "describe reflects created folder",
			method:       http.MethodGet,
			path:         accountPath("/folders/rtf1"),
			wantStatus:   http.StatusOK,
			wantContains: "Folder",
		},
		{
			name:         "update folder",
			method:       http.MethodPut,
			path:         accountPath("/folders/rtf1"),
			body:         map[string]any{"Name": "RT Renamed"},
			wantStatus:   http.StatusOK,
			wantContains: "FolderId",
		},
		{
			name:         "list folders includes folder",
			method:       http.MethodGet,
			path:         accountPath("/folders"),
			wantStatus:   http.StatusOK,
			wantContains: "FolderSummaryList",
		},
		{
			name:         "delete folder",
			method:       http.MethodDelete,
			path:         accountPath("/folders/rtf1"),
			wantStatus:   http.StatusOK,
			wantContains: "FolderId",
		},
		{
			name:         "describe after delete returns not found",
			method:       http.MethodGet,
			path:         accountPath("/folders/rtf1"),
			wantStatus:   http.StatusNotFound,
			wantContains: "Message",
		},
	}

	runRoundTrip(t, h, steps)

	// describe reflects renamed value before delete is verified separately below.
	assert.Equal(t, 0, quicksight.FolderCount(b), "folder removed after delete")
}

func TestQuickSight_FolderDescribeReflectsUpdate(t *testing.T) { //nolint:paralleltest // shared handler state.
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/folders/upd1"), map[string]any{"Name": "Original"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPut, accountPath("/folders/upd1"), map[string]any{"Name": "Updated"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, accountPath("/folders/upd1"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	folder, ok := body["Folder"].(map[string]any)
	require.True(t, ok, "Folder object present")
	assert.Equal(t, "Updated", folder["Name"], "describe reflects updated name")
}

func TestQuickSight_TemplateRoundTrip(t *testing.T) { //nolint:paralleltest // shared handler state.
	b := newTestBackend(t)
	h := quicksight.NewHandler(b)

	steps := []roundTripStep{
		{
			name:         "describe missing template returns not found",
			method:       http.MethodGet,
			path:         accountPath("/templates/rtt1"),
			wantStatus:   http.StatusNotFound,
			wantContains: "Message",
		},
		{
			name:         "create template",
			method:       http.MethodPost,
			path:         accountPath("/templates/rtt1"),
			body:         map[string]any{"Name": "RT Tpl"},
			wantStatus:   http.StatusOK,
			wantContains: "TemplateId",
		},
		{
			name:         "describe reflects created template",
			method:       http.MethodGet,
			path:         accountPath("/templates/rtt1"),
			wantStatus:   http.StatusOK,
			wantContains: "Template",
		},
		{
			name:         "list templates includes template",
			method:       http.MethodGet,
			path:         accountPath("/templates"),
			wantStatus:   http.StatusOK,
			wantContains: "TemplateSummaryList",
		},
		{
			name:         "delete template",
			method:       http.MethodDelete,
			path:         accountPath("/templates/rtt1"),
			wantStatus:   http.StatusOK,
			wantContains: "TemplateId",
		},
		{
			name:         "describe after delete returns not found",
			method:       http.MethodGet,
			path:         accountPath("/templates/rtt1"),
			wantStatus:   http.StatusNotFound,
			wantContains: "Message",
		},
	}

	runRoundTrip(t, h, steps)
	assert.Equal(t, 0, quicksight.TemplateCount(b), "template removed after delete")
}

func TestQuickSight_ThemeRoundTrip(t *testing.T) { //nolint:paralleltest // shared handler state.
	b := newTestBackend(t)
	h := quicksight.NewHandler(b)

	steps := []roundTripStep{
		{
			name:         "describe missing theme returns not found",
			method:       http.MethodGet,
			path:         accountPath("/themes/rtth1"),
			wantStatus:   http.StatusNotFound,
			wantContains: "Message",
		},
		{
			name:         "create theme",
			method:       http.MethodPost,
			path:         accountPath("/themes/rtth1"),
			body:         map[string]any{"Name": "RT Theme"},
			wantStatus:   http.StatusOK,
			wantContains: "ThemeId",
		},
		{
			name:         "describe reflects created theme",
			method:       http.MethodGet,
			path:         accountPath("/themes/rtth1"),
			wantStatus:   http.StatusOK,
			wantContains: "Theme",
		},
		{
			name:         "list themes includes theme",
			method:       http.MethodGet,
			path:         accountPath("/themes"),
			wantStatus:   http.StatusOK,
			wantContains: "ThemeSummaryList",
		},
		{
			name:         "delete theme",
			method:       http.MethodDelete,
			path:         accountPath("/themes/rtth1"),
			wantStatus:   http.StatusOK,
			wantContains: "ThemeId",
		},
		{
			name:         "describe after delete returns not found",
			method:       http.MethodGet,
			path:         accountPath("/themes/rtth1"),
			wantStatus:   http.StatusNotFound,
			wantContains: "Message",
		},
	}

	runRoundTrip(t, h, steps)
	assert.Equal(t, 0, quicksight.ThemeCount(b), "theme removed after delete")
}

func TestQuickSight_VPCConnectionRoundTrip(t *testing.T) { //nolint:paralleltest // shared handler state.
	b := newTestBackend(t)
	h := quicksight.NewHandler(b)

	steps := []roundTripStep{
		{
			name:         "describe missing vpc returns not found",
			method:       http.MethodGet,
			path:         accountPath("/vpc-connections/rtv1"),
			wantStatus:   http.StatusNotFound,
			wantContains: "Message",
		},
		{
			name:         "create vpc connection",
			method:       http.MethodPost,
			path:         accountPath("/vpc-connections"),
			body:         map[string]any{"VPCConnectionId": "rtv1", "Name": "RT VPC"},
			wantStatus:   http.StatusOK,
			wantContains: "VPCConnectionId",
		},
		{
			name:         "describe reflects created vpc connection",
			method:       http.MethodGet,
			path:         accountPath("/vpc-connections/rtv1"),
			wantStatus:   http.StatusOK,
			wantContains: "VPCConnection",
		},
		{
			name:         "list vpc connections includes connection",
			method:       http.MethodGet,
			path:         accountPath("/vpc-connections"),
			wantStatus:   http.StatusOK,
			wantContains: "VPCConnectionSummaries",
		},
		{
			name:         "update vpc connection",
			method:       http.MethodPut,
			path:         accountPath("/vpc-connections/rtv1"),
			body:         map[string]any{"Name": "RT VPC Renamed"},
			wantStatus:   http.StatusOK,
			wantContains: "VPCConnectionId",
		},
		{
			name:         "delete vpc connection",
			method:       http.MethodDelete,
			path:         accountPath("/vpc-connections/rtv1"),
			wantStatus:   http.StatusOK,
			wantContains: "VPCConnectionId",
		},
		{
			name:         "describe after delete returns not found",
			method:       http.MethodGet,
			path:         accountPath("/vpc-connections/rtv1"),
			wantStatus:   http.StatusNotFound,
			wantContains: "Message",
		},
	}

	runRoundTrip(t, h, steps)
	assert.Equal(t, 0, quicksight.VPCConnectionCount(b), "vpc connection removed after delete")
}

func TestQuickSight_BrandRoundTrip(t *testing.T) { //nolint:paralleltest // shared handler state.
	b := newTestBackend(t)
	h := quicksight.NewHandler(b)

	steps := []roundTripStep{
		{
			name:         "describe missing brand returns not found",
			method:       http.MethodGet,
			path:         accountPath("/brands/rtb1"),
			wantStatus:   http.StatusNotFound,
			wantContains: "Message",
		},
		{
			name:         "create brand",
			method:       http.MethodPost,
			path:         accountPath("/brands/rtb1"),
			body:         map[string]any{"BrandName": "RT Brand"},
			wantStatus:   http.StatusOK,
			wantContains: "BrandId",
		},
		{
			name:         "describe reflects created brand",
			method:       http.MethodGet,
			path:         accountPath("/brands/rtb1"),
			wantStatus:   http.StatusOK,
			wantContains: "Brand",
		},
		{
			name:         "list brands includes brand",
			method:       http.MethodGet,
			path:         accountPath("/brands"),
			wantStatus:   http.StatusOK,
			wantContains: "Brands",
		},
		{
			name:         "delete brand",
			method:       http.MethodDelete,
			path:         accountPath("/brands/rtb1"),
			wantStatus:   http.StatusOK,
			wantContains: "BrandId",
		},
		{
			name:         "describe after delete returns not found",
			method:       http.MethodGet,
			path:         accountPath("/brands/rtb1"),
			wantStatus:   http.StatusNotFound,
			wantContains: "Message",
		},
	}

	runRoundTrip(t, h, steps)
	assert.Equal(t, 0, quicksight.BrandCount(b), "brand removed after delete")
}

func TestQuickSight_DuplicateCreateConflicts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body any
		name string
		path string
	}{
		{name: "folder", path: accountPath("/folders/dup1"), body: map[string]any{"Name": "Dup"}},
		{name: "template", path: accountPath("/templates/dup1"), body: map[string]any{"Name": "Dup"}},
		{name: "theme", path: accountPath("/themes/dup1"), body: map[string]any{"Name": "Dup"}},
		{name: "brand", path: accountPath("/brands/dup1"), body: map[string]any{"BrandName": "Dup"}},
		{
			name: "vpc",
			path: accountPath("/vpc-connections"),
			body: map[string]any{"VPCConnectionId": "dup1", "Name": "Dup"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, tc.path, tc.body)
			require.Equal(t, http.StatusOK, rec.Code, "first create")

			rec = doRequest(t, h, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, http.StatusConflict, rec.Code, "duplicate create conflicts")
		})
	}
}
