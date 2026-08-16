package quicksight_test

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// isBase64Int returns true if s is a valid base64-encoded decimal integer.
func isBase64Int(s string) bool {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return false
	}

	_, err = strconv.Atoi(string(b))

	return err == nil
}

// TestPaginationTokensAreOpaque verifies that NextToken values are base64-encoded
// integer offsets, not raw item names/IDs.
func TestPaginationTokensAreOpaque(t *testing.T) {
	t.Parallel()

	const accountID = "000000000000"

	tests := []struct {
		setup   func(h *quicksight.Handler)
		listKey string
		name    string
		path    string
	}{
		{
			name:    "ListNamespaces",
			path:    accountPath("/namespaces"),
			listKey: "Namespaces",
			setup: func(h *quicksight.Handler) {
				for i := range 3 {
					doRequest(t, h, http.MethodPost, accountPath(""),
						map[string]any{"Namespace": fmt.Sprintf("ns%d", i), "IdentityStore": "QUICKSIGHT"})
				}
			},
		},
		{
			name:    "ListFolders",
			path:    accountPath("/folders"),
			listKey: "FolderSummaryList",
			setup: func(h *quicksight.Handler) {
				for i := range 3 {
					doRequest(t, h, http.MethodPost, accountPath(fmt.Sprintf("/folders/f%d", i)),
						map[string]any{"Name": fmt.Sprintf("Folder%d", i)})
				}
			},
		},
		{
			name:    "ListTemplates",
			path:    accountPath("/templates"),
			listKey: "TemplateSummaryList",
			setup: func(h *quicksight.Handler) {
				for i := range 3 {
					doRequest(t, h, http.MethodPost, accountPath(fmt.Sprintf("/templates/tpl%d", i)),
						map[string]any{"Name": fmt.Sprintf("Template%d", i)})
				}
			},
		},
		{
			name:    "ListThemes",
			path:    accountPath("/themes"),
			listKey: "ThemeSummaryList",
			setup: func(h *quicksight.Handler) {
				for i := range 3 {
					doRequest(t, h, http.MethodPost, accountPath(fmt.Sprintf("/themes/th%d", i)),
						map[string]any{"Name": fmt.Sprintf("Theme%d", i)})
				}
			},
		},
		{
			name:    "ListVPCConnections",
			path:    fmt.Sprintf("/accounts/%s/vpc-connections", accountID),
			listKey: "VPCConnectionSummaries",
			setup: func(h *quicksight.Handler) {
				for i := range 3 {
					doRequest(t, h, http.MethodPost, fmt.Sprintf("/accounts/%s/vpc-connections", accountID),
						map[string]any{"VPCConnectionId": fmt.Sprintf("vpc%d", i), "Name": fmt.Sprintf("VPC%d", i)})
				}
			},
		},
		{
			name:    "ListBrands",
			path:    fmt.Sprintf("/accounts/%s/brands", accountID),
			listKey: "Brands",
			setup: func(h *quicksight.Handler) {
				for i := range 3 {
					doRequest(t, h, http.MethodPost,
						fmt.Sprintf("/accounts/%s/brands/br%d", accountID, i),
						map[string]any{"BrandName": fmt.Sprintf("Brand%d", i)})
				}
			},
		},
		{
			name:    "ListDashboardVersions",
			listKey: "DashboardVersionSummaryList",
			path:    accountPath("/dashboards/dash1/versions"),
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash1"),
					map[string]any{"Name": "Dash"})
				// UpdateDashboard creates new versions
				for range 3 {
					doRequest(t, h, http.MethodPut, accountPath("/dashboards/dash1"),
						map[string]any{"Name": "Dash Updated"})
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			h := quicksight.NewHandler(b)
			tc.setup(h)

			rec := doRequest(t, h, http.MethodGet, tc.path+"?max-results=2", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			body := parseBody(t, rec)
			tok, _ := body["NextToken"].(string)
			require.NotEmpty(t, tok, "expected NextToken for page 1 of 3")
			assert.True(t, isBase64Int(tok), "NextToken must be base64-encoded integer, got %q", tok)

			b64, _ := base64.StdEncoding.DecodeString(tok)
			offset, _ := strconv.Atoi(string(b64))
			assert.Equal(t, 2, offset, "offset must equal page size")
		})
	}
}

// TestListDashboardVersionsPagination verifies maxResults/nextToken work on version list.
func TestListDashboardVersionsPagination(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	h := quicksight.NewHandler(b)

	doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash1"),
		map[string]any{"Name": "Dash"})
	for range 4 {
		doRequest(t, h, http.MethodPut, accountPath("/dashboards/dash1"),
			map[string]any{"Name": "Dash"})
	}

	rec1 := doRequest(t, h, http.MethodGet, accountPath("/dashboards/dash1/versions?max-results=3"), nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	body1 := parseBody(t, rec1)

	versions1, _ := body1["DashboardVersionSummaryList"].([]any)
	assert.Len(t, versions1, 3)

	tok, _ := body1["NextToken"].(string)
	require.NotEmpty(t, tok, "expected NextToken after first page")
	assert.True(t, isBase64Int(tok))

	rec2 := doRequest(t, h, http.MethodGet,
		accountPath(fmt.Sprintf("/dashboards/dash1/versions?max-results=3&next-token=%s",
			strings.ReplaceAll(tok, "+", "%2B"))), nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	body2 := parseBody(t, rec2)

	versions2, _ := body2["DashboardVersionSummaryList"].([]any)
	assert.NotEmpty(t, versions2)
	_, hasNext := body2["NextToken"]
	assert.False(t, hasNext, "last page must not have NextToken")
}
