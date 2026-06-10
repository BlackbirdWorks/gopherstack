package account_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/account"
)

func newTestHandler(t *testing.T) *account.Handler {
	t.Helper()

	return account.NewHandler(account.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(t *testing.T, h *account.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=key/20230101/us-east-1/account/aws4_request")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	assert.Equal(t, "Account", h.Name())
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	h.Reset()
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "DescribeAccount")
	assert.Contains(t, ops, "ListRegions")
	assert.Contains(t, ops, "GetAlternateContact")
	assert.Contains(t, ops, "PutAlternateContact")
	assert.Contains(t, ops, "DeleteAlternateContact")
	assert.Contains(t, ops, "GetContactInformation")
	assert.Contains(t, ops, "PutContactInformation")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		authSvc   string
		wantMatch bool
	}{
		{
			name:      "account path with account service",
			path:      "/account",
			authSvc:   "account",
			wantMatch: true,
		},
		{
			name:      "regions path",
			path:      "/regions",
			authSvc:   "account",
			wantMatch: true,
		},
		{
			name:      "alternateContact path",
			path:      "/account/alternateContact",
			authSvc:   "account",
			wantMatch: true,
		},
		{
			name:      "contact path",
			path:      "/account/contact",
			authSvc:   "account",
			wantMatch: true,
		},
		{
			name:      "wrong service",
			path:      "/account",
			authSvc:   "s3",
			wantMatch: false,
		},
		{
			name:      "wrong path",
			path:      "/other",
			authSvc:   "account",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.authSvc != "" {
				req.Header.Set(
					"Authorization",
					"AWS4-HMAC-SHA256 Credential=key/20230101/us-east-1/"+tt.authSvc+"/aws4_request",
				)
			}
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "DescribeAccount", method: http.MethodGet, path: "/account", wantOp: "DescribeAccount"},
		{name: "ListRegions", method: http.MethodGet, path: "/regions", wantOp: "ListRegions"},
		{
			name:   "GetAlternateContact",
			method: http.MethodGet,
			path:   "/account/alternateContact",
			wantOp: "GetAlternateContact",
		},
		{
			name:   "PutAlternateContact",
			method: http.MethodPut,
			path:   "/account/alternateContact",
			wantOp: "PutAlternateContact",
		},
		{
			name:   "DeleteAlternateContact",
			method: http.MethodDelete,
			path:   "/account/alternateContact",
			wantOp: "DeleteAlternateContact",
		},
		{
			name:   "GetContactInformation",
			method: http.MethodGet,
			path:   "/account/contact",
			wantOp: "GetContactInformation",
		},
		{
			name:   "PutContactInformation",
			method: http.MethodPut,
			path:   "/account/contact",
			wantOp: "PutContactInformation",
		},
		{name: "Unknown", method: http.MethodGet, path: "/unknown-path", wantOp: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/account/alternateContact", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	assert.Equal(t, "account/alternateContact", h.ExtractResource(c))
}

func TestHandler_DescribeAccount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/account", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Account account.Details `json:"Account"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotEmpty(t, out.Account.ID)
	assert.NotEmpty(t, out.Account.Arn)
}

func TestHandler_ListRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantEmpty bool
	}{
		{name: "no_filter", query: ""},
		{name: "filter_enabled_default", query: "?regionOptStatusContains=ENABLED_BY_DEFAULT", wantEmpty: false},
		{name: "filter_enabled", query: "?regionOptStatusContains=ENABLED", wantEmpty: true},
		{name: "filter_disabled", query: "?regionOptStatusContains=DISABLED", wantEmpty: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/regions"+tt.query, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Regions []account.Region `json:"Regions"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			if !tt.wantEmpty {
				assert.NotEmpty(t, out.Regions)
			}
		})
	}
}

// TestHandler_ListRegions_Pagination verifies that ListRegions honours maxResults and
// returns an opaque nextToken, and that paging through with the token yields every
// region exactly once with no overlap.
func TestHandler_ListRegions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Discover the full unpaged region set first.
	allRec := doRequest(t, h, http.MethodGet, "/regions", nil)
	require.Equal(t, http.StatusOK, allRec.Code)

	var all struct {
		NextToken string           `json:"NextToken"`
		Regions   []account.Region `json:"Regions"`
	}
	require.NoError(t, json.NewDecoder(allRec.Body).Decode(&all))
	require.Empty(t, all.NextToken, "unpaged listing must not return a token")
	require.Greater(t, len(all.Regions), 2, "need several regions to exercise paging")

	const pageSize = 2
	seen := make([]string, 0, len(all.Regions))
	token := ""

	for {
		path := "/regions?maxResults=2"
		if token != "" {
			path += "&nextToken=" + token
		}

		rec := doRequest(t, h, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var page struct {
			NextToken string           `json:"NextToken"`
			Regions   []account.Region `json:"Regions"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&page))
		require.LessOrEqual(t, len(page.Regions), pageSize, "page must not exceed maxResults")

		for _, r := range page.Regions {
			seen = append(seen, r.RegionName)
		}

		if page.NextToken == "" {
			break
		}
		token = page.NextToken
	}

	// Every region appears exactly once across the pages — no overlap, no gaps.
	assert.Len(t, seen, len(all.Regions))
	assert.ElementsMatch(t, regionNames(all.Regions), seen)
}

func regionNames(regions []account.Region) []string {
	names := make([]string, 0, len(regions))
	for _, r := range regions {
		names = append(names, r.RegionName)
	}

	return names
}

func TestHandler_AlternateContact_PutGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contactType string
	}{
		{name: "billing", contactType: "BILLING"},
		{name: "operations", contactType: "OPERATIONS"},
		{name: "security", contactType: "SECURITY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Get before put → not found
			getRec := doRequest(
				t,
				h,
				http.MethodGet,
				"/account/alternateContact?alternateContactType="+tt.contactType,
				nil,
			)
			assert.Equal(t, http.StatusNotFound, getRec.Code)

			// Put contact
			putRec := doRequest(t, h, http.MethodPut, "/account/alternateContact", map[string]any{
				"AlternateContactType": tt.contactType,
				"Name":                 "Test Contact",
				"EmailAddress":         "test@example.com",
				"PhoneNumber":          "+1-555-555-5555",
				"Title":                "Manager",
			})
			assert.Equal(t, http.StatusOK, putRec.Code)

			// Get after put
			getRec2 := doRequest(
				t,
				h,
				http.MethodGet,
				"/account/alternateContact?alternateContactType="+tt.contactType,
				nil,
			)
			require.Equal(t, http.StatusOK, getRec2.Code)

			var out struct {
				AlternateContact map[string]any `json:"AlternateContact"`
			}
			require.NoError(t, json.NewDecoder(getRec2.Body).Decode(&out))
			assert.Equal(t, "Test Contact", out.AlternateContact["Name"])

			// Delete
			delRec := doRequest(
				t,
				h,
				http.MethodDelete,
				"/account/alternateContact?alternateContactType="+tt.contactType,
				nil,
			)
			assert.Equal(t, http.StatusOK, delRec.Code)

			// Get after delete → not found
			getRec3 := doRequest(
				t,
				h,
				http.MethodGet,
				"/account/alternateContact?alternateContactType="+tt.contactType,
				nil,
			)
			assert.Equal(t, http.StatusNotFound, getRec3.Code)
		})
	}
}

func TestHandler_AlternateContact_InvalidMethod(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/account/alternateContact", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_ContactInformation_PutGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Get before put → not found
	getRec := doRequest(t, h, http.MethodGet, "/account/contact", nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)

	// Put contact info
	putRec := doRequest(t, h, http.MethodPut, "/account/contact", map[string]any{
		"FullName":      "ACME Corporation",
		"AddressLine1":  "123 Main St",
		"City":          "Seattle",
		"StateOrRegion": "WA",
		"PostalCode":    "98101",
		"CountryCode":   "US",
		"PhoneNumber":   "+1-555-555-5555",
		"WebsiteUrl":    "https://example.com",
	})
	assert.Equal(t, http.StatusOK, putRec.Code)

	// Get after put
	getRec2 := doRequest(t, h, http.MethodGet, "/account/contact", nil)
	require.Equal(t, http.StatusOK, getRec2.Code)

	var out struct {
		ContactInformation map[string]any `json:"ContactInformation"`
	}
	require.NoError(t, json.NewDecoder(getRec2.Body).Decode(&out))
	assert.Equal(t, "ACME Corporation", out.ContactInformation["FullName"])
}

func TestHandler_ContactInformation_InvalidMethod(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete, "/account/contact", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/unknown/path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")

	err := b.PutAlternateContact(&account.AlternateContact{
		AlternateContactType: account.ContactTypeBilling,
		Name:                 "Test",
		EmailAddress:         "test@example.com",
	})
	require.NoError(t, err)

	b.Reset()

	_, err = b.GetAlternateContact(account.ContactTypeBilling)
	require.Error(t, err)
}

func TestBackend_DeleteAlternateContact_NotFound(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.DeleteAlternateContact(account.ContactTypeBilling)
	require.Error(t, err)
}

func TestBackend_PutContactInformation_Get(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.GetContactInformation()
	require.Error(t, err)

	err = b.PutContactInformation(&account.ContactInformation{
		FullName: "Test Corp",
	})
	require.NoError(t, err)

	info, err := b.GetContactInformation()
	require.NoError(t, err)
	assert.Equal(t, "Test Corp", info.FullName)
}
