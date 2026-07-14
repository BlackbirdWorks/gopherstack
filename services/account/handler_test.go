package account_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/awstest"
	"github.com/blackbirdworks/gopherstack/services/account"
)

func newTestHandler(t *testing.T) *account.Handler {
	t.Helper()

	return account.NewHandler(account.NewInMemoryBackend("000000000000", "us-east-1"))
}

// doRequest issues a POST request against path with the given JSON body.
// Account Management is a rest-json1 service: every operation is a POST to a
// fixed operation-named path, with every parameter (including AccountId,
// RegionName, AlternateContactType, ...) carried in the JSON request body.
func doRequest(t *testing.T, h *account.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	c, rec := awstest.NewJSONContext(t, http.MethodPost, path, body)
	c.Request().Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=key/20230101/us-east-1/account/aws4_request",
	)
	require.NoError(t, h.Handler()(c))

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

	for _, op := range []string{
		"GetContactInformation", "PutContactInformation",
		"GetAlternateContact", "PutAlternateContact", "DeleteAlternateContact",
		"ListRegions", "GetRegionOptStatus", "EnableRegion", "DisableRegion",
		"GetPrimaryEmail", "StartPrimaryEmailUpdate", "AcceptPrimaryEmailUpdate",
		"GetAccountInformation", "PutAccountName",
	} {
		assert.Contains(t, ops, op)
	}

	// DescribeAccount/CloseAccount are not real Account Management
	// operations (CloseAccount belongs to AWS Organizations) and must not
	// be advertised.
	assert.NotContains(t, ops, "DescribeAccount")
	assert.NotContains(t, ops, "CloseAccount")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		method    string
		authSvc   string
		wantMatch bool
	}{
		{
			name:      "get contact info matches",
			path:      "/getContactInformation",
			method:    http.MethodPost,
			authSvc:   "account",
			wantMatch: true,
		},
		{
			name:      "put contact info matches",
			path:      "/putContactInformation",
			method:    http.MethodPost,
			authSvc:   "account",
			wantMatch: true,
		},
		{
			name:      "list regions matches",
			path:      "/listRegions",
			method:    http.MethodPost,
			authSvc:   "account",
			wantMatch: true,
		},
		{
			name:      "get account information matches",
			path:      "/getAccountInformation",
			method:    http.MethodPost,
			authSvc:   "account",
			wantMatch: true,
		},
		{
			name:      "wrong service",
			path:      "/getContactInformation",
			method:    http.MethodPost,
			authSvc:   "s3",
			wantMatch: false,
		},
		{name: "wrong path", path: "/other", method: http.MethodPost, authSvc: "account", wantMatch: false},
		{
			name: "GET is rejected -- every account op is POST-only", path: "/getContactInformation",
			method: http.MethodGet, authSvc: "account", wantMatch: false,
		},
		{
			name: "legacy REST-ish path no longer matches", path: "/account/contact",
			method: http.MethodGet, authSvc: "account", wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)

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
		path   string
		wantOp string
	}{
		{name: "GetContactInformation", path: "/getContactInformation", wantOp: "GetContactInformation"},
		{name: "PutContactInformation", path: "/putContactInformation", wantOp: "PutContactInformation"},
		{name: "GetAlternateContact", path: "/getAlternateContact", wantOp: "GetAlternateContact"},
		{name: "PutAlternateContact", path: "/putAlternateContact", wantOp: "PutAlternateContact"},
		{name: "DeleteAlternateContact", path: "/deleteAlternateContact", wantOp: "DeleteAlternateContact"},
		{name: "ListRegions", path: "/listRegions", wantOp: "ListRegions"},
		{name: "GetRegionOptStatus", path: "/getRegionOptStatus", wantOp: "GetRegionOptStatus"},
		{name: "EnableRegion", path: "/enableRegion", wantOp: "EnableRegion"},
		{name: "DisableRegion", path: "/disableRegion", wantOp: "DisableRegion"},
		{name: "GetPrimaryEmail", path: "/getPrimaryEmail", wantOp: "GetPrimaryEmail"},
		{name: "StartPrimaryEmailUpdate", path: "/startPrimaryEmailUpdate", wantOp: "StartPrimaryEmailUpdate"},
		{name: "AcceptPrimaryEmailUpdate", path: "/acceptPrimaryEmailUpdate", wantOp: "AcceptPrimaryEmailUpdate"},
		{name: "GetAccountInformation", path: "/getAccountInformation", wantOp: "GetAccountInformation"},
		{name: "PutAccountName", path: "/putAccountName", wantOp: "PutAccountName"},
		{name: "Unknown", path: "/unknown-path", wantOp: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
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
	req := httptest.NewRequest(http.MethodPost, "/getAlternateContact", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	assert.Equal(t, "getAlternateContact", h.ExtractResource(c))
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "/unknown/path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_WrongMethod(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	c, rec := awstest.NewJSONContext(t, http.MethodGet, "/getContactInformation", nil)
	c.Request().Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=key/20230101/us-east-1/account/aws4_request",
	)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_GetAccountInformation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "/getAccountInformation", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	respBody := rec.Body.Bytes()

	var out account.Info
	require.NoError(t, json.Unmarshal(respBody, &out))
	assert.NotEmpty(t, out.AccountID)
	assert.NotEmpty(t, out.AccountCreatedDate)
	assert.Equal(t, account.StateActive, out.AccountState)

	// Response is a flat object, not wrapped like most other operations.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(respBody, &raw))
	assert.NotContains(t, raw, "Account")
}

func TestHandler_PutAccountName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		accountName string
		wantStatus  int
	}{
		{name: "valid_name", accountName: "My Corp", wantStatus: http.StatusOK},
		{name: "empty_name", accountName: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{}
			if tt.accountName != "" {
				body["AccountName"] = tt.accountName
			}

			rec := doRequest(t, h, "/putAccountName", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				infoRec := doRequest(t, h, "/getAccountInformation", map[string]any{})
				require.Equal(t, http.StatusOK, infoRec.Code)

				var out account.Info
				require.NoError(t, json.NewDecoder(infoRec.Body).Decode(&out))
				assert.Equal(t, tt.accountName, out.AccountName)
			}
		})
	}
}

func TestHandler_ContactInformation_PutGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	getRec := doRequest(t, h, "/getContactInformation", map[string]any{})
	assert.Equal(t, http.StatusNotFound, getRec.Code)

	full := map[string]any{
		"FullName":         "ACME Corporation",
		"AddressLine1":     "123 Main St",
		"AddressLine2":     "Suite 100",
		"AddressLine3":     "Building B",
		"City":             "Seattle",
		"StateOrRegion":    "WA",
		"PostalCode":       "98101",
		"CountryCode":      "US",
		"PhoneNumber":      "+1-555-555-5555",
		"CompanyName":      "ACME Corp",
		"DistrictOrCounty": "King",
		"WebsiteUrl":       "https://example.com",
	}

	putRec := doRequest(t, h, "/putContactInformation", map[string]any{"ContactInformation": full})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec2 := doRequest(t, h, "/getContactInformation", map[string]any{})
	require.Equal(t, http.StatusOK, getRec2.Code)

	var out struct {
		ContactInformation map[string]any `json:"ContactInformation"`
	}
	require.NoError(t, json.NewDecoder(getRec2.Body).Decode(&out))

	// Every field set on Put round-trips through Get exactly.
	for k, v := range full {
		assert.Equalf(t, v, out.ContactInformation[k], "field %s did not round-trip", k)
	}
}

func TestHandler_PutContactInformation_RequiredFields(t *testing.T) {
	t.Parallel()

	full := map[string]any{
		"FullName":     "ACME Corporation",
		"AddressLine1": "123 Main St",
		"City":         "Seattle",
		"PostalCode":   "98101",
		"CountryCode":  "US",
		"PhoneNumber":  "+1-555-555-5555",
	}

	tests := []struct {
		name       string
		omit       string
		wantStatus int
	}{
		{name: "complete_ok", omit: "", wantStatus: http.StatusOK},
		{name: "missing_full_name", omit: "FullName", wantStatus: http.StatusBadRequest},
		{name: "missing_address_line1", omit: "AddressLine1", wantStatus: http.StatusBadRequest},
		{name: "missing_city", omit: "City", wantStatus: http.StatusBadRequest},
		{name: "missing_postal_code", omit: "PostalCode", wantStatus: http.StatusBadRequest},
		{name: "missing_country_code", omit: "CountryCode", wantStatus: http.StatusBadRequest},
		{name: "missing_phone_number", omit: "PhoneNumber", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			info := make(map[string]any, len(full))
			maps.Copy(info, full)

			if tt.omit != "" {
				delete(info, tt.omit)
			}

			rec := doRequest(t, h, "/putContactInformation", map[string]any{"ContactInformation": info})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
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

			// Get before put -> not found.
			getRec := doRequest(t, h, "/getAlternateContact", map[string]any{
				"AlternateContactType": tt.contactType,
			})
			assert.Equal(t, http.StatusNotFound, getRec.Code)

			putRec := doRequest(t, h, "/putAlternateContact", map[string]any{
				"AlternateContactType": tt.contactType,
				"Name":                 "Test Contact",
				"EmailAddress":         "test@example.com",
				"PhoneNumber":          "+1-555-555-5555",
				"Title":                "Manager",
			})
			assert.Equal(t, http.StatusOK, putRec.Code)

			getRec2 := doRequest(t, h, "/getAlternateContact", map[string]any{
				"AlternateContactType": tt.contactType,
			})
			require.Equal(t, http.StatusOK, getRec2.Code)

			var out struct {
				AlternateContact map[string]any `json:"AlternateContact"`
			}
			require.NoError(t, json.NewDecoder(getRec2.Body).Decode(&out))
			assert.Equal(t, "Test Contact", out.AlternateContact["Name"])
			assert.Equal(t, tt.contactType, out.AlternateContact["AlternateContactType"])

			delRec := doRequest(t, h, "/deleteAlternateContact", map[string]any{
				"AlternateContactType": tt.contactType,
			})
			assert.Equal(t, http.StatusOK, delRec.Code)

			getRec3 := doRequest(t, h, "/getAlternateContact", map[string]any{
				"AlternateContactType": tt.contactType,
			})
			assert.Equal(t, http.StatusNotFound, getRec3.Code)
		})
	}
}

func TestHandler_GetAlternateContact_InvalidType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contactType string
		wantStatus  int
	}{
		{name: "invalid_type", contactType: "INVALID", wantStatus: http.StatusBadRequest},
		{name: "empty_type", contactType: "", wantStatus: http.StatusBadRequest},
		{name: "billing_valid_but_unset", contactType: "BILLING", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "/getAlternateContact", map[string]any{"AlternateContactType": tt.contactType})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteAlternateContact_InvalidType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contactType string
		wantStatus  int
	}{
		{name: "invalid_type", contactType: "BOGUS", wantStatus: http.StatusBadRequest},
		{name: "empty_type", contactType: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "/deleteAlternateContact", map[string]any{"AlternateContactType": tt.contactType})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PutAlternateContact_RequiredFields(t *testing.T) {
	t.Parallel()

	full := map[string]any{
		"AlternateContactType": "BILLING",
		"EmailAddress":         "ops@example.com",
		"Name":                 "Ops Team",
		"PhoneNumber":          "+1-555-0100",
		"Title":                "Operations",
	}

	tests := []struct {
		name       string
		omit       string
		wantStatus int
	}{
		{name: "complete_ok", omit: "", wantStatus: http.StatusOK},
		{name: "missing_type", omit: "AlternateContactType", wantStatus: http.StatusBadRequest},
		{name: "missing_email", omit: "EmailAddress", wantStatus: http.StatusBadRequest},
		{name: "missing_name", omit: "Name", wantStatus: http.StatusBadRequest},
		{name: "missing_phone", omit: "PhoneNumber", wantStatus: http.StatusBadRequest},
		{name: "missing_title", omit: "Title", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := make(map[string]any, len(full))
			maps.Copy(body, full)

			if tt.omit != "" {
				delete(body, tt.omit)
			}

			rec := doRequest(t, h, "/putAlternateContact", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

func TestHandler_ListRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    []string
		wantCount int
	}{
		{name: "no_filter", wantCount: 8},
		{name: "filter_enabled_default", filter: []string{"ENABLED_BY_DEFAULT"}, wantCount: 6},
		{name: "filter_enabled", filter: []string{"ENABLED"}, wantCount: 2},
		{name: "filter_disabled", filter: []string{"DISABLED"}, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{}

			if tt.filter != nil {
				body["RegionOptStatusContains"] = tt.filter
			}

			rec := doRequest(t, h, "/listRegions", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Regions []account.Region `json:"Regions"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.Regions, tt.wantCount)
		})
	}
}

func TestHandler_ListRegions_RegionFieldsPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "/listRegions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Regions []map[string]any `json:"Regions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.Regions)

	for _, r := range out.Regions {
		assert.Contains(t, r, "RegionName")
		assert.Contains(t, r, "RegionOptStatus")
	}
}

func TestHandler_ListRegions_Alphabetical(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "/listRegions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Regions []account.Region `json:"Regions"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotEmpty(t, out.Regions)

	for i := 1; i < len(out.Regions); i++ {
		assert.Less(t, out.Regions[i-1].RegionName, out.Regions[i].RegionName)
	}
}

// TestHandler_ListRegions_Pagination verifies that MaxResults limits page
// size and that the returned NextToken allows full traversal with no
// duplication or gaps.
func TestHandler_ListRegions_Pagination(t *testing.T) {
	t.Parallel()

	const totalRegions = 8

	tests := []struct {
		name       string
		maxResults int
		wantPages  int
	}{
		{"maxResults_1", 1, totalRegions},
		{"maxResults_3", 3, 3},
		{"maxResults_4", 4, 2},
		{"maxResults_8_exact", 8, 1},
		{"maxResults_50_exceeds_total", 50, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seen := map[string]int{}
			nextToken := ""
			pages := 0

			for {
				body := map[string]any{"MaxResults": tc.maxResults}
				if nextToken != "" {
					body["NextToken"] = nextToken
				}

				rec := doRequest(t, h, "/listRegions", body)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp struct {
					NextToken string           `json:"NextToken"`
					Regions   []map[string]any `json:"Regions"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				require.LessOrEqual(t, len(resp.Regions), tc.maxResults)

				for _, r := range resp.Regions {
					name, _ := r["RegionName"].(string)
					seen[name]++
				}

				pages++
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}

				require.Less(t, pages, 20, "pagination must terminate")
			}

			assert.Equal(t, tc.wantPages, pages)
			assert.Len(t, seen, totalRegions)

			for name, count := range seen {
				assert.Equalf(t, 1, count, "region %s visited %d times", name, count)
			}
		})
	}
}

func TestHandler_ListRegions_InvalidMaxResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
	}{
		{name: "negative", maxResults: -1},
		{name: "over_max", maxResults: 51},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "/listRegions", map[string]any{"MaxResults": tt.maxResults})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_GetRegionOptStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		regionName     string
		wantStatus     string
		wantHTTPStatus int
	}{
		{
			name: "enabled_by_default", regionName: "us-east-1",
			wantStatus: "ENABLED_BY_DEFAULT", wantHTTPStatus: http.StatusOK,
		},
		{
			name: "opt_in_enabled", regionName: "ap-southeast-1",
			wantStatus: "ENABLED", wantHTTPStatus: http.StatusOK,
		},
		{name: "unknown_region", regionName: "zz-fake-1", wantHTTPStatus: http.StatusNotFound},
		{name: "missing_region_name", regionName: "", wantHTTPStatus: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{}
			if tc.regionName != "" {
				body["RegionName"] = tc.regionName
			}

			rec := doRequest(t, h, "/getRegionOptStatus", body)
			require.Equal(t, tc.wantHTTPStatus, rec.Code)

			if tc.wantStatus != "" {
				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Equal(t, tc.regionName, out["RegionName"])
				assert.Equal(t, tc.wantStatus, out["RegionOptStatus"])
			}
		})
	}
}

func TestHandler_EnableDisableRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		regionName string
	}{
		{name: "ap-southeast-1", regionName: "ap-southeast-1"},
		{name: "ap-northeast-1", regionName: "ap-northeast-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			statusRec := doRequest(t, h, "/getRegionOptStatus", map[string]any{"RegionName": tc.regionName})
			require.Equal(t, http.StatusOK, statusRec.Code)

			var statusOut map[string]any
			require.NoError(t, json.NewDecoder(statusRec.Body).Decode(&statusOut))
			assert.Equal(t, "ENABLED", statusOut["RegionOptStatus"])

			disableRec := doRequest(t, h, "/disableRegion", map[string]any{"RegionName": tc.regionName})
			assert.Equal(t, http.StatusOK, disableRec.Code)

			afterDisableRec := doRequest(t, h, "/getRegionOptStatus", map[string]any{"RegionName": tc.regionName})
			require.Equal(t, http.StatusOK, afterDisableRec.Code)

			var afterDisable map[string]any
			require.NoError(t, json.NewDecoder(afterDisableRec.Body).Decode(&afterDisable))
			assert.Equal(t, "DISABLED", afterDisable["RegionOptStatus"])

			enableRec := doRequest(t, h, "/enableRegion", map[string]any{"RegionName": tc.regionName})
			assert.Equal(t, http.StatusOK, enableRec.Code)

			afterEnableRec := doRequest(t, h, "/getRegionOptStatus", map[string]any{"RegionName": tc.regionName})
			require.Equal(t, http.StatusOK, afterEnableRec.Code)

			var afterEnable map[string]any
			require.NoError(t, json.NewDecoder(afterEnableRec.Body).Decode(&afterEnable))
			assert.Equal(t, "ENABLED", afterEnable["RegionOptStatus"])
		})
	}
}

func TestHandler_EnableDisableRegion_DefaultRegionRejected(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"/enableRegion", "/disableRegion"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, path, map[string]any{"RegionName": "us-east-1"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_EnableDisableRegion_MissingRegionName(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"/enableRegion", "/disableRegion"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, path, map[string]any{})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_EnableDisableRegion_NotFound(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"/enableRegion", "/disableRegion"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, path, map[string]any{"RegionName": "zz-invalid-1"})
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_PrimaryEmail_GetDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "/getPrimaryEmail", map[string]any{"AccountId": "000000000000"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotEmpty(t, out["PrimaryEmail"])
}

func TestHandler_PrimaryEmail_GetMissingAccountID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "/getPrimaryEmail", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_PrimaryEmail_UpdateFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newEmail string
	}{
		{name: "update_email", newEmail: "new@example.com"},
		{name: "update_email_again", newEmail: "another@example.org"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			startRec := doRequest(t, h, "/startPrimaryEmailUpdate", map[string]any{
				"AccountId":    "000000000000",
				"PrimaryEmail": tc.newEmail,
			})
			require.Equal(t, http.StatusOK, startRec.Code)

			var startOut map[string]any
			require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
			assert.Equal(t, "PENDING", startOut["Status"])

			acceptRec := doRequest(t, h, "/acceptPrimaryEmailUpdate", map[string]any{
				"AccountId":    "000000000000",
				"Otp":          "123456",
				"PrimaryEmail": tc.newEmail,
			})
			require.Equal(t, http.StatusOK, acceptRec.Code)

			var acceptOut map[string]any
			require.NoError(t, json.NewDecoder(acceptRec.Body).Decode(&acceptOut))
			assert.Equal(t, "ACCEPTED", acceptOut["Status"])

			getRec := doRequest(t, h, "/getPrimaryEmail", map[string]any{"AccountId": "000000000000"})
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut map[string]any
			require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
			assert.Equal(t, tc.newEmail, getOut["PrimaryEmail"])
		})
	}
}

func TestHandler_PrimaryEmail_AcceptInvalidOTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		otp        string
		wantStatus int
	}{
		{name: "wrong_otp", otp: "000000", wantStatus: http.StatusBadRequest},
		{name: "no_pending", otp: "", wantStatus: http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.otp != "" {
				doRequest(t, h, "/startPrimaryEmailUpdate", map[string]any{
					"AccountId": "000000000000", "PrimaryEmail": "x@example.com",
				})

				rec := doRequest(t, h, "/acceptPrimaryEmailUpdate", map[string]any{
					"AccountId": "000000000000", "Otp": tc.otp, "PrimaryEmail": "x@example.com",
				})
				assert.Equal(t, tc.wantStatus, rec.Code)
			} else {
				rec := doRequest(t, h, "/acceptPrimaryEmailUpdate", map[string]any{
					"AccountId": "000000000000", "Otp": "999999", "PrimaryEmail": "y@example.com",
				})
				assert.Equal(t, tc.wantStatus, rec.Code)
			}
		})
	}
}

func TestHandler_PrimaryEmail_StartMissingFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing_account_id", body: map[string]any{"PrimaryEmail": "a@example.com"}},
		{name: "missing_primary_email", body: map[string]any{"AccountId": "000000000000"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "/startPrimaryEmailUpdate", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_AcceptPrimaryEmailUpdate_MissingFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing_account_id", body: map[string]any{"Otp": "123456", "PrimaryEmail": "a@example.com"}},
		{name: "missing_otp", body: map[string]any{"AccountId": "000000000000", "PrimaryEmail": "a@example.com"}},
		{name: "missing_primary_email", body: map[string]any{"AccountId": "000000000000", "Otp": "123456"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "/acceptPrimaryEmailUpdate", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestHandler_ErrorEnvelope verifies a modeled error carries the exception
// type in both the X-Amzn-Errortype header and the body's __type field --
// aws-sdk-go-v2's rest-json1 deserializer resolves the exception from these,
// not from the message text.
func TestHandler_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "/getAlternateContact", map[string]any{"AlternateContactType": "BILLING"})
	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "ResourceNotFoundException", rec.Header().Get("X-Amzn-Errortype"))

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
}
