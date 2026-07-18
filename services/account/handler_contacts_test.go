package account_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
