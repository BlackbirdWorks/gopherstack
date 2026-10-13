package account_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/account"
)

func TestHandler_Validation(t *testing.T) {
	t.Parallel()

	validContact := func() map[string]any {
		return map[string]any{
			"FullName": "Jane", "AddressLine1": "1 Main", "City": "Seattle", "PostalCode": "98101",
			"CountryCode": "US", "StateOrRegion": "WA", "PhoneNumber": "+12065550100",
		}
	}
	with := func(k string, v any) map[string]any {
		m := validContact()
		m[k] = v

		return m
	}
	alt := func(k, v string) map[string]any {
		m := map[string]any{
			"AlternateContactType": "BILLING", "EmailAddress": "a@b.com", "Name": "n",
			"PhoneNumber": "+1 (206) 555-0100", "Title": "t",
		}
		m[k] = v

		return m
	}

	tests := []struct {
		body     any
		name     string
		path     string
		wantCode string
		wantMsg  string
		want     int
	}{
		{
			name: "contact_ok",
			path: "/putContactInformation",
			body: map[string]any{"ContactInformation": validContact()},
			want: 200,
		},
		{
			name:     "contact_phone_no_plus",
			path:     "/putContactInformation",
			body:     map[string]any{"ContactInformation": with("PhoneNumber", "555-0100")},
			want:     400,
			wantCode: "ValidationException",
			wantMsg:  "PhoneNumber",
		},
		{
			name:     "contact_country_three",
			path:     "/putContactInformation",
			body:     map[string]any{"ContactInformation": with("CountryCode", "USA")},
			want:     400,
			wantCode: "ValidationException",
			wantMsg:  "CountryCode",
		},
		{
			name:     "contact_state_required",
			path:     "/putContactInformation",
			body:     map[string]any{"ContactInformation": with("StateOrRegion", "")},
			want:     400,
			wantCode: "ValidationException",
			wantMsg:  "StateOrRegion",
		},
		{
			name:     "contact_city_long",
			path:     "/putContactInformation",
			body:     map[string]any{"ContactInformation": with("City", strings.Repeat("c", 51))},
			want:     400,
			wantCode: "ValidationException",
			wantMsg:  "City",
		},
		{name: "alt_ok", path: "/putAlternateContact", body: alt("Name", "n"), want: 200},
		{
			name:     "alt_bad_email",
			path:     "/putAlternateContact",
			body:     alt("EmailAddress", "bad"),
			want:     400,
			wantCode: "ValidationException",
			wantMsg:  "EmailAddress",
		},
		{
			name:     "alt_bad_phone",
			path:     "/putAlternateContact",
			body:     alt("PhoneNumber", "abc"),
			want:     400,
			wantCode: "ValidationException",
			wantMsg:  "PhoneNumber",
		},
		{
			name:     "alt_long_title",
			path:     "/putAlternateContact",
			body:     alt("Title", strings.Repeat("t", 51)),
			want:     400,
			wantCode: "ValidationException",
			wantMsg:  "Title",
		},
		{
			name:     "name_long",
			path:     "/putAccountName",
			body:     map[string]any{"AccountName": strings.Repeat("a", 51)},
			want:     400,
			wantCode: "ValidationException",
			wantMsg:  "AccountName",
		},
		{
			name: "name_max",
			path: "/putAccountName",
			body: map[string]any{"AccountName": strings.Repeat("a", 50)},
			want: 200,
		},
		{
			name:     "list_bad_status",
			path:     "/listRegions",
			body:     map[string]any{"RegionOptStatusContains": []string{"FOO"}},
			want:     400,
			wantCode: "ValidationException",
			wantMsg:  "RegionOptStatusContains",
		},
		{
			name:     "list_junk_token",
			path:     "/listRegions",
			body:     map[string]any{"NextToken": "junk"},
			want:     400,
			wantCode: "ValidationException",
			wantMsg:  "nextToken",
		},
		{
			name:     "enable_default_region",
			path:     "/enableRegion",
			body:     map[string]any{"RegionName": "us-east-1"},
			want:     400,
			wantCode: "ValidationException",
			wantMsg:  "only opt-in",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), tt.path, tt.body)
			assert.Equal(t, tt.want, rec.Code, rec.Body.String())

			if tt.want == http.StatusOK {
				return
			}

			var out struct {
				Type    string `json:"__type"`
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantCode, out.Type)
			assert.Contains(t, out.Message, tt.wantMsg)
			assert.False(t, strings.HasPrefix(out.Message, tt.wantCode), "message repeats the code: %q", out.Message)
		})
	}
}

func TestBackend_SetLifecycleDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  account.RegionOptStatus
		delay time.Duration
		wait  time.Duration
	}{
		{name: "mid_transition", delay: 5 * time.Second, wait: time.Second, want: account.RegionOptStatusEnabling},
		{name: "settled", delay: 5 * time.Second, wait: 6 * time.Second, want: account.RegionOptStatusEnabled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := account.NewInMemoryBackend("000000000000", "us-east-1")
				defer b.Close()
				b.SetLifecycleDelay(tt.delay)

				require.NoError(t, b.EnableRegion("me-south-1"))
				time.Sleep(tt.wait)

				got, err := b.GetRegionOptStatus("me-south-1")
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			})
		})
	}
}
