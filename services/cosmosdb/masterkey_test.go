package cosmosdb_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

// TestVerifyMasterKey_KnownAnswerVector proves the HMAC-SHA256
// canonicalization/signing algorithm itself is correct, independent of
// whether enforcement is ever turned on (it's opt-in and off by default --
// see AZURE.md section 3/5). The expected signature was computed
// independently via Python's hmac/hashlib against the exact canonical
// string AZURE.md section 3 specifies:
//
//	lowercase(verb) + "\n" + lowercase(resourceType) + "\n" + resourceId +
//	"\n" + lowercase(x-ms-date) + "\n" + lowercase(date-header) + "\n"
//
// for verb=GET, resourceType=dbs, resourceId=dbs/mydb,
// x-ms-date="Thu, 01 Jan 1970 00:00:00 GMT", date-header="" (absent), keyed
// with the real emulator's well-known DefaultMasterKey:
//
//	canonical = "get\ndbs\ndbs/mydb\nthu, 01 jan 1970 00:00:00 gmt\n\n"
//	sig       = base64(HMAC-SHA256(base64decode(DefaultMasterKey), canonical))
//	          = "0UrOUjNuyWU/2xulf8ZyCV7Yf/Yr0BeqSlr7CJyEWhI="
func TestVerifyMasterKey_KnownAnswerVector(t *testing.T) {
	t.Parallel()

	const wantSig = "0UrOUjNuyWU/2xulf8ZyCV7Yf/Yr0BeqSlr7CJyEWhI="

	req := httptest.NewRequest(http.MethodGet, "/dbs/mydb", nil)
	req.Header.Set("X-Ms-Date", "Thu, 01 Jan 1970 00:00:00 GMT")

	authHeader := "type=master&ver=1.0&sig=" + wantSig
	req.Header.Set("Authorization", url.QueryEscape(authHeader))

	ok, err := cosmosdb.VerifyMasterKey(cosmosdb.DefaultMasterKey, req)
	require.NoError(t, err)
	assert.True(t, ok, "hand-computed known-answer vector must verify")
}

func TestVerifyMasterKey_WrongSignatureRejected(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/dbs/mydb", nil)
	req.Header.Set("X-Ms-Date", "Thu, 01 Jan 1970 00:00:00 GMT")

	authHeader := "type=master&ver=1.0&sig=bogus-signature=="
	req.Header.Set("Authorization", url.QueryEscape(authHeader))

	ok, err := cosmosdb.VerifyMasterKey(cosmosdb.DefaultMasterKey, req)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestVerifyMasterKey_MalformedHeaderReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header string
	}{
		{name: "empty", header: ""},
		{name: "missing sig", header: url.QueryEscape("type=master&ver=1.0")},
		{name: "not url encoded percent", header: "%"},
		{name: "unknown field", header: url.QueryEscape("type=master&ver=1.0&sig=abc&bogus=1")},
		{name: "empty field value", header: url.QueryEscape("type=master&ver=&sig=abc")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, "/dbs/mydb", nil)
			req.Header.Set("Authorization", tt.header)

			_, err := cosmosdb.VerifyMasterKey(cosmosdb.DefaultMasterKey, req)
			require.ErrorIs(t, err, cosmosdb.ErrMalformedAuthorization)
		})
	}
}

func TestResourceTypeAndIDFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		method           string
		path             string
		wantResourceType string
		wantResourceID   string
	}{
		{
			name: "get database item", method: http.MethodGet, path: "/dbs/mydb",
			wantResourceType: "dbs", wantResourceID: "dbs/mydb",
		},
		{
			name: "get container item", method: http.MethodGet, path: "/dbs/mydb/colls/mycoll",
			wantResourceType: "colls", wantResourceID: "dbs/mydb/colls/mycoll",
		},
		{
			name: "list databases collection", method: http.MethodGet, path: "/dbs",
			wantResourceType: "dbs", wantResourceID: "dbs",
		},
		{
			name: "create container posts against parent database", method: http.MethodPost, path: "/dbs/mydb/colls",
			wantResourceType: "colls", wantResourceID: "dbs/mydb",
		},
		{
			name:   "create/query document posts against parent container",
			method: http.MethodPost, path: "/dbs/mydb/colls/mycoll/docs",
			wantResourceType: "docs", wantResourceID: "dbs/mydb/colls/mycoll",
		},
		{
			name: "empty path", method: http.MethodGet, path: "/",
			wantResourceType: "", wantResourceID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotType, gotID := cosmosdb.ResourceTypeAndIDFor(tt.method, tt.path)
			assert.Equal(t, tt.wantResourceType, gotType)
			assert.Equal(t, tt.wantResourceID, gotID)
		})
	}
}
