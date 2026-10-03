package sesv2_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

const mrHome = "us-east-1"

func newRegionHandler() *sesv2.Handler {
	h := sesv2.NewHandler(sesv2.NewInMemoryBackend())
	h.EnableRegions()

	return h
}

func regionCall(t *testing.T, h *sesv2.Handler, region, method, path, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Less(t, rec.Code, http.StatusMultipleChoices, rec.Body.String())

	out := map[string]any{}
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	}

	return out
}

func configSetNames(t *testing.T, h *sesv2.Handler, region string) []string {
	t.Helper()

	list, _ := regionCall(t, h, region, http.MethodGet, "/v2/email/configuration-sets", "")["ConfigurationSets"].([]any)

	names := make([]string, 0, len(list))
	for _, v := range list {
		n, _ := v.(string)
		names = append(names, n)
	}

	return names
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{"us-east-1", "eu-west-1"}},
		{name: "three-regions", regions: []string{"us-east-1", "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()

			for _, r := range tc.regions {
				regionCall(
					t,
					h,
					r,
					http.MethodPost,
					"/v2/email/configuration-sets",
					`{"ConfigurationSetName":"shared"}`,
				)
				regionCall(
					t,
					h,
					r,
					http.MethodPost,
					"/v2/email/configuration-sets",
					`{"ConfigurationSetName":"only-`+r+`"}`,
				)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, configSetNames(t, h, r))
			}
		})
	}
}

func TestHandler_MultiRegionMailAggregates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "home-only", regions: []string{mrHome}},
		{name: "home-and-peer", regions: []string{mrHome, "eu-west-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()
			send := `{"FromEmailAddress":"s@example.com","Destination":{"ToAddresses":["r@example.com"]},` +
				`"Content":{"Simple":{"Subject":{"Data":"hi"},"Body":{"Text":{"Data":"b"}}}}}`

			for _, r := range tc.regions {
				regionCall(t, h, r, http.MethodPost, "/v2/email/identities", `{"EmailIdentity":"s@example.com"}`)
				regionCall(t, h, r, http.MethodPost, "/v2/email/outbound-emails", send)
			}

			got := map[string]int{}

			for _, b := range h.MailBackends() {
				got[b.Region()] += len(b.ListEmails())
			}

			assert.Len(t, got, len(tc.regions))

			for _, r := range tc.regions {
				assert.Equal(t, 1, got[r], r)
			}
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		remote bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "peer-round-trips", remote: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler()
			regionCall(
				t,
				src,
				mrHome,
				http.MethodPost,
				"/v2/email/configuration-sets",
				`{"ConfigurationSetName":"home"}`,
			)

			if tc.remote {
				regionCall(
					t,
					src,
					"eu-west-1",
					http.MethodPost,
					"/v2/email/configuration-sets",
					`{"ConfigurationSetName":"eu"}`,
				)
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)

			if !tc.remote {
				assert.Equal(t, src.Backend.Snapshot(context.Background()), snap)
			}

			dst := newRegionHandler()
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, configSetNames(t, dst, mrHome))
			assert.Equal(t, tc.remote, len(configSetNames(t, dst, "eu-west-1")) == 1)

			old := newRegionHandler()
			require.NoError(t, old.Backend.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, configSetNames(t, old, mrHome))
		})
	}
}
