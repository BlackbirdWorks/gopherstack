package ses_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/ses"
)

const mrHome = "us-east-1"

func newRegionHandler() *ses.Handler {
	h := ses.NewHandler(ses.NewInMemoryBackend().WithRegion(mrHome).WithAccountID("000000000000"))
	h.EnableRegions()

	return h
}

func regionForm(t *testing.T, h *ses.Handler, region string, form url.Values) (int, string) {
	t.Helper()

	form.Set("Version", "2010-12-01")

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec.Code, rec.Body.String()
}

func createConfigSet(t *testing.T, h *ses.Handler, region, name string) {
	t.Helper()

	code, body := regionForm(t, h, region, url.Values{
		"Action": {"CreateConfigurationSet"}, "ConfigurationSet.Name": {name},
	})
	require.Less(t, code, http.StatusMultipleChoices, body)
}

func configSetsIn(t *testing.T, h *ses.Handler, region string) string {
	t.Helper()

	code, body := regionForm(t, h, region, url.Values{"Action": {"ListConfigurationSets"}})
	require.Less(t, code, http.StatusMultipleChoices, body)

	return body
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{mrHome, "eu-west-1"}},
		{name: "three-regions", regions: []string{mrHome, "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()

			for _, r := range tc.regions {
				createConfigSet(t, h, r, "shared")
				createConfigSet(t, h, r, "only-"+r)
			}

			for _, r := range tc.regions {
				out := configSetsIn(t, h, r)
				assert.Contains(t, out, "only-"+r)

				for _, other := range tc.regions {
					if other != r {
						assert.NotContains(t, out, "only-"+other)
					}
				}
			}
		})
	}
}

func TestHandler_MultiRegionSendIdentityPerRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		verifyIn string
		sendFrom string
		wantOK   bool
	}{
		{name: "same-region", verifyIn: "eu-west-1", sendFrom: "eu-west-1", wantOK: true},
		{name: "other-region", verifyIn: mrHome, sendFrom: "eu-west-1", wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()
			code, body := regionForm(t, h, tc.verifyIn, url.Values{
				"Action": {"VerifyEmailIdentity"}, "EmailAddress": {"s@example.com"},
			})
			require.Less(t, code, http.StatusMultipleChoices, body)

			code, body = regionForm(t, h, tc.sendFrom, url.Values{
				"Action": {"SendEmail"}, "Source": {"s@example.com"},
				"Destination.ToAddresses.member.1": {"r@example.com"},
				"Message.Subject.Data":             {"hi"}, "Message.Body.Text.Data": {"b"},
			})

			if tc.wantOK {
				assert.Less(t, code, http.StatusMultipleChoices, body)
				assert.Len(t, h.MailBackends()[len(h.MailBackends())-1].ListEmails(), 1)

				return
			}

			assert.GreaterOrEqual(t, code, http.StatusBadRequest)
			assert.Contains(t, body, "EU-WEST-1")
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		extraRegion string
		wantRegions bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "with-sibling", extraRegion: "eu-west-1", wantRegions: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler()
			createConfigSet(t, src, mrHome, "shared")

			if tc.extraRegion != "" {
				createConfigSet(t, src, tc.extraRegion, "eu-only")
			}

			snap := src.Snapshot(t.Context())

			var doc map[string]json.RawMessage

			require.NoError(t, json.Unmarshal(snap, &doc))

			_, hasRegions := doc["regions"]
			assert.Equal(t, tc.wantRegions, hasRegions)

			dst := newRegionHandler()
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Contains(t, configSetsIn(t, dst, mrHome), "shared")

			if tc.extraRegion != "" {
				assert.Contains(t, configSetsIn(t, dst, tc.extraRegion), "eu-only")
				assert.NotContains(t, configSetsIn(t, dst, mrHome), "eu-only")
			}
		})
	}
}

func TestHandler_MultiRegionLegacyRestore(t *testing.T) {
	t.Parallel()

	legacy := ses.NewHandler(ses.NewInMemoryBackend().WithRegion(mrHome))
	createConfigSet(t, legacy, mrHome, "shared")

	dst := newRegionHandler()
	createConfigSet(t, dst, "eu-west-1", "eu-only")

	require.NoError(t, dst.Restore(t.Context(), legacy.Snapshot(t.Context())))
	assert.Contains(t, configSetsIn(t, dst, mrHome), "shared")
	assert.NotContains(t, configSetsIn(t, dst, "eu-west-1"), "eu-only")
}
