package ses_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

func TestHandler_ListConfigurationSets_MaxItems(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create several configuration sets.
	for _, name := range []string{"cs1", "cs2", "cs3"} {
		rec := postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name="+name)
		assert.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "no_max_items",
			body:         "Action=ListConfigurationSets&Version=2010-12-01",
			wantCode:     http.StatusOK,
			wantContains: "ListConfigurationSetsResponse",
		},
		{
			name:         "with_max_items",
			body:         "Action=ListConfigurationSets&Version=2010-12-01&MaxItems=2",
			wantCode:     http.StatusOK,
			wantContains: "ListConfigurationSetsResponse",
		},
		{
			name:         "invalid_max_items_treated_as_zero",
			body:         "Action=ListConfigurationSets&Version=2010-12-01&MaxItems=notanumber",
			wantCode:     http.StatusOK,
			wantContains: "ListConfigurationSetsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribeConfigurationSet_WithOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "not_found",
			body:         "Action=DescribeConfigurationSet&Version=2010-12-01&ConfigurationSetName=missing",
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "with_tracking_options",
			setup: func(h *ses.Handler) {
				postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name=cstrack")
				postForm(
					t,
					h,
					"Action=CreateConfigurationSetTrackingOptions&Version=2010-12-01&ConfigurationSetName=cstrack&TrackingOptions.CustomRedirectDomain=track.example.com", //nolint:lll // existing issue.
				)
			},
			body:         "Action=DescribeConfigurationSet&Version=2010-12-01&ConfigurationSetName=cstrack",
			wantCode:     http.StatusOK,
			wantContains: "track.example.com",
		},
		{
			name: "with_delivery_options",
			setup: func(h *ses.Handler) {
				postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name=csdel")
				postForm(
					t,
					h,
					"Action=PutConfigurationSetDeliveryOptions&Version=2010-12-01&ConfigurationSetName=csdel&DeliveryOptions.TlsPolicy=Require", //nolint:lll // existing issue.
				)
			},
			body:         "Action=DescribeConfigurationSet&Version=2010-12-01&ConfigurationSetName=csdel",
			wantCode:     http.StatusOK,
			wantContains: "Require",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_PutConfigurationSetDeliveryOptions_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "config_set_not_found",
			body:         "Action=PutConfigurationSetDeliveryOptions&Version=2010-12-01&ConfigurationSetName=missing&DeliveryOptions.TlsPolicy=Require", //nolint:lll // existing issue.
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_UpdateConfigurationSetReputationMetricsEnabled_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "config_set_not_found",
			body:         "Action=UpdateConfigurationSetReputationMetricsEnabled&Version=2010-12-01&ConfigurationSetName=missing&Enabled=true", //nolint:lll // existing issue.
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_UpdateConfigurationSetSendingEnabled_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "config_set_not_found",
			body:         "Action=UpdateConfigurationSetSendingEnabled&Version=2010-12-01&ConfigurationSetName=missing&Enabled=true", //nolint:lll // existing issue.
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_UpdateConfigurationSetTrackingOptions_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "config_set_not_found",
			body:         "Action=UpdateConfigurationSetTrackingOptions&Version=2010-12-01&ConfigurationSetName=missing&TrackingOptions.CustomRedirectDomain=x.com", //nolint:lll // existing issue.
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name:         "tracking_options_not_found",
			body:         "",
			wantCode:     http.StatusOK, // placeholder, will be overridden in setup
			wantContains: "TrackingOptionsDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.name == "tracking_options_not_found" {
				h := newHandler()
				postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name=csnotrack")
				rec := postForm(
					t,
					h,
					"Action=UpdateConfigurationSetTrackingOptions&Version=2010-12-01&ConfigurationSetName=csnotrack&TrackingOptions.CustomRedirectDomain=x.com", //nolint:lll // existing issue.
				)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), tt.wantContains)

				return
			}

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestCreateConfigurationSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":                {"CreateConfigurationSet"},
		"Version":               {"2010-12-01"},
		"ConfigurationSet.Name": {"cs-new"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateConfigurationSetResponse")

	assert.Equal(t, 1, h.Backend.(*ses.InMemoryBackend).ConfigSetCount())
}

func TestCreateConfigurationSet_Duplicate_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":                {"CreateConfigurationSet"},
		"Version":               {"2010-12-01"},
		"ConfigurationSet.Name": {"cs1"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDeleteConfigurationSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs-del"))

	rec := postForm(t, h, url.Values{
		"Action":               {"DeleteConfigurationSet"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs-del"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).ConfigSetCount())
}

func TestListConfigurationSets_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs-a"))
	require.NoError(t, h.Backend.CreateConfigurationSet("cs-b"))

	rec := postForm(t, h, "Action=ListConfigurationSets&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListConfigurationSetsResponse")
	assert.Contains(t, rec.Body.String(), "cs-a")
}

func TestDescribeConfigurationSet_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs-desc"))
	require.NoError(t, h.Backend.PutConfigurationSetDeliveryOptions("cs-desc", "Require"))

	rec := postForm(t, h, url.Values{
		"Action":               {"DescribeConfigurationSet"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs-desc"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "cs-desc")
	assert.Contains(t, rec.Body.String(), "Require")
}

func TestDescribeConfigurationSet_NotFound_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":               {"DescribeConfigurationSet"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"noexist"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUpdateConfigurationSetSendingEnabled_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":               {"UpdateConfigurationSetSendingEnabled"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs1"},
		"Enabled":              {"false"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	desc, err := h.Backend.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	assert.False(t, desc.SendingEnabled)
}

func TestUpdateConfigurationSetReputationMetricsEnabled_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":               {"UpdateConfigurationSetReputationMetricsEnabled"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs1"},
		"Enabled":              {"true"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	desc, err := h.Backend.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	assert.True(t, desc.ReputationMetricsEnabled)
}

func TestPutConfigurationSetDeliveryOptions_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":                    {"PutConfigurationSetDeliveryOptions"},
		"Version":                   {"2010-12-01"},
		"ConfigurationSetName":      {"cs1"},
		"DeliveryOptions.TlsPolicy": {"Require"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	desc, err := h.Backend.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	require.NotNil(t, desc.DeliveryOptions)
	assert.Equal(t, "Require", desc.DeliveryOptions.TLSPolicy)
}

func TestCreateAndDeleteTrackingOptions_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":                               {"CreateConfigurationSetTrackingOptions"},
		"Version":                              {"2010-12-01"},
		"ConfigurationSetName":                 {"cs1"},
		"TrackingOptions.CustomRedirectDomain": {"tracking.example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, h.Backend.(*ses.InMemoryBackend).TrackingOptionsCount())

	rec = postForm(t, h, url.Values{
		"Action":               {"DeleteConfigurationSetTrackingOptions"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs1"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).TrackingOptionsCount())
}

func TestUpdateConfigurationSetTrackingOptions_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))
	require.NoError(t, h.Backend.CreateConfigurationSetTrackingOptions("cs1", "old.example.com"))

	rec := postForm(t, h, url.Values{
		"Action":                               {"UpdateConfigurationSetTrackingOptions"},
		"Version":                              {"2010-12-01"},
		"ConfigurationSetName":                 {"cs1"},
		"TrackingOptions.CustomRedirectDomain": {"new.example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	desc, err := h.Backend.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	require.NotNil(t, desc.TrackingOptions)
	assert.Equal(t, "new.example.com", desc.TrackingOptions.CustomRedirectDomain)
}

func TestPutConfigurationSetDeliveryOptions_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateConfigurationSet("cs1"))
	require.NoError(t, b.PutConfigurationSetDeliveryOptions("cs1", "Require"))

	desc, err := b.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	require.NotNil(t, desc.DeliveryOptions)
	assert.Equal(t, "Require", desc.DeliveryOptions.TLSPolicy)
}

func TestUpdateConfigurationSetSendingEnabled_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateConfigurationSet("cs1"))

	desc, err := b.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	assert.True(t, desc.SendingEnabled, "new config set starts with sending enabled")

	require.NoError(t, b.UpdateConfigurationSetSendingEnabled("cs1", false))

	desc, err = b.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	assert.False(t, desc.SendingEnabled)
}

func TestUpdateConfigurationSetReputationMetricsEnabled_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateConfigurationSet("cs1"))
	require.NoError(t, b.UpdateConfigurationSetReputationMetricsEnabled("cs1", true))

	desc, err := b.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	assert.True(t, desc.ReputationMetricsEnabled)
}

func TestDescribeConfigurationSet_ReturnsDeliveryAndReputation_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	body := url.Values{
		"Action":               {"DescribeConfigurationSet"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs1"},
	}.Encode()

	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeConfigurationSetResponse")
	assert.Contains(t, rec.Body.String(), "ReputationOptions")
}

func TestConfigurationSet_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateConfigurationSet("cs1"))
	require.NoError(t, b.PutConfigurationSetDeliveryOptions("cs1", "Require"))
	require.NoError(t, b.UpdateConfigurationSetSendingEnabled("cs1", false))
	require.NoError(t, b.UpdateConfigurationSetReputationMetricsEnabled("cs1", true))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := ses.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	desc, err := fresh.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	require.NotNil(t, desc.DeliveryOptions)
	assert.Equal(t, "Require", desc.DeliveryOptions.TLSPolicy)
	assert.False(t, desc.SendingEnabled)
	assert.True(t, desc.ReputationMetricsEnabled)
}

func TestListConfigurationSets_EmptyAndNextTokenAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		numSets int
	}{
		{name: "empty_state", numSets: 0},
		{name: "two_sets", numSets: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			for i := range tt.numSets {
				require.NoError(t, h.Backend.CreateConfigurationSet(
					strings.Repeat("cs", i+1),
				))
			}

			rec := postForm(t, h, url.Values{
				"Action":  {"ListConfigurationSets"},
				"Version": {"2010-12-01"},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "ConfigurationSets",
				"ConfigurationSets element must be present")
			assert.NotContains(t, body, "<NextToken>",
				"NextToken must be absent when all config sets fit on one page")
		})
	}
}

func TestDescribeConfigurationSet_EventDestinationsEmptyNotAbsent(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":               {"DescribeConfigurationSet"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs1"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "EventDestinations",
		"EventDestinations must be present in DescribeConfigurationSet even when empty")
	assert.NotContains(t, body, "<MatchingEventTypes>",
		"no MatchingEventTypes expected when config set has no event destinations")
}

func TestDescribeConfigurationSet_ReputationOptionsPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantSending    string
		wantMetrics    string
		sendingEnabled bool
		metricsEnabled bool
	}{
		{
			name:           "defaults",
			sendingEnabled: true,
			metricsEnabled: false,
			wantSending:    "<SendingEnabled>true</SendingEnabled>",
			wantMetrics:    "<ReputationMetricsEnabled>false</ReputationMetricsEnabled>",
		},
		{
			name:           "sending_disabled",
			sendingEnabled: false,
			metricsEnabled: true,
			wantSending:    "<SendingEnabled>false</SendingEnabled>",
			wantMetrics:    "<ReputationMetricsEnabled>true</ReputationMetricsEnabled>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

			if !tt.sendingEnabled {
				require.NoError(t, h.Backend.UpdateConfigurationSetSendingEnabled("cs1", false))
			}
			if tt.metricsEnabled {
				require.NoError(t, h.Backend.UpdateConfigurationSetReputationMetricsEnabled("cs1", true))
			}

			rec := postForm(t, h, url.Values{
				"Action":               {"DescribeConfigurationSet"},
				"Version":              {"2010-12-01"},
				"ConfigurationSetName": {"cs1"},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "<ReputationOptions>", "ReputationOptions must be present")
			assert.Contains(t, body, tt.wantSending)
			assert.Contains(t, body, tt.wantMetrics)
		})
	}
}

// TestSESNewOps_CreateConfigurationSetTrackingOptions covers tracking options creation.
func TestCreateConfigurationSetTrackingOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			body: url.Values{
				"Action":                               {"CreateConfigurationSetTrackingOptions"},
				"Version":                              {"2010-12-01"},
				"ConfigurationSetName":                 {"cs"},
				"TrackingOptions.CustomRedirectDomain": {"track.example.com"},
			}.Encode(),
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
			},
			wantCode:     http.StatusOK,
			wantContains: "CreateConfigurationSetTrackingOptionsResponse",
		},
		{
			name: "config_set_not_found",
			body: url.Values{
				"Action":                               {"CreateConfigurationSetTrackingOptions"},
				"Version":                              {"2010-12-01"},
				"ConfigurationSetName":                 {"missing"},
				"TrackingOptions.CustomRedirectDomain": {"track.example.com"},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "tracking_options_already_exist",
			body: url.Values{
				"Action":                               {"CreateConfigurationSetTrackingOptions"},
				"Version":                              {"2010-12-01"},
				"ConfigurationSetName":                 {"cs"},
				"TrackingOptions.CustomRedirectDomain": {"track2.example.com"},
			}.Encode(),
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
				require.NoError(t, h.Backend.CreateConfigurationSetTrackingOptions("cs", "track.example.com"))
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "TrackingOptionsAlreadyExists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestSESNewOps_DeleteConfigurationSetTrackingOptions covers tracking options deletion.
func TestDeleteConfigurationSetTrackingOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=DeleteConfigurationSetTrackingOptions&Version=2010-12-01&ConfigurationSetName=cs",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
				require.NoError(t, h.Backend.CreateConfigurationSetTrackingOptions("cs", "track.example.com"))
			},
			wantCode:     http.StatusOK,
			wantContains: "DeleteConfigurationSetTrackingOptionsResponse",
		},
		{
			name:         "config_set_not_found",
			body:         "Action=DeleteConfigurationSetTrackingOptions&Version=2010-12-01&ConfigurationSetName=missing",
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "tracking_options_not_found",
			body: "Action=DeleteConfigurationSetTrackingOptions&Version=2010-12-01&ConfigurationSetName=cs",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "TrackingOptionsDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

// TestBackend_DeleteConfigurationSet_Cascade tests that deleting a config set cascades.
func TestBackend_DeleteConfigurationSet_Cascade(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateConfigurationSet("cs"))
	require.NoError(t, b.CreateConfigurationSetEventDestination(
		"cs", ses.EventDestination{Name: "dest", MatchingEventTypes: []string{"send"}},
	))
	require.NoError(t, b.CreateConfigurationSetTrackingOptions("cs", "track.example.com"))

	assert.Equal(t, 1, b.EventDestinationCount())
	assert.Equal(t, 1, b.TrackingOptionsCount())

	require.NoError(t, b.DeleteConfigurationSet("cs"))

	assert.Equal(t, 0, b.EventDestinationCount())
	assert.Equal(t, 0, b.TrackingOptionsCount())
}

// TestHandler_DeleteConfigurationSet_Cascade tests that the delete config set handler cascades.
func TestHandler_DeleteConfigurationSet_Cascade(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
	require.NoError(t, h.Backend.CreateConfigurationSetEventDestination(
		"cs", ses.EventDestination{Name: "dest", MatchingEventTypes: []string{"send"}},
	))
	require.NoError(t, h.Backend.CreateConfigurationSetTrackingOptions("cs", "track.example.com"))

	body := url.Values{
		"Action":               {"DeleteConfigurationSet"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs"},
	}.Encode()
	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteConfigurationSetResponse")

	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).EventDestinationCount())
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).TrackingOptionsCount())
}

func TestSESBackend_ConfigSetCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *ses.InMemoryBackend)
		verify func(t *testing.T, b *ses.InMemoryBackend)
		name   string
	}{
		{
			name: "create_and_list",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateConfigurationSet("cs1"))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				p := b.ListConfigurationSets("", 0)
				require.Len(t, p.Data, 1)
				assert.Equal(t, "cs1", p.Data[0])
			},
		},
		{
			name: "create_duplicate_returns_error",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateConfigurationSet("dup"))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				err := b.CreateConfigurationSet("dup")
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrConfigSetExists)
			},
		},
		{
			name: "delete_existing",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateConfigurationSet("del"))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				require.NoError(t, b.DeleteConfigurationSet("del"))
				assert.Equal(t, 0, b.ConfigSetCount())
			},
		},
		{
			name: "delete_missing_returns_error",
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				err := b.DeleteConfigurationSet("nonexistent")
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrConfigSetNotFound)
			},
		},
		{
			name: "list_sorted",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateConfigurationSet("zzz"))
				require.NoError(t, b.CreateConfigurationSet("aaa"))
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				p := b.ListConfigurationSets("", 0)
				require.Len(t, p.Data, 2)
				assert.Equal(t, "aaa", p.Data[0])
				assert.Equal(t, "zzz", p.Data[1])
			},
		},
		{
			name: "create_empty_name_error",
			verify: func(t *testing.T, b *ses.InMemoryBackend) {
				t.Helper()

				err := b.CreateConfigurationSet("")
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrInvalidParameter)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()

			if tt.setup != nil {
				tt.setup(b)
			}

			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

func TestSESHandler_ConfigSetCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name: "CreateConfigurationSet_ok",
			body: url.Values{
				"Action":                {"CreateConfigurationSet"},
				"Version":               {"2010-12-01"},
				"ConfigurationSet.Name": {"myCS"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "CreateConfigurationSetResponse",
		},
		{
			name: "CreateConfigurationSet_duplicate",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("dup"))
			},
			body: url.Values{
				"Action":                {"CreateConfigurationSet"},
				"Version":               {"2010-12-01"},
				"ConfigurationSet.Name": {"dup"},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetAlreadyExists",
		},
		{
			name: "DeleteConfigurationSet_ok",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("del"))
			},
			body: url.Values{
				"Action":               {"DeleteConfigurationSet"},
				"Version":              {"2010-12-01"},
				"ConfigurationSetName": {"del"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "DeleteConfigurationSetResponse",
		},
		{
			name: "DeleteConfigurationSet_missing",
			body: url.Values{
				"Action":               {"DeleteConfigurationSet"},
				"Version":              {"2010-12-01"},
				"ConfigurationSetName": {"nope"},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "ListConfigurationSets_ok",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))
				require.NoError(t, h.Backend.CreateConfigurationSet("cs2"))
			},
			body: url.Values{
				"Action":  {"ListConfigurationSets"},
				"Version": {"2010-12-01"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "ListConfigurationSetsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}
