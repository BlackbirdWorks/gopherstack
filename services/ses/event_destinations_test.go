package ses_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ses"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_UpdateConfigurationSetEventDestination_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "config_set_not_found",
			body:         "Action=UpdateConfigurationSetEventDestination&Version=2010-12-01&ConfigurationSetName=missing&EventDestination.Name=dest", //nolint:lll // existing issue.
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "event_destination_not_found",
			setup: func(h *ses.Handler) {
				postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name=csupd")
			},
			body:         "Action=UpdateConfigurationSetEventDestination&Version=2010-12-01&ConfigurationSetName=csupd&EventDestination.Name=nodest", //nolint:lll // existing issue.
			wantCode:     http.StatusBadRequest,
			wantContains: "EventDestinationDoesNotExist",
		},
		{
			name: "valid_update",
			setup: func(h *ses.Handler) {
				postForm(t, h, "Action=CreateConfigurationSet&Version=2010-12-01&ConfigurationSet.Name=csupd2")
				postForm(t, h, url.Values{
					"Action":                   {"CreateConfigurationSetEventDestination"},
					"Version":                  {"2010-12-01"},
					"ConfigurationSetName":     {"csupd2"},
					"EventDestination.Name":    {"mydest"},
					"EventDestination.Enabled": {"true"},
					"EventDestination.MatchingEventTypes.member.1": {"send"},
				}.Encode())
			},
			body: url.Values{
				"Action":                   {"UpdateConfigurationSetEventDestination"},
				"Version":                  {"2010-12-01"},
				"ConfigurationSetName":     {"csupd2"},
				"EventDestination.Name":    {"mydest"},
				"EventDestination.Enabled": {"false"},
				"EventDestination.MatchingEventTypes.member.1": {"bounce"},
			}.Encode(),
			wantCode:     http.StatusOK,
			wantContains: "UpdateConfigurationSetEventDestinationResponse",
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

func TestCreateConfigurationSetEventDestination_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	rec := postForm(t, h, url.Values{
		"Action":                   {"CreateConfigurationSetEventDestination"},
		"Version":                  {"2010-12-01"},
		"ConfigurationSetName":     {"cs1"},
		"EventDestination.Name":    {"ed1"},
		"EventDestination.Enabled": {"true"},
		"EventDestination.MatchingEventTypes.member.1": {"Send"},
		"EventDestination.MatchingEventTypes.member.2": {"Bounce"},
		"EventDestination.SNSDestination.TopicARN":     {"arn:aws:sns:us-east-1:123:topic"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, h.Backend.(*ses.InMemoryBackend).EventDestinationCount())
}

func TestCreateConfigurationSetEventDestination_Duplicate_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))
	require.NoError(t, h.Backend.CreateConfigurationSetEventDestination("cs1", ses.EventDestination{
		Name:               "ed1",
		MatchingEventTypes: []string{"Send"},
	}))

	rec := postForm(t, h, url.Values{
		"Action":                {"CreateConfigurationSetEventDestination"},
		"Version":               {"2010-12-01"},
		"ConfigurationSetName":  {"cs1"},
		"EventDestination.Name": {"ed1"},
		"EventDestination.MatchingEventTypes.member.1": {"Bounce"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUpdateConfigurationSetEventDestination_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))
	require.NoError(t, h.Backend.CreateConfigurationSetEventDestination("cs1", ses.EventDestination{
		Name:               "ed1",
		Enabled:            false,
		MatchingEventTypes: []string{"Send"},
	}))

	rec := postForm(t, h, url.Values{
		"Action":                   {"UpdateConfigurationSetEventDestination"},
		"Version":                  {"2010-12-01"},
		"ConfigurationSetName":     {"cs1"},
		"EventDestination.Name":    {"ed1"},
		"EventDestination.Enabled": {"true"},
		"EventDestination.MatchingEventTypes.member.1": {"Send"},
		"EventDestination.MatchingEventTypes.member.2": {"Bounce"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	desc, err := h.Backend.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	require.Len(t, desc.EventDestinations, 1)
	assert.True(t, desc.EventDestinations[0].Enabled)
}

func TestDeleteConfigurationSetEventDestination_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))
	require.NoError(t, h.Backend.CreateConfigurationSetEventDestination("cs1", ses.EventDestination{
		Name:               "ed-del",
		MatchingEventTypes: []string{"Send"},
	}))

	rec := postForm(t, h, url.Values{
		"Action":               {"DeleteConfigurationSetEventDestination"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs1"},
		"EventDestinationName": {"ed-del"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).EventDestinationCount())
}

// TestSESNewOps_CreateConfigurationSetEventDestination covers event destination creation.
func TestCreateConfigurationSetEventDestination(t *testing.T) {
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
				"Action":                   {"CreateConfigurationSetEventDestination"},
				"Version":                  {"2010-12-01"},
				"ConfigurationSetName":     {"my-config-set"},
				"EventDestination.Name":    {"my-dest"},
				"EventDestination.Enabled": {"true"},
				"EventDestination.MatchingEventTypes.member.1": {"send"},
				"EventDestination.MatchingEventTypes.member.2": {"bounce"},
				"EventDestination.SNSDestination.TopicARN":     {"arn:aws:sns:us-east-1:123456789012:my-topic"},
			}.Encode(),
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("my-config-set"))
			},
			wantCode:     http.StatusOK,
			wantContains: "CreateConfigurationSetEventDestinationResponse",
		},
		{
			name: "config_set_not_found",
			body: url.Values{
				"Action":                {"CreateConfigurationSetEventDestination"},
				"Version":               {"2010-12-01"},
				"ConfigurationSetName":  {"nonexistent"},
				"EventDestination.Name": {"dest"},
			}.Encode(),
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "duplicate_destination_returns_error",
			body: url.Values{
				"Action":                {"CreateConfigurationSetEventDestination"},
				"Version":               {"2010-12-01"},
				"ConfigurationSetName":  {"cs"},
				"EventDestination.Name": {"existing-dest"},
			}.Encode(),
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
				require.NoError(
					t,
					h.Backend.CreateConfigurationSetEventDestination("cs", ses.EventDestination{Name: "existing-dest"}),
				)
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "EventDestinationAlreadyExists",
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

// TestSESNewOps_DeleteConfigurationSetEventDestination covers event destination deletion.
func TestDeleteConfigurationSetEventDestination(t *testing.T) {
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
			body: "Action=DeleteConfigurationSetEventDestination&Version=2010-12-01" +
				"&ConfigurationSetName=cs&EventDestinationName=dest",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
				require.NoError(
					t,
					h.Backend.CreateConfigurationSetEventDestination("cs", ses.EventDestination{Name: "dest"}),
				)
			},
			wantCode:     http.StatusOK,
			wantContains: "DeleteConfigurationSetEventDestinationResponse",
		},
		{
			name: "config_set_not_found",
			body: "Action=DeleteConfigurationSetEventDestination&Version=2010-12-01" +
				"&ConfigurationSetName=missing&EventDestinationName=dest",
			wantCode:     http.StatusBadRequest,
			wantContains: "ConfigurationSetDoesNotExist",
		},
		{
			name: "destination_not_found",
			body: "Action=DeleteConfigurationSetEventDestination&Version=2010-12-01" +
				"&ConfigurationSetName=cs&EventDestinationName=missing",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.CreateConfigurationSet("cs"))
			},
			wantCode:     http.StatusBadRequest,
			wantContains: "EventDestinationDoesNotExist",
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
