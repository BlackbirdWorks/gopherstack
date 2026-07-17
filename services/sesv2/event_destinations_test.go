package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestCreateConfigurationSetEventDestination tests event destination creation.
func TestCreateConfigurationSetEventDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setup         func(*sesv2.InMemoryBackend)
		configSetName string
		destName      string
		wantErr       bool
	}{
		{
			name: "happy_path",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateConfigurationSet("my-set")
			},
			configSetName: "my-set",
			destName:      "my-dest",
		},
		{
			name:          "config_set_not_found",
			setup:         func(*sesv2.InMemoryBackend) {},
			configSetName: "no-such-set",
			destName:      "my-dest",
			wantErr:       true,
		},
		{
			name: "duplicate_destination",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateConfigurationSet("my-set")
				_, _ = b.CreateConfigurationSetEventDestination("my-set", "my-dest", true, nil)
			},
			configSetName: "my-set",
			destName:      "my-dest",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			_, err := backend.CreateConfigurationSetEventDestination(
				tt.configSetName, tt.destName, true, []string{"SEND"},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestCreateConfigurationSetEventDestinationHTTP tests event destination creation via HTTP.
func TestCreateConfigurationSetEventDestinationHTTP(t *testing.T) {
	t.Parallel()

	h, backend := newSESv2TestHandler(t)
	_, err := backend.CreateConfigurationSet("my-cs")
	require.NoError(t, err)

	body := map[string]any{
		"EventDestinationName": "my-dest",
		"EventDestination": map[string]any{
			"Enabled":            true,
			"MatchingEventTypes": []string{"SEND"},
		},
	}
	rec := doReq(
		t,
		h,
		http.MethodPost,
		"/v2/email/configuration-sets/my-cs/event-destinations",
		body,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetConfigurationSetEventDestinations tests the GetConfigurationSetEventDestinations operation.
func TestGetConfigurationSetEventDestinations(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/configuration-sets", map[string]any{
		"ConfigurationSetName": "EventDestCS",
	})

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/configuration-sets/EventDestCS/event-destinations",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDeleteConfigurationSetEventDestination tests the DeleteConfigurationSetEventDestination operation.
func TestDeleteConfigurationSetEventDestination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/configuration-sets", map[string]any{
		"ConfigurationSetName": "DelEventDestCS",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/configuration-sets/DelEventDestCS/event-destinations",
		map[string]any{
			"EventDestinationName": "sns-dest",
			"EventDestination": map[string]any{
				"Enabled":            true,
				"MatchingEventTypes": []string{"SEND"},
			},
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v2/email/configuration-sets/DelEventDestCS/event-destinations/sns-dest",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestUpdateConfigurationSetEventDestination tests the UpdateConfigurationSetEventDestination operation.
func TestUpdateConfigurationSetEventDestination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/configuration-sets", map[string]any{
		"ConfigurationSetName": "UpdEventDestCS",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/configuration-sets/UpdEventDestCS/event-destinations",
		map[string]any{
			"EventDestinationName": "upd-dest",
			"EventDestination": map[string]any{
				"Enabled":            true,
				"MatchingEventTypes": []string{"SEND"},
			},
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/configuration-sets/UpdEventDestCS/event-destinations/upd-dest",
		map[string]any{
			"EventDestination": map[string]any{
				"Enabled":            false,
				"MatchingEventTypes": []string{"BOUNCE"},
			},
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}
