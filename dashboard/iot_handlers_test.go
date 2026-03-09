package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/internal/teststack"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDashboard_IoT covers the IoT Core dashboard handler.
func TestDashboard_IoT(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *teststack.Stack)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "index renders empty state",
			wantCode:     http.StatusOK,
			wantContains: "IoT Core",
		},
		{
			name: "index renders things",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.IoTHandler.Backend.CreateThing(&iotbackend.CreateThingInput{
					ThingName: "my-test-device",
				})
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "my-test-device",
		},
		{
			name: "index renders topic rules",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				err := s.IoTHandler.Backend.CreateTopicRule(&iotbackend.CreateTopicRuleInput{
					RuleName: "test-rule",
					TopicRulePayload: &iotbackend.TopicRulePayload{
						SQL: "SELECT temperature FROM 'sensors/+/data'",
					},
				})
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "test-rule",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/iot", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_IoT_CreateThing covers the create Thing handler.
func TestDashboard_IoT_CreateThing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		formValues  url.Values
		name        string
		wantThingIn string
		wantCode    int
	}{
		{
			name:        "creates thing and redirects",
			formValues:  url.Values{"name": {"new-device"}},
			wantCode:    http.StatusFound,
			wantThingIn: "new-device",
		},
		{
			name:       "empty name returns bad request",
			formValues: url.Values{"name": {""}},
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			req := httptest.NewRequest(http.MethodPost, "/dashboard/iot/thing/create",
				strings.NewReader(tt.formValues.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)

			if tt.wantThingIn != "" {
				things := s.IoTHandler.Backend.ListThings()
				var found bool
				for _, th := range things {
					if th.ThingName == tt.wantThingIn {
						found = true

						break
					}
				}
				assert.True(t, found, "expected thing %q to be created", tt.wantThingIn)
			}
		})
	}
}

// TestDashboard_IoT_DeleteThing covers the delete Thing handler.
func TestDashboard_IoT_DeleteThing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *teststack.Stack)
		formValues url.Values
		name       string
		wantCode   int
	}{
		{
			name:       "deletes existing thing and redirects",
			formValues: url.Values{"name": {"to-delete"}},
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.IoTHandler.Backend.CreateThing(&iotbackend.CreateThingInput{
					ThingName: "to-delete",
				})
				require.NoError(t, err)
			},
			wantCode: http.StatusFound,
		},
		{
			name:       "empty name returns bad request",
			formValues: url.Values{"name": {""}},
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "nonexistent thing returns not found",
			formValues: url.Values{"name": {"does-not-exist"}},
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/iot/thing/delete",
				strings.NewReader(tt.formValues.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
