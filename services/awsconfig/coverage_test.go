package awsconfig_test

import (
	"net/http"
	"strings"
	"testing"

	"net/http/httptest"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigHandler_DeleteConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
					"ConfigurationRecorder": map[string]any{
						"name":    "default",
						"roleARN": "arn:aws:iam::000000000000:role/config",
					},
				})
			},
			body:     map[string]any{"ConfigurationRecorderName": "default"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConfigurationRecorderName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteConfigurationRecorder", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DeleteDeliveryChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
					"DeliveryChannel": map[string]any{
						"name":         "default",
						"s3BucketName": "my-bucket",
						"snsTopicARN":  "",
					},
				})
			},
			body:     map[string]any{"DeliveryChannelName": "default"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"DeliveryChannelName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteDeliveryChannel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_ExtractResource_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
		want   string
	}{
		{
			name:   "PutConfigurationRecorder_invalid_json",
			action: "PutConfigurationRecorder",
			body:   `not-valid-json`,
			want:   "",
		},
		{
			name:   "StartConfigurationRecorder_invalid_json",
			action: "StartConfigurationRecorder",
			body:   `not-valid-json`,
			want:   "",
		},
		{
			name:   "PutDeliveryChannel_invalid_json",
			action: "PutDeliveryChannel",
			body:   `not-valid-json`,
			want:   "",
		},
		{
			name:   "DeleteConfigurationRecorder_fallback",
			action: "DeleteConfigurationRecorder",
			body:   `{"ConfigurationRecorderName":"rec1"}`,
			want:   "rec1",
		},
		{
			name:   "DeleteConfigurationRecorder_invalid_json",
			action: "DeleteConfigurationRecorder",
			body:   `not-valid-json`,
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("X-Amz-Target", "StarlingDoveService."+tt.action)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestAWSConfigBackend_DeleteDeliveryChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *awsconfig.InMemoryBackend)
		name    string
		delName string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutDeliveryChannel("ch1", "bucket", ""))
			},
			delName: "ch1",
		},
		{
			name:    "not_found",
			delName: "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteDeliveryChannel(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestAWSConfigBackend_DeleteConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *awsconfig.InMemoryBackend)
		name    string
		delName string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationRecorder("rec1", "arn:aws:iam::000000000000:role/r"))
			},
			delName: "rec1",
		},
		{
			name:    "not_found",
			delName: "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteConfigurationRecorder(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
