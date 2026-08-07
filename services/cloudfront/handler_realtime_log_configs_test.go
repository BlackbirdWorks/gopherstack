package cloudfront_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestSamplingRateValidation verifies that realtime log configs enforce valid sampling rates.
func TestSamplingRateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rate     int
		wantCode int
	}{
		{name: "rate_1_accepted", rate: 1, wantCode: http.StatusCreated},
		{name: "rate_50_accepted", rate: 50, wantCode: http.StatusCreated},
		{name: "rate_100_accepted", rate: 100, wantCode: http.StatusCreated},
		{name: "rate_0_rejected", rate: 0, wantCode: http.StatusBadRequest},
		{name: "rate_101_rejected", rate: 101, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend(t)
			h := cloudfront.NewHandler(b)

			body := fmt.Sprintf(`<RealtimeLogConfig>
				<Name>log-config-%d</Name>
				<SamplingRate>%d</SamplingRate>
				<EndPoints>
					<member>
						<StreamType>Kinesis</StreamType>
						<KinesisStreamConfig>
							<StreamARN>arn:aws:kinesis:us-east-1:123456789012:stream/test</StreamARN>
							<RoleARN>arn:aws:iam::123456789012:role/test</RoleARN>
						</KinesisStreamConfig>
					</member>
				</EndPoints>
				<Fields><member>timestamp</member></Fields>
			</RealtimeLogConfig>`, tt.rate, tt.rate)

			rec := doReq(t, h, http.MethodPost, "/2020-05-31/realtime-log-config", body)
			assert.Equal(t, tt.wantCode, rec.Code, "rate=%d body=%s", tt.rate, rec.Body.String())
		})
	}
}

// TestRealtimeLogConfigCRUD covers the full Realtime Log Config lifecycle via the HTTP handler.
func TestRealtimeLogConfigCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder, string)
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "create_realtime_log_config",
			method: http.MethodPost,
			path:   "/2020-05-31/realtime-log-config",
			body: []byte(
				`<RealtimeLogConfig>` +
					`<Name>my-rt-log</Name>` +
					`<SamplingRate>100</SamplingRate>` +
					`<Fields><Field>timestamp</Field></Fields>` +
					`</RealtimeLogConfig>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<RealtimeLogConfig")
				assert.Contains(t, rec.Body.String(), "<ARN>")
			},
		},
		{
			name:   "list_realtime_log_configs",
			method: http.MethodGet,
			path:   "/2020-05-31/realtime-log-config",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateRealtimeLogConfig("list-rt-log", 50, []string{"ts"})
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<RealtimeLogConfigs")
			},
		},
		{
			name:   "get_realtime_log_config",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateRealtimeLogConfig("get-rt-log", 75, []string{"ts"})
				require.NoError(t, err)

				return "/2020-05-31/realtime-log-config/get-rt-log"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<RealtimeLogConfig")
			},
		},
		{
			name:   "update_realtime_log_config",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<RealtimeLogConfig>` +
					`<SamplingRate>90</SamplingRate>` +
					`<Fields><Field>uri</Field></Fields>` +
					`</RealtimeLogConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateRealtimeLogConfig("upd-rt-log", 50, []string{"ts"})
				require.NoError(t, err)

				return "/2020-05-31/realtime-log-config/upd-rt-log"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<RealtimeLogConfig")
			},
		},
		{
			name:   "delete_realtime_log_config",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateRealtimeLogConfig("del-rt-log", 50, []string{"ts"})
				require.NoError(t, err)

				return "/2020-05-31/realtime-log-config/del-rt-log"
			},
			wantStatus: http.StatusNoContent,
			check:      nil,
		},
		{
			name:   "get_realtime_log_config_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/realtime-log-config/doesnotexist",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Error>")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := tt.path
			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			rec := doXML(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}

// TestInMemoryBackend_RealtimeLogConfig tests Realtime Log Config backend operations directly.
func TestInMemoryBackend_RealtimeLogConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "create_get_list_update_delete",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				cfg, err := b.CreateRealtimeLogConfig("rl-cfg", 100, []string{"timestamp", "uri"})
				require.NoError(t, err)
				assert.NotEmpty(t, cfg.ARN)

				got, err := b.GetRealtimeLogConfig(cfg.ARN)
				require.NoError(t, err)
				assert.Equal(t, "rl-cfg", got.Name)
				assert.Equal(t, int64(100), got.SamplingRate)

				list := b.ListRealtimeLogConfigs()
				assert.Len(t, list, 1)

				updated, err := b.UpdateRealtimeLogConfig(cfg.ARN, 50, []string{"uri"})
				require.NoError(t, err)
				assert.Equal(t, int64(50), updated.SamplingRate)

				require.NoError(t, b.DeleteRealtimeLogConfig(cfg.ARN))
				_, err = b.GetRealtimeLogConfig(cfg.ARN)
				require.Error(t, err)
			},
		},
		{
			name: "get_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetRealtimeLogConfig("doesnotexist")
				require.Error(t, err)
			},
		},
		{
			name: "update_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateRealtimeLogConfig("doesnotexist", 0, nil)
				require.Error(t, err)
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteRealtimeLogConfig("doesnotexist")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}
