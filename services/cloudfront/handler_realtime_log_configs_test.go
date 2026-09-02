package cloudfront_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

const testKinesisStreamARN = "arn:aws:kinesis:us-east-1:123456789012:stream/test"

const testKinesisRoleARN = "arn:aws:iam::123456789012:role/test"

// realtimeLogConfigRequestBody builds a real-shaped CreateRealtimeLogConfigRequest
// body: root CreateRealtimeLogConfigRequest, EndPoints wrapped in <member>, Fields
// wrapped in <Field> (api_op_CreateRealtimeLogConfig.go, serializers.go:2558-2609).
func realtimeLogConfigRequestBody(name string, rate int) string {
	return fmt.Sprintf(`<CreateRealtimeLogConfigRequest>`+
		`<Name>%s</Name>`+
		`<SamplingRate>%d</SamplingRate>`+
		`<EndPoints>`+
		`<member>`+
		`<StreamType>Kinesis</StreamType>`+
		`<KinesisStreamConfig>`+
		`<StreamARN>%s</StreamARN>`+
		`<RoleARN>%s</RoleARN>`+
		`</KinesisStreamConfig>`+
		`</member>`+
		`</EndPoints>`+
		`<Fields><Field>timestamp</Field></Fields>`+
		`</CreateRealtimeLogConfigRequest>`,
		name, rate, testKinesisStreamARN, testKinesisRoleARN)
}

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

			body := realtimeLogConfigRequestBody(fmt.Sprintf("log-config-%d", tt.rate), tt.rate)

			rec := doReq(t, h, http.MethodPost, "/2020-05-31/realtime-log-config", body)
			assert.Equal(t, tt.wantCode, rec.Code, "rate=%d body=%s", tt.rate, rec.Body.String())
		})
	}
}

// TestCreateRealtimeLogConfig_EndPointsWired is a regression test for
// gopherstack-nfka: a real client's exact request root (CreateRealtimeLogConfigRequest)
// and EndPoints element must survive to backend state and the response, not be
// silently dropped. Fails against the pre-fix handler two ways: the old
// xml:"RealtimeLogConfig" root tag never matches a real client's root element name
// (xml.Unmarshal errors and the whole body -- Name, Fields, SamplingRate, and
// EndPoints -- is discarded), and even with the root fixed, EndPoints was never
// declared as a struct field at all.
func TestCreateRealtimeLogConfig_EndPointsWired(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := realtimeLogConfigRequestBody("endpoints-wired", 50)

	rec := doReq(t, h, http.MethodPost, "/2020-05-31/realtime-log-config", body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	respBody := rec.Body.String()
	assert.Contains(
		t,
		respBody,
		"<RealtimeLogConfig>",
		"response must nest fields under a RealtimeLogConfig child element",
	)
	assert.Contains(t, respBody, testKinesisStreamARN)
	assert.Contains(t, respBody, testKinesisRoleARN)

	cfgs := h.Backend.ListRealtimeLogConfigs()
	require.Len(t, cfgs, 1)
	require.Len(t, cfgs[0].EndPoints, 1)
	assert.Equal(t, "Kinesis", cfgs[0].EndPoints[0].StreamType)
	assert.Equal(t, testKinesisStreamARN, cfgs[0].EndPoints[0].StreamARN)
	assert.Equal(t, testKinesisRoleARN, cfgs[0].EndPoints[0].RoleARN)
}

// TestCreateRealtimeLogConfig_MissingEndPointsRejected verifies the required
// EndPoints member is enforced rather than silently accepted as absent.
func TestCreateRealtimeLogConfig_MissingEndPointsRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := `<CreateRealtimeLogConfigRequest>` +
		`<Name>no-endpoints</Name>` +
		`<SamplingRate>50</SamplingRate>` +
		`<Fields><Field>timestamp</Field></Fields>` +
		`</CreateRealtimeLogConfigRequest>`

	rec := doReq(t, h, http.MethodPost, "/2020-05-31/realtime-log-config", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidArgument")
}

// TestRealtimeLogConfigCRUD covers the full Realtime Log Config lifecycle via the HTTP handler.
// Get, Update, and Delete are RPC-style operations: Get and Delete POST to their own
// distinct paths, Update PUTs to the base path, and all three carry ARN/Name in the
// body rather than a path segment (api_op_{Get,Update,Delete}RealtimeLogConfig.go).
func TestRealtimeLogConfigCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) []byte
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:       "create_realtime_log_config",
			method:     http.MethodPost,
			path:       "/2020-05-31/realtime-log-config",
			body:       []byte(realtimeLogConfigRequestBody("my-rt-log", 100)),
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<RealtimeLogConfig>")
				assert.Contains(t, rec.Body.String(), "<ARN>")
			},
		},
		{
			name:   "list_realtime_log_configs",
			method: http.MethodGet,
			path:   "/2020-05-31/realtime-log-config",
			setup: func(t *testing.T, h *cloudfront.Handler) []byte {
				t.Helper()
				_, err := h.Backend.CreateRealtimeLogConfig(
					"list-rt-log", 50, []string{"ts"}, testEndPoints(),
				)
				require.NoError(t, err)

				return nil
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<RealtimeLogConfigs")
			},
		},
		{
			name:   "get_realtime_log_config",
			method: http.MethodPost,
			path:   "/2020-05-31/get-realtime-log-config",
			setup: func(t *testing.T, h *cloudfront.Handler) []byte {
				t.Helper()
				cfg, err := h.Backend.CreateRealtimeLogConfig(
					"get-rt-log", 75, []string{"ts"}, testEndPoints(),
				)
				require.NoError(t, err)

				return []byte(
					`<GetRealtimeLogConfigRequest><Name>` + cfg.Name + `</Name></GetRealtimeLogConfigRequest>`,
				)
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				body := rec.Body.String()
				assert.Contains(t, body, "<RealtimeLogConfig>")
				assert.Contains(t, body, testKinesisStreamARN)
			},
		},
		{
			name:   "update_realtime_log_config",
			method: http.MethodPut,
			path:   "/2020-05-31/realtime-log-config",
			setup: func(t *testing.T, h *cloudfront.Handler) []byte {
				t.Helper()
				cfg, err := h.Backend.CreateRealtimeLogConfig(
					"upd-rt-log", 50, []string{"ts"}, testEndPoints(),
				)
				require.NoError(t, err)

				return []byte(`<UpdateRealtimeLogConfigRequest>` +
					`<ARN>` + cfg.ARN + `</ARN>` +
					`<SamplingRate>90</SamplingRate>` +
					`<Fields><Field>uri</Field></Fields>` +
					`<EndPoints><member><StreamType>Kinesis</StreamType>` +
					`<KinesisStreamConfig><StreamARN>` + testKinesisStreamARN + `</StreamARN>` +
					`<RoleARN>` + testKinesisRoleARN + `</RoleARN></KinesisStreamConfig>` +
					`</member></EndPoints>` +
					`</UpdateRealtimeLogConfigRequest>`)
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				body := rec.Body.String()
				assert.Contains(t, body, "<RealtimeLogConfig>")
				assert.Contains(t, body, "<SamplingRate>90</SamplingRate>")
			},
		},
		{
			name:   "delete_realtime_log_config",
			method: http.MethodPost,
			path:   "/2020-05-31/delete-realtime-log-config",
			setup: func(t *testing.T, h *cloudfront.Handler) []byte {
				t.Helper()
				cfg, err := h.Backend.CreateRealtimeLogConfig(
					"del-rt-log", 50, []string{"ts"}, testEndPoints(),
				)
				require.NoError(t, err)

				return []byte(
					`<DeleteRealtimeLogConfigRequest><ARN>` + cfg.ARN + `</ARN></DeleteRealtimeLogConfigRequest>`,
				)
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:   "get_realtime_log_config_not_found",
			method: http.MethodPost,
			path:   "/2020-05-31/get-realtime-log-config",
			body:   []byte(`<GetRealtimeLogConfigRequest><Name>doesnotexist</Name></GetRealtimeLogConfigRequest>`),
			setup: func(t *testing.T, _ *cloudfront.Handler) []byte {
				t.Helper()

				return nil
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Error>")
				assert.Contains(t, rec.Body.String(), "NoSuchRealtimeLogConfig")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := tt.body
			if tt.setup != nil {
				if b := tt.setup(t, h); b != nil {
					body = b
				}
			}

			rec := doXML(t, h, tt.method, tt.path, body)

			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
			if tt.check != nil {
				tt.check(t, rec)
			}
		})
	}
}

// TestRealtimeLogConfigCRUD_RealClient drives Create/Get/Update/Delete through
// the real aws-sdk-go-v2 CloudFront client, the strongest available check that
// gopherstack's request parsing and response wrapping match the real wire shape
// byte-for-byte -- the SDK itself refuses to decode a response that doesn't nest
// fields under a <RealtimeLogConfig> child (deserializers.go:
// awsRestxml_deserializeOpDocumentCreateRealtimeLogConfigOutput).
func TestRealtimeLogConfigCRUD_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	created, err := client.CreateRealtimeLogConfig(t.Context(), &cfsdk.CreateRealtimeLogConfigInput{
		Name:         aws.String("real-client-rt-log"),
		SamplingRate: aws.Int64(100),
		Fields:       []string{"timestamp"},
		EndPoints: []types.EndPoint{
			{
				StreamType: aws.String("Kinesis"),
				KinesisStreamConfig: &types.KinesisStreamConfig{
					RoleARN:   aws.String(testKinesisRoleARN),
					StreamARN: aws.String(testKinesisStreamARN),
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.RealtimeLogConfig)
	assert.NotEmpty(t, created.RealtimeLogConfig.ARN)
	require.Len(t, created.RealtimeLogConfig.EndPoints, 1)
	require.NotNil(t, created.RealtimeLogConfig.EndPoints[0].KinesisStreamConfig)
	assert.Equal(t, testKinesisStreamARN, *created.RealtimeLogConfig.EndPoints[0].KinesisStreamConfig.StreamARN)
	assert.Equal(t, testKinesisRoleARN, *created.RealtimeLogConfig.EndPoints[0].KinesisStreamConfig.RoleARN)

	got, err := client.GetRealtimeLogConfig(t.Context(), &cfsdk.GetRealtimeLogConfigInput{
		ARN: created.RealtimeLogConfig.ARN,
	})
	require.NoError(t, err)
	require.NotNil(t, got.RealtimeLogConfig)
	require.Len(t, got.RealtimeLogConfig.EndPoints, 1)

	updated, err := client.UpdateRealtimeLogConfig(t.Context(), &cfsdk.UpdateRealtimeLogConfigInput{
		ARN:          created.RealtimeLogConfig.ARN,
		SamplingRate: aws.Int64(25),
		Fields:       []string{"uri"},
		EndPoints:    created.RealtimeLogConfig.EndPoints,
	})
	require.NoError(t, err)
	require.NotNil(t, updated.RealtimeLogConfig)
	assert.Equal(t, int64(25), *updated.RealtimeLogConfig.SamplingRate)

	_, err = client.DeleteRealtimeLogConfig(t.Context(), &cfsdk.DeleteRealtimeLogConfigInput{
		ARN: created.RealtimeLogConfig.ARN,
	})
	require.NoError(t, err)

	_, err = client.GetRealtimeLogConfig(t.Context(), &cfsdk.GetRealtimeLogConfigInput{
		ARN: created.RealtimeLogConfig.ARN,
	})
	require.Error(t, err)
}

// testEndPoints returns a minimal valid EndPoints slice for backend-level tests.
func testEndPoints() []cloudfront.RealtimeLogEndPoint {
	return []cloudfront.RealtimeLogEndPoint{
		{StreamType: "Kinesis", RoleARN: testKinesisRoleARN, StreamARN: testKinesisStreamARN},
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
				cfg, err := b.CreateRealtimeLogConfig("rl-cfg", 100, []string{"timestamp", "uri"}, testEndPoints())
				require.NoError(t, err)
				assert.NotEmpty(t, cfg.ARN)
				require.Len(t, cfg.EndPoints, 1)

				got, err := b.GetRealtimeLogConfig(cfg.ARN)
				require.NoError(t, err)
				assert.Equal(t, "rl-cfg", got.Name)
				assert.Equal(t, int64(100), got.SamplingRate)

				list := b.ListRealtimeLogConfigs()
				assert.Len(t, list, 1)

				updated, err := b.UpdateRealtimeLogConfig(cfg.ARN, 50, []string{"uri"}, testEndPoints())
				require.NoError(t, err)
				assert.Equal(t, int64(50), updated.SamplingRate)

				require.NoError(t, b.DeleteRealtimeLogConfig(cfg.ARN))
				_, err = b.GetRealtimeLogConfig(cfg.ARN)
				require.Error(t, err)
			},
		},
		{
			name: "create_missing_endpoints_rejected",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRealtimeLogConfig("no-endpoints", 50, []string{"ts"}, nil)
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
				_, err := b.UpdateRealtimeLogConfig("doesnotexist", 0, nil, nil)
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

// TestListRealtimeLogConfigs_ItemShape_RealClient is a regression test for
// gopherstack-21my: ListRealtimeLogConfigs' item struct emitted only ARN/Name/
// SamplingRate, dropping Fields and EndPoints entirely from every item even
// though the real RealtimeLogConfig item deserializer (cloudfront@v1.67.4
// deserializers.go, awsRestxml_deserializeDocumentRealtimeLogConfig) reads both
// -- the sibling GetRealtimeLogConfig already emitted them correctly, so this
// was the "Get and List differ, only Get got it right" trap. Seeds two distinct
// configs with distinguishable non-zero Fields/EndPoints and asserts both come
// back populated and correctly matched by ARN.
func TestListRealtimeLogConfigs_ItemShape_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	mk := func(name, field, roleARN, streamARN string, rate int64) *cfsdk.CreateRealtimeLogConfigOutput {
		out, err := client.CreateRealtimeLogConfig(t.Context(), &cfsdk.CreateRealtimeLogConfigInput{
			Name:         aws.String(name),
			SamplingRate: aws.Int64(rate),
			Fields:       []string{field},
			EndPoints: []types.EndPoint{
				{
					StreamType: aws.String("Kinesis"),
					KinesisStreamConfig: &types.KinesisStreamConfig{
						RoleARN:   aws.String(roleARN),
						StreamARN: aws.String(streamARN),
					},
				},
			},
		})
		require.NoError(t, err)

		return out
	}

	first := mk("list-shape-log-1", "timestamp",
		"arn:aws:iam::123456789012:role/first", "arn:aws:kinesis:us-east-1:123456789012:stream/first", 50)
	second := mk("list-shape-log-2", "c-ip",
		"arn:aws:iam::123456789012:role/second", "arn:aws:kinesis:us-east-1:123456789012:stream/second", 75)

	listed, err := client.ListRealtimeLogConfigs(t.Context(), &cfsdk.ListRealtimeLogConfigsInput{})
	require.NoError(t, err)
	require.NotNil(t, listed.RealtimeLogConfigs)
	require.Len(t, listed.RealtimeLogConfigs.Items, 2)

	byARN := make(map[string]types.RealtimeLogConfig, 2)
	for _, item := range listed.RealtimeLogConfigs.Items {
		require.NotNil(t, item.ARN)
		byARN[*item.ARN] = item
	}

	item1, ok := byARN[*first.RealtimeLogConfig.ARN]
	require.True(t, ok)
	require.Len(t, item1.Fields, 1)
	assert.Equal(t, "timestamp", item1.Fields[0])
	require.Len(t, item1.EndPoints, 1)
	require.NotNil(t, item1.EndPoints[0].KinesisStreamConfig)
	assert.Equal(t, "arn:aws:iam::123456789012:role/first", *item1.EndPoints[0].KinesisStreamConfig.RoleARN)
	assert.Equal(t, "arn:aws:kinesis:us-east-1:123456789012:stream/first",
		*item1.EndPoints[0].KinesisStreamConfig.StreamARN)

	item2, ok := byARN[*second.RealtimeLogConfig.ARN]
	require.True(t, ok)
	require.Len(t, item2.Fields, 1)
	assert.Equal(t, "c-ip", item2.Fields[0])
	require.Len(t, item2.EndPoints, 1)
	require.NotNil(t, item2.EndPoints[0].KinesisStreamConfig)
	assert.Equal(t, "arn:aws:iam::123456789012:role/second", *item2.EndPoints[0].KinesisStreamConfig.RoleARN)
	assert.Equal(t, "arn:aws:kinesis:us-east-1:123456789012:stream/second",
		*item2.EndPoints[0].KinesisStreamConfig.StreamARN)
}
