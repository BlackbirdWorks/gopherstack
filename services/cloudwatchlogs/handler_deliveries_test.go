package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_Delivery_CreateWithTags(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	deliveryBody := `{"deliverySourceName":"src",` +
		`"deliveryDestinationArn":"arn:aws:logs:us-east-1:123:delivery-destination:dst",` +
		`"tags":{"env":"prod"}}`
	rec := doLogsRequest(t, h, e, "CreateDelivery", deliveryBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	delivery, ok := out["delivery"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, delivery["id"])
	assert.NotEmpty(t, delivery["arn"])
	assert.Equal(t, "src", delivery["deliverySourceName"])
	tags, ok := delivery["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["env"])
}

func TestHandler_DeliveryTags_DeepCopy(t *testing.T) {
	t.Parallel()

	// Create a delivery with tags.
	backend := cloudwatchlogs.NewInMemoryBackend()
	e := echo.New()
	h := cloudwatchlogs.NewHandler(backend)

	body := `{"deliverySourceName":"src",` +
		`"deliveryDestinationArn":"arn:aws:logs:us-east-1:123:delivery-destination:dst",` +
		`"tags":{"env":"prod"}}`
	rec := doLogsRequest(t, h, e, "CreateDelivery", body)
	require.Equal(t, http.StatusOK, rec.Code)

	// Seed a delivery and mutate the tags after seeding — the stored delivery must not change.
	mutatingTags := map[string]string{"key": "original"}
	cloudwatchlogs.AddDeliveryInternal(backend, cloudwatchlogs.Delivery{
		ID:                     "test-delivery",
		Arn:                    "arn:aws:logs:us-east-1:123:delivery:test-delivery",
		DeliverySourceName:     "src2",
		DeliveryDestinationArn: "arn:aws:logs:us-east-1:123:delivery-destination:dst",
		Tags:                   mutatingTags,
	})

	// Mutate the original map. The stored delivery should not be affected.
	mutatingTags["key"] = "mutated"

	// Verify the stored delivery is unaffected by snapshotting and restoring.
	snap := backend.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := cloudwatchlogs.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))
}

func TestHandler_DeliveryDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutDeliveryDestination/OK",
			action: "PutDeliveryDestination",
			body: map[string]any{
				"name":         "my-dest",
				"outputFormat": "JSON",
				"deliveryDestinationConfiguration": map[string]any{
					"destinationResourceArn": "arn:aws:s3:::my-bucket",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutDeliveryDestination/EmptyName",
			action: "PutDeliveryDestination",
			body: map[string]any{
				"name": "",
				"deliveryDestinationConfiguration": map[string]any{
					"destinationResourceArn": "arn:aws:s3:::bucket",
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "GetDeliveryDestination/OK",
			action: "GetDeliveryDestination",
			body:   map[string]any{"name": "my-dest"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(
					t,
					h,
					e,
					"PutDeliveryDestination",
					`{"name":"my-dest","deliveryDestinationConfiguration":{"destinationResourceArn":"arn:aws:s3:::bucket"}}`,
				)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "GetDeliveryDestination/NotFound",
			action:   "GetDeliveryDestination",
			body:     map[string]any{"name": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DescribeDeliveryDestinations/WithEntries",
			action: "DescribeDeliveryDestinations",
			body:   map[string]any{},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(
					t,
					h,
					e,
					"PutDeliveryDestination",
					`{"name":"dest1","deliveryDestinationConfiguration":{"destinationResourceArn":"arn:aws:s3:::b1"}}`,
				)
				doLogsRequest(
					t,
					h,
					e,
					"PutDeliveryDestination",
					`{"name":"dest2","deliveryDestinationConfiguration":{"destinationResourceArn":"arn:aws:s3:::b2"}}`,
				)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteDeliveryDestination/OK",
			action: "DeleteDeliveryDestination",
			body:   map[string]any{"name": "my-dest"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(
					t,
					h,
					e,
					"PutDeliveryDestination",
					`{"name":"my-dest","deliveryDestinationConfiguration":{"destinationResourceArn":"arn:aws:s3:::bucket"}}`,
				)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteDeliveryDestination/NotFound",
			action:   "DeleteDeliveryDestination",
			body:     map[string]any{"name": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "PutDeliveryDestinationPolicy/OK",
			action: "PutDeliveryDestinationPolicy",
			body: map[string]any{
				"deliveryDestinationName":   "my-dest",
				"deliveryDestinationPolicy": `{"Statement":[]}`,
			},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(
					t,
					h,
					e,
					"PutDeliveryDestination",
					`{"name":"my-dest","deliveryDestinationConfiguration":{"destinationResourceArn":"arn:aws:s3:::bucket"}}`,
				)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "GetDeliveryDestinationPolicy/OK",
			action: "GetDeliveryDestinationPolicy",
			body:   map[string]any{"deliveryDestinationName": "my-dest"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(
					t,
					h,
					e,
					"PutDeliveryDestination",
					`{"name":"my-dest","deliveryDestinationConfiguration":{"destinationResourceArn":"arn:aws:s3:::bucket"}}`,
				)
				doLogsRequest(
					t,
					h,
					e,
					"PutDeliveryDestinationPolicy",
					`{"deliveryDestinationName":"my-dest","deliveryDestinationPolicy":"{\"Statement\":[]}"}`,
				)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteDeliveryDestinationPolicy/OK",
			action: "DeleteDeliveryDestinationPolicy",
			body:   map[string]any{"deliveryDestinationName": "my-dest"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(
					t,
					h,
					e,
					"PutDeliveryDestination",
					`{"name":"my-dest","deliveryDestinationConfiguration":{"destinationResourceArn":"arn:aws:s3:::bucket"}}`,
				)
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)
			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_DeliveryDestination_WireShape locks the AWS wire shape for
// aws-sdk-go-v2 types.DeliveryDestination: the target resource ARN must
// appear nested as
// deliveryDestinationConfiguration.destinationResourceArn (not a flat
// string under deliveryDestinationConfiguration), deliveryDestinationType
// must be present as a top-level sibling field, and the response must not
// carry a flat "policy" key (policy is a different operation's shape).
func TestHandler_DeliveryDestination_WireShape(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)

	putRec := doLogsRequest(t, h, e, "PutDeliveryDestination", `{
		"name": "my-dest",
		"outputFormat": "json",
		"deliveryDestinationType": "S3",
		"deliveryDestinationConfiguration": {"destinationResourceArn": "arn:aws:s3:::my-bucket"}
	}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	checkShape := func(t *testing.T, dest map[string]any) {
		t.Helper()

		assert.Equal(t, "S3", dest["deliveryDestinationType"])

		config, ok := dest["deliveryDestinationConfiguration"].(map[string]any)
		require.True(t, ok, "deliveryDestinationConfiguration must be a nested object")
		assert.Equal(t, "arn:aws:s3:::my-bucket", config["destinationResourceArn"])

		_, hasPolicy := dest["policy"]
		assert.False(t, hasPolicy, "policy must not appear on a DeliveryDestination")
	}

	var putOut map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putOut))
	putDest, ok := putOut["deliveryDestination"].(map[string]any)
	require.True(t, ok)
	checkShape(t, putDest)

	getRec := doLogsRequest(t, h, e, "GetDeliveryDestination", `{"name":"my-dest"}`)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	getDest, ok := getOut["deliveryDestination"].(map[string]any)
	require.True(t, ok)
	checkShape(t, getDest)

	describeRec := doLogsRequest(t, h, e, "DescribeDeliveryDestinations", `{}`)
	require.Equal(t, http.StatusOK, describeRec.Code)

	var describeOut map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeOut))
	dests, ok := describeOut["deliveryDestinations"].([]any)
	require.True(t, ok)
	require.Len(t, dests, 1)
	describedDest, ok := dests[0].(map[string]any)
	require.True(t, ok)
	checkShape(t, describedDest)
}

func TestHandler_DeliverySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutDeliverySource/OK",
			action: "PutDeliverySource",
			body: map[string]any{
				"name":        "my-src",
				"logType":     "APPLICATION_LOGS",
				"resourceArn": "arn:aws:ec2:::instance/i-123",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutDeliverySource/EmptyName",
			action: "PutDeliverySource",
			body: map[string]any{
				"name":    "",
				"logType": "FLOW_LOGS",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "GetDeliverySource/OK",
			action: "GetDeliverySource",
			body:   map[string]any{"name": "my-src"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutDeliverySource", `{"name":"my-src","logType":"APPLICATION_LOGS"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "GetDeliverySource/NotFound",
			action:   "GetDeliverySource",
			body:     map[string]any{"name": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DescribeDeliverySources/WithEntries",
			action: "DescribeDeliverySources",
			body:   map[string]any{},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutDeliverySource", `{"name":"src1","logType":"APPLICATION_LOGS"}`)
				doLogsRequest(t, h, e, "PutDeliverySource", `{"name":"src2","logType":"FLOW_LOGS"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteDeliverySource/OK",
			action: "DeleteDeliverySource",
			body:   map[string]any{"name": "my-src"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutDeliverySource", `{"name":"my-src","logType":"APPLICATION_LOGS"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteDeliverySource/NotFound",
			action:   "DeleteDeliverySource",
			body:     map[string]any{"name": "ghost"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)
			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_DeliverySource_WireShape locks two things about the AWS wire
// contract for aws-sdk-go-v2 types.DeliverySource / PutDeliverySourceInput:
//
//  1. The input field is the singular "resourceArn" (a plain string), not a
//     "resourceArns" array -- a previous version of this handler parsed the
//     wrong (plural) key, so a real SDK client's request silently lost its
//     resource ARN entirely (the backend always saw an empty slice).
//  2. The response includes logType/resourceArns/service/tags, not just
//     name/arn -- service is never client-supplied; it is derived
//     server-side from the resource ARN's service segment.
func TestHandler_DeliverySource_WireShape(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)

	putRec := doLogsRequest(t, h, e, "PutDeliverySource", `{
		"name": "my-src",
		"logType": "APPLICATION_LOGS",
		"resourceArn": "arn:aws:workmail:us-east-1:123456789012:organization/m-abc"
	}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	checkShape := func(t *testing.T, src map[string]any) {
		t.Helper()

		assert.Equal(t, "APPLICATION_LOGS", src["logType"])
		assert.Equal(t, "workmail", src["service"], "service must be derived from the resource ARN")

		arns, ok := src["resourceArns"].([]any)
		require.True(t, ok, "resourceArns must be present as an array")
		require.Len(t, arns, 1)
		assert.Equal(t, "arn:aws:workmail:us-east-1:123456789012:organization/m-abc", arns[0])
	}

	var putOut map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putOut))
	putSrc, ok := putOut["deliverySource"].(map[string]any)
	require.True(t, ok)
	checkShape(t, putSrc)

	getRec := doLogsRequest(t, h, e, "GetDeliverySource", `{"name":"my-src"}`)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	getSrc, ok := getOut["deliverySource"].(map[string]any)
	require.True(t, ok)
	checkShape(t, getSrc)

	describeRec := doLogsRequest(t, h, e, "DescribeDeliverySources", `{}`)
	require.Equal(t, http.StatusOK, describeRec.Code)

	var describeOut map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeOut))
	srcs, ok := describeOut["deliverySources"].([]any)
	require.True(t, ok)
	require.Len(t, srcs, 1)
	describedSrc, ok := srcs[0].(map[string]any)
	require.True(t, ok)
	checkShape(t, describedSrc)
}

func TestHandler_UpdateDeliveryConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) string
		body     func(deliveryID string) map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "UpdateDeliveryConfiguration/FieldDelimiter",
			action: "UpdateDeliveryConfiguration",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) string {
				t.Helper()
				rec := doLogsRequest(
					t,
					h,
					e,
					"CreateDelivery",
					`{"deliverySourceName":"src","deliveryDestinationArn":"arn:aws:logs:us-east-1:123:delivery-destination:dst"}`,
				)
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				id, _ := resp["delivery"]["id"].(string)

				return id
			},
			body: func(deliveryID string) map[string]any {
				return map[string]any{"id": deliveryID, "fieldDelimiter": ","}
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "UpdateDeliveryConfiguration/RecordFields",
			action: "UpdateDeliveryConfiguration",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) string {
				t.Helper()
				rec := doLogsRequest(
					t,
					h,
					e,
					"CreateDelivery",
					`{"deliverySourceName":"src","deliveryDestinationArn":"arn:aws:logs:us-east-1:123:delivery-destination:dst"}`,
				)
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				id, _ := resp["delivery"]["id"].(string)

				return id
			},
			body: func(deliveryID string) map[string]any {
				return map[string]any{"id": deliveryID, "recordFields": []string{"@timestamp", "@message"}}
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "UpdateDeliveryConfiguration/NotFound",
			action: "UpdateDeliveryConfiguration",
			setup: func(_ *testing.T, _ *cloudwatchlogs.Handler, _ *echo.Echo) string {
				return "nonexistent-id"
			},
			body: func(deliveryID string) map[string]any {
				return map[string]any{"id": deliveryID, "fieldDelimiter": ","}
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			var deliveryID string
			if tt.setup != nil {
				deliveryID = tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body(deliveryID))
			require.NoError(t, err)
			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CreateDeliveryOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantKey  string
		wantVal  string
		wantCode int
	}{
		// CreateDelivery
		{
			name:   "CreateDelivery/OK",
			action: "CreateDelivery",
			body: map[string]any{
				"deliverySourceName":     "my-source",
				"deliveryDestinationArn": "arn:aws:logs:us-east-1:123:delivery-destination:dst",
			},
			wantCode: http.StatusOK,
			wantKey:  "delivery",
		},
		{
			name:   "CreateDelivery/MissingSource",
			action: "CreateDelivery",
			body: map[string]any{
				"deliveryDestinationArn": "arn:aws:logs:us-east-1:123:delivery-destination:dst",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateDelivery/MissingDestination",
			action:   "CreateDelivery",
			body:     map[string]any{"deliverySourceName": "my-source"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := cloudwatchlogs.NewInMemoryBackend()
			h := cloudwatchlogs.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.wantKey != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				if tt.wantVal != "" {
					assert.Equal(t, tt.wantVal, out[tt.wantKey])
				} else {
					assert.NotEmpty(t, out[tt.wantKey], "expected non-empty %s", tt.wantKey)
				}
			}
		})
	}
}

func TestHandler_DeliveryResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body       map[string]any
		name       string
		action     string
		wantFields []string
		wantCode   int
	}{
		{
			name:   "PutDeliveryDestination/HasDeliveryDestination",
			action: "PutDeliveryDestination",
			body: map[string]any{
				"name":         "d",
				"outputFormat": "JSON",
				"deliveryDestinationConfiguration": map[string]any{
					"destinationResourceArn": "arn:aws:s3:::b",
				},
			},
			wantFields: []string{"deliveryDestination"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "DescribeDeliveryDestinations/HasDeliveryDestinations",
			action:     "DescribeDeliveryDestinations",
			body:       map[string]any{},
			wantFields: []string{"deliveryDestinations"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "PutDeliverySource/HasDeliverySource",
			action:     "PutDeliverySource",
			body:       map[string]any{"name": "s", "logType": "FLOW_LOGS"},
			wantFields: []string{"deliverySource"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "DescribeDeliverySources/HasDeliverySources",
			action:     "DescribeDeliverySources",
			body:       map[string]any{},
			wantFields: []string{"deliverySources"},
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)
			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			for _, field := range tt.wantFields {
				assert.Contains(t, resp, field, "response should contain field %q", field)
			}
		})
	}
}
