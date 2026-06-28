package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// newTestHandler returns a fresh Handler + Echo for completeness handler tests.
func newTestHandler(t *testing.T) (*cloudwatchlogs.Handler, *echo.Echo) {
	t.Helper()
	b := cloudwatchlogs.NewInMemoryBackend()
	t.Cleanup(func() { b.Close() })
	h := cloudwatchlogs.NewHandler(b)

	return h, echo.New()
}

// ---- ResourcePolicy handler tests ----

func TestHandler_ResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body          map[string]any
		name          string
		action        string
		wantBodyField string
		wantCode      int
	}{
		{
			name:     "PutResourcePolicy/OK",
			action:   "PutResourcePolicy",
			body:     map[string]any{"policyName": "my-policy", "policyDocument": `{"Version":"2012-10-17"}`},
			wantCode: http.StatusOK,
		},
		{
			name:     "PutResourcePolicy/EmptyName",
			action:   "PutResourcePolicy",
			body:     map[string]any{"policyName": "", "policyDocument": `{}`},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DescribeResourcePolicies/Empty",
			action:   "DescribeResourcePolicies",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:   "DescribeResourcePolicies/WithEntries",
			action: "DescribeResourcePolicies",
			body:   map[string]any{},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutResourcePolicy",
					`{"policyName":"p1","policyDocument":"{}"}`)
				doLogsRequest(t, h, e, "PutResourcePolicy",
					`{"policyName":"p2","policyDocument":"{}"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteResourcePolicy/OK",
			action: "DeleteResourcePolicy",
			body:   map[string]any{"policyName": "my-policy"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutResourcePolicy",
					`{"policyName":"my-policy","policyDocument":"{}"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteResourcePolicy/NotFound",
			action:   "DeleteResourcePolicy",
			body:     map[string]any{"policyName": "ghost"},
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

// ---- DeliveryDestination handler tests ----

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

// ---- DeliverySource handler tests ----

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
				"name":         "my-src",
				"logType":      "APPLICATION_LOGS",
				"resourceArns": []string{"arn:aws:ec2:::instance/i-123"},
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

// ---- Destination (log routing) handler tests ----

func TestHandler_Destination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutDestination/OK",
			action: "PutDestination",
			body: map[string]any{
				"destinationName": "my-dest",
				"targetArn":       "arn:aws:kinesis:::stream/my-stream",
				"roleArn":         "arn:aws:iam:::role/my-role",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutDestination/EmptyName",
			action: "PutDestination",
			body: map[string]any{
				"destinationName": "",
				"targetArn":       "arn:aws:kinesis:::stream/s",
				"roleArn":         "arn:aws:iam:::role/r",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "PutDestinationPolicy/OK",
			action: "PutDestinationPolicy",
			body: map[string]any{
				"destinationName": "my-dest",
				"accessPolicy":    `{"Statement":[]}`,
			},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(
					t,
					h,
					e,
					"PutDestination",
					`{"destinationName":"my-dest","targetArn":"arn:t","roleArn":"arn:r"}`,
				)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutDestinationPolicy/NotFound",
			action: "PutDestinationPolicy",
			body: map[string]any{
				"destinationName": "ghost",
				"accessPolicy":    `{}`,
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DescribeDestinations/WithPrefix",
			action: "DescribeDestinations",
			body:   map[string]any{"DestinationNamePrefix": "prod"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(
					t,
					h,
					e,
					"PutDestination",
					`{"destinationName":"prod-dest","targetArn":"arn:t","roleArn":"arn:r"}`,
				)
				doLogsRequest(
					t,
					h,
					e,
					"PutDestination",
					`{"destinationName":"dev-dest","targetArn":"arn:t2","roleArn":"arn:r2"}`,
				)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteDestination/OK",
			action: "DeleteDestination",
			body:   map[string]any{"destinationName": "my-dest"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(
					t,
					h,
					e,
					"PutDestination",
					`{"destinationName":"my-dest","targetArn":"arn:t","roleArn":"arn:r"}`,
				)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteDestination/NotFound",
			action:   "DeleteDestination",
			body:     map[string]any{"destinationName": "ghost"},
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

// ---- IndexPolicy handler tests ----

func TestHandler_IndexPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutIndexPolicy/OK",
			action: "PutIndexPolicy",
			body: map[string]any{
				"logGroupIdentifier": "/aws/lambda/fn",
				"policyDocument":     `{"fields":["@message"]}`,
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutIndexPolicy/EmptyIdentifier",
			action: "PutIndexPolicy",
			body: map[string]any{
				"logGroupIdentifier": "",
				"policyDocument":     `{}`,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeIndexPolicies/WithEntries",
			action: "DescribeIndexPolicies",
			body:   map[string]any{},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIndexPolicy",
					`{"logGroupIdentifier":"/grp1","policyDocument":"{}"}`)
				doLogsRequest(t, h, e, "PutIndexPolicy",
					`{"logGroupIdentifier":"/grp2","policyDocument":"{}"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteIndexPolicy/OK",
			action: "DeleteIndexPolicy",
			body:   map[string]any{"logGroupIdentifier": "/aws/lambda/fn"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIndexPolicy",
					`{"logGroupIdentifier":"/aws/lambda/fn","policyDocument":"{}"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteIndexPolicy/NotFound",
			action:   "DeleteIndexPolicy",
			body:     map[string]any{"logGroupIdentifier": "ghost"},
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

// ---- Transformer handler tests ----

func TestHandler_Transformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutTransformer/OK",
			action: "PutTransformer",
			body: map[string]any{
				"logGroupIdentifier": "/aws/lambda/fn",
				"transformerConfig":  []map[string]any{{"parseJSON": map[string]any{}}},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutTransformer/EmptyIdentifier",
			action: "PutTransformer",
			body: map[string]any{
				"logGroupIdentifier": "",
				"transformerConfig":  []map[string]any{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "GetTransformer/OK",
			action: "GetTransformer",
			body:   map[string]any{"logGroupIdentifier": "/aws/lambda/fn"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutTransformer",
					`{"logGroupIdentifier":"/aws/lambda/fn","transformerConfig":[{"parseJSON":{}}]}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "GetTransformer/NotFound",
			action:   "GetTransformer",
			body:     map[string]any{"logGroupIdentifier": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteTransformer/OK",
			action: "DeleteTransformer",
			body:   map[string]any{"logGroupIdentifier": "/aws/lambda/fn"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutTransformer",
					`{"logGroupIdentifier":"/aws/lambda/fn","transformerConfig":[{"parseJSON":{}}]}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteTransformer/NotFound",
			action:   "DeleteTransformer",
			body:     map[string]any{"logGroupIdentifier": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "TestTransformer/OK",
			action: "TestTransformer",
			body: map[string]any{
				"logGroupIdentifier": "/grp",
				"logEventMessages":   []string{`{"level":"ERROR","msg":"oops"}`},
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

// ---- Integration handler tests ----

func TestHandler_Integration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutIntegration/OK",
			action: "PutIntegration",
			body: map[string]any{
				"integrationName": "my-opensearch",
				"integrationType": "OPENSEARCH",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutIntegration/EmptyName",
			action: "PutIntegration",
			body: map[string]any{
				"integrationName": "",
				"integrationType": "OPENSEARCH",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "GetIntegration/OK",
			action: "GetIntegration",
			body:   map[string]any{"integrationName": "my-opensearch"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIntegration",
					`{"integrationName":"my-opensearch","integrationType":"OPENSEARCH"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "GetIntegration/NotFound",
			action:   "GetIntegration",
			body:     map[string]any{"integrationName": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "ListIntegrations/WithEntries",
			action: "ListIntegrations",
			body:   map[string]any{},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIntegration", `{"integrationName":"ig1","integrationType":"OPENSEARCH"}`)
				doLogsRequest(t, h, e, "PutIntegration", `{"integrationName":"ig2","integrationType":"OPENSEARCH"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteIntegration/OK",
			action: "DeleteIntegration",
			body:   map[string]any{"integrationName": "my-opensearch"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIntegration",
					`{"integrationName":"my-opensearch","integrationType":"OPENSEARCH"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteIntegration/NotFound",
			action:   "DeleteIntegration",
			body:     map[string]any{"integrationName": "ghost"},
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

// ---- LogGroupDeletionProtection handler tests ----

func TestHandler_LogGroupDeletionProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutLogGroupDeletionProtection/Enable",
			action: "PutLogGroupDeletionProtection",
			body: map[string]any{
				"logGroupIdentifier": "/aws/lambda/fn",
				"deletionProtected":  true,
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutLogGroupDeletionProtection/Disable",
			action: "PutLogGroupDeletionProtection",
			body: map[string]any{
				"logGroupIdentifier": "/aws/lambda/fn",
				"deletionProtected":  false,
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutLogGroupDeletionProtection/EmptyIdentifier",
			action: "PutLogGroupDeletionProtection",
			body: map[string]any{
				"logGroupIdentifier": "",
				"deletionProtected":  true,
			},
			wantCode: http.StatusBadRequest,
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

// ---- UpdateDeliveryConfiguration handler tests ----

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

// ---- Stub operations handler tests ----

func TestHandler_CompletenessStubs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		action        string
		wantListField string
		wantCode      int
	}{
		{
			name:          "DescribeConfigurationTemplates/ReturnsEmpty",
			action:        "DescribeConfigurationTemplates",
			body:          map[string]any{},
			wantCode:      http.StatusOK,
			wantListField: "configurationTemplates",
		},
		{
			name:          "DescribeFieldIndexes/ReturnsEmpty",
			action:        "DescribeFieldIndexes",
			body:          map[string]any{},
			wantCode:      http.StatusOK,
			wantListField: "fieldIndexes",
		},
		{
			// DescribeImportTaskBatches is validation-only: taskId is required.
			name:     "DescribeImportTaskBatches/RequiresTaskID",
			action:   "DescribeImportTaskBatches",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:          "ListAggregateLogGroupSummaries/ReturnsEmpty",
			action:        "ListAggregateLogGroupSummaries",
			body:          map[string]any{},
			wantCode:      http.StatusOK,
			wantListField: "logGroupSummaries",
		},
		{
			name:          "ListSourcesForS3TableIntegration/ReturnsEmpty",
			action:        "ListSourcesForS3TableIntegration",
			body:          map[string]any{},
			wantCode:      http.StatusOK,
			wantListField: "sources",
		},
		{
			name:     "DisassociateSourceFromS3TableIntegration/OK",
			action:   "DisassociateSourceFromS3TableIntegration",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:     "PutBearerTokenAuthentication/OK",
			action:   "PutBearerTokenAuthentication",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			// StartLiveTail is validation-only: logGroupIdentifiers is required.
			name:     "StartLiveTail/RequiresLogGroups",
			action:   "StartLiveTail",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			// GetLogFields requires a data source / log group identifier.
			name:     "GetLogFields/RequiresDataSource",
			action:   "GetLogFields",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			// GetLogObject requires a log object pointer.
			name:     "GetLogObject/RequiresPointer",
			action:   "GetLogObject",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := makeLogsRequest(t, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantListField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				list, ok := resp[tt.wantListField].([]any)
				require.True(t, ok, "expected list field %q in response", tt.wantListField)
				assert.Empty(t, list)
			}
		})
	}
}

// ---- DataProtectionPolicy handler tests ----

func TestHandler_DataProtectionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutDataProtectionPolicy/OK",
			action: "PutDataProtectionPolicy",
			body: map[string]any{
				"logGroupIdentifier": "/aws/lambda/fn",
				"policyDocument":     `{"Name":"protect","Version":"2021-06-01","Statement":[]}`,
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "GetDataProtectionPolicy/OK",
			action: "GetDataProtectionPolicy",
			body:   map[string]any{"logGroupIdentifier": "/aws/lambda/fn"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutDataProtectionPolicy",
					`{"logGroupIdentifier":"/aws/lambda/fn","policyDocument":"{\"Name\":\"protect\"}"}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteDataProtectionPolicy/OK",
			action: "DeleteDataProtectionPolicy",
			body:   map[string]any{"logGroupIdentifier": "/aws/lambda/fn"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutDataProtectionPolicy",
					`{"logGroupIdentifier":"/aws/lambda/fn","policyDocument":"{}"}`)
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

// ---- Handler response shape tests ----

func TestHandler_CompletenessResponseShapes(t *testing.T) {
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
			name:       "PutResourcePolicy/HasResourcePolicy",
			action:     "PutResourcePolicy",
			body:       map[string]any{"policyName": "p", "policyDocument": `{}`},
			wantFields: []string{"resourcePolicy"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "DescribeResourcePolicies/HasResourcePolicies",
			action:     "DescribeResourcePolicies",
			body:       map[string]any{},
			wantFields: []string{"resourcePolicies"},
			wantCode:   http.StatusOK,
		},
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
		{
			name:   "PutDestination/HasDestination",
			action: "PutDestination",
			body: map[string]any{
				"destinationName": "d",
				"targetArn":       "arn:t",
				"roleArn":         "arn:r",
			},
			wantFields: []string{"destination"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "DescribeDestinations/HasDestinations",
			action:     "DescribeDestinations",
			body:       map[string]any{},
			wantFields: []string{"destinations"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "PutIndexPolicy/HasIndexPolicy",
			action:     "PutIndexPolicy",
			body:       map[string]any{"logGroupIdentifier": "/grp", "policyDocument": `{}`},
			wantFields: []string{"indexPolicy"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "DescribeIndexPolicies/HasIndexPolicies",
			action:     "DescribeIndexPolicies",
			body:       map[string]any{},
			wantFields: []string{"indexPolicies"},
			wantCode:   http.StatusOK,
		},
		{
			name:   "PutIntegration/HasIntegrationName",
			action: "PutIntegration",
			body: map[string]any{
				"integrationName": "ig",
				"integrationType": "OPENSEARCH",
			},
			wantFields: []string{"integrationName"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "ListIntegrations/HasIntegrationSummaries",
			action:     "ListIntegrations",
			body:       map[string]any{},
			wantFields: []string{"integrationSummaries"},
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
