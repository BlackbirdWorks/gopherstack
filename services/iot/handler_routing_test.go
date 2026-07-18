package iot_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_Operations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           any
		validateOutput func(t *testing.T, body []byte)
		name           string
		method         string
		path           string
		wantOp         string
		wantResource   string
		wantStatus     int
	}{
		{
			name:   "CreateThing",
			method: http.MethodPost,
			path:   "/things/my-thing",
			body: map[string]any{
				"thingTypeName": "Sensor",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "CreateThing",
			wantResource: "my-thing",
			validateOutput: func(t *testing.T, body []byte) {
				t.Helper()
				var out map[string]string
				require.NoError(t, json.Unmarshal(body, &out))
				assert.Equal(t, "my-thing", out["thingName"])
			},
		},
		{
			name:   "DescribeThing",
			method: http.MethodGet,
			path:   "/things/my-thing",
			body:   nil,
			validateOutput: func(t *testing.T, _ []byte) {
				t.Helper()
				// We need to create it first for a real test,
				// but here we just test routing/dispatch.
			},
			wantStatus:   http.StatusNotFound, // Not created in this specific test
			wantOp:       "DescribeThing",
			wantResource: "my-thing",
		},
		{
			name:   "CreatePolicy",
			method: http.MethodPost,
			path:   "/policies/my-policy",
			body: map[string]any{
				"policyDocument": "{}",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "CreatePolicy",
			wantResource: "my-policy",
		},
		{
			name:         "DescribeEndpoint",
			method:       http.MethodGet,
			path:         "/endpoint?endpointType=iot:Data-ATS",
			body:         nil,
			wantStatus:   http.StatusOK,
			wantOp:       "DescribeEndpoint",
			wantResource: "",
		},
		{
			name:         "UnknownOperation",
			method:       http.MethodGet,
			path:         "/invalid-path",
			body:         nil,
			wantStatus:   http.StatusBadRequest,
			wantOp:       "Unknown",
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := iot.NewInMemoryBackend()
			handler := iot.NewHandler(backend, nil)

			var reqBody []byte
			if tt.body != nil {
				var err error
				reqBody, err = json.Marshal(tt.body)
				require.NoError(t, err)
			}

			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader(reqBody))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			// Test Metadata methods
			assert.Equal(t, tt.wantOp, handler.ExtractOperation(c))
			assert.Equal(t, tt.wantResource, handler.ExtractResource(c))
			assert.Positive(t, handler.MatchPriority())
			assert.Equal(t, "IoT", handler.Name())
			assert.Contains(t, handler.GetSupportedOperations(), "CreateThing")

			// Test actual handler dispatch
			err := handler.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.validateOutput != nil && rec.Code == http.StatusOK {
				tt.validateOutput(t, rec.Body.Bytes())
			}
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	handler := iot.NewHandler(iot.NewInMemoryBackend(), nil)
	matcher := handler.RouteMatcher()

	tests := []struct {
		path string
		name string
		want bool
	}{
		{"/things/t1", "things_prefix", true},
		{"/rules/r1", "rules_prefix", true},
		{"/policies/p1", "policies_prefix", true},
		{"/endpoint", "endpoint_exact", true},
		{"/accept-certificate-transfer/cert-123", "accept_cert_transfer", true},
		{"/billing-groups/addThingToBillingGroup", "add_thing_to_billing_group", true},
		{"/thing-groups/addThingToThingGroup", "add_thing_to_thing_group", true},
		{"/packages/pkg/versions/v1/sbom", "package_sbom", true},
		{"/jobs/job-1/targets", "job_targets", true},
		{"/security-profiles/sp1/targets", "security_profile_targets", true},
		{"/audit/mitigationactions/tasks/task-1/cancel", "audit_mitigation_cancel", true},
		{"/audit/tasks/task-2/cancel", "audit_task_cancel", true},
		{"/s3/bucket", "other_service", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestHandler_NewOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		headers      map[string]string
		name         string
		method       string
		path         string
		wantOp       string
		wantResource string
		wantStatus   int
	}{
		{
			name:         "AcceptCertificateTransfer",
			method:       http.MethodPatch,
			path:         "/accept-certificate-transfer/cert-abc",
			body:         map[string]any{"setAsActive": true},
			wantStatus:   http.StatusOK,
			wantOp:       "AcceptCertificateTransfer",
			wantResource: "cert-abc",
		},
		{
			name:   "AddThingToBillingGroup",
			method: http.MethodPut,
			path:   "/billing-groups/addThingToBillingGroup",
			body: map[string]any{
				"billingGroupName": "group-1",
				"thingName":        "thing-1",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "AddThingToBillingGroup",
			wantResource: "",
		},
		{
			name:   "AddThingToThingGroup",
			method: http.MethodPut,
			path:   "/thing-groups/addThingToThingGroup",
			body: map[string]any{
				"thingGroupName": "group-2",
				"thingName":      "thing-2",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "AddThingToThingGroup",
			wantResource: "",
		},
		{
			name:   "AssociateSbomWithPackageVersion",
			method: http.MethodPut,
			path:   "/packages/my-pkg/versions/1.0.0/sbom",
			body: map[string]any{
				"sbom": map[string]any{
					"s3Location": map[string]any{
						"bucket":  "my-bucket",
						"key":     "sbom.json",
						"version": "v1",
					},
				},
			},
			wantStatus:   http.StatusOK,
			wantOp:       "AssociateSbomWithPackageVersion",
			wantResource: "my-pkg",
		},
		{
			name:   "AssociateTargetsWithJob",
			method: http.MethodPost,
			path:   "/jobs/job-42/targets",
			body: map[string]any{
				"targets": []string{"arn:aws:iot:us-east-1:123:thing/t1"},
				"comment": "test",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "AssociateTargetsWithJob",
			wantResource: "job-42",
		},
		{
			name:   "AttachPolicy",
			method: http.MethodPut,
			path:   "/target-policies/my-policy",
			body: map[string]any{
				"target": "arn:aws:iot:us-east-1:000000000000:cert/xyz",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "AttachPolicy",
			wantResource: "my-policy",
		},
		{
			name:   "AttachSecurityProfile",
			method: http.MethodPut,
			path: "/security-profiles/sp-1/targets?" +
				"securityProfileTargetArn=arn:aws:iot:us-east-1:000000000000:all/things",
			body:         nil,
			wantStatus:   http.StatusOK,
			wantOp:       "AttachSecurityProfile",
			wantResource: "sp-1",
		},
		{
			name:   "AttachThingPrincipal",
			method: http.MethodPut,
			path:   "/things/my-thing/principals",
			body:   nil,
			headers: map[string]string{
				"x-amzn-principal": "arn:aws:iot:us-east-1:000000000000:cert/certid",
			},
			wantStatus:   http.StatusOK,
			wantOp:       "AttachThingPrincipal",
			wantResource: "my-thing",
		},
		{
			name:         "CancelAuditMitigationActionsTask",
			method:       http.MethodPut,
			path:         "/audit/mitigationactions/tasks/task-99/cancel",
			body:         nil,
			wantStatus:   http.StatusOK,
			wantOp:       "CancelAuditMitigationActionsTask",
			wantResource: "task-99",
		},
		{
			name:         "CancelAuditTask",
			method:       http.MethodPut,
			path:         "/audit/tasks/audit-task-1/cancel",
			body:         nil,
			wantStatus:   http.StatusOK,
			wantOp:       "CancelAuditTask",
			wantResource: "audit-task-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := iot.NewInMemoryBackend()
			handler := iot.NewHandler(backend, nil)

			var reqBody []byte
			if tt.body != nil {
				var err error
				reqBody, err = json.Marshal(tt.body)
				require.NoError(t, err)
			}

			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader(reqBody))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)

			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, handler.ExtractOperation(c))
			assert.Equal(t, tt.wantResource, handler.ExtractResource(c))

			err := handler.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
