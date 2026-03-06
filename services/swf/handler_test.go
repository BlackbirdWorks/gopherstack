package swf_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/swf"
)

func newTestSWFHandler(t *testing.T) *swf.Handler {
	t.Helper()

	return swf.NewHandler(swf.NewInMemoryBackend())
}

func doSWFRequest(t *testing.T, h *swf.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "SimpleWorkflowService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

type setupAction struct {
	body   any
	action string
}

func TestSWFHandler_Actions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body              any
		name              string
		action            string
		wantRespContains  string
		wantNotEmptyField string
		setup             []setupAction
		wantCode          int
	}{
		{
			name:     "RegisterDomain",
			action:   "RegisterDomain",
			body:     map[string]any{"name": "my-domain", "description": "test"},
			wantCode: http.StatusOK,
		},
		{
			name: "ListDomains",
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "d1"}},
				{action: "RegisterDomain", body: map[string]any{"name": "d2"}},
			},
			action:           "ListDomains",
			body:             map[string]any{"registrationStatus": "REGISTERED"},
			wantCode:         http.StatusOK,
			wantRespContains: "domainInfos",
		},
		{
			name:     "DeprecateDomain",
			setup:    []setupAction{{action: "RegisterDomain", body: map[string]any{"name": "my-domain"}}},
			action:   "DeprecateDomain",
			body:     map[string]any{"name": "my-domain"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeprecateDomain_NotFound",
			action:   "DeprecateDomain",
			body:     map[string]any{"name": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "RegisterWorkflowType",
			action:   "RegisterWorkflowType",
			body:     map[string]any{"domain": "my-domain", "name": "my-workflow", "version": "1.0"},
			wantCode: http.StatusOK,
		},
		{
			name: "ListWorkflowTypes",
			setup: []setupAction{
				{action: "RegisterWorkflowType", body: map[string]any{"domain": "d1", "name": "wf1", "version": "1.0"}},
			},
			action:           "ListWorkflowTypes",
			body:             map[string]any{"domain": "d1"},
			wantCode:         http.StatusOK,
			wantRespContains: "typeInfos",
		},
		{
			name:              "StartWorkflowExecution",
			action:            "StartWorkflowExecution",
			body:              map[string]any{"domain": "my-domain", "workflowId": "wf-001"},
			wantCode:          http.StatusOK,
			wantNotEmptyField: "runId",
		},
		{
			name: "DescribeWorkflowExecution",
			setup: []setupAction{
				{action: "StartWorkflowExecution", body: map[string]any{"domain": "d1", "workflowId": "wf-001"}},
			},
			action:           "DescribeWorkflowExecution",
			body:             map[string]any{"domain": "d1", "execution": map[string]any{"workflowId": "wf-001"}},
			wantCode:         http.StatusOK,
			wantRespContains: "executionInfo",
		},
		{
			name:     "UnknownAction",
			action:   "UnknownAction",
			body:     nil,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "RegisterDomain_AlreadyExists",
			setup:    []setupAction{{action: "RegisterDomain", body: map[string]any{"name": "my-domain"}}},
			action:   "RegisterDomain",
			body:     map[string]any{"name": "my-domain"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "RegisterDomain_Deprecated",
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "my-domain"}},
				{action: "DeprecateDomain", body: map[string]any{"name": "my-domain"}},
			},
			action:   "RegisterDomain",
			body:     map[string]any{"name": "my-domain"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "RegisterWorkflowType_AlreadyExists",
			setup: []setupAction{{action: "RegisterWorkflowType", body: map[string]any{
				"domain": "my-domain", "name": "my-wf", "version": "1.0",
			}}},
			action:   "RegisterWorkflowType",
			body:     map[string]any{"domain": "my-domain", "name": "my-wf", "version": "1.0"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DescribeWorkflowExecution_NotFound",
			action:   "DescribeWorkflowExecution",
			body:     map[string]any{"domain": "d1", "execution": map[string]any{"workflowId": "nonexistent"}},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)

			for _, s := range tt.setup {
				doSWFRequest(t, h, s.action, s.body)
			}

			rec := doSWFRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantRespContains != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, tt.wantRespContains)
			}

			if tt.wantNotEmptyField != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp[tt.wantNotEmptyField])
			}
		})
	}
}

func TestSWFHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "Match",
			target:    "SimpleWorkflowService.RegisterDomain",
			wantMatch: true,
		},
		{
			name:      "NoMatch",
			target:    "Firehose_20150804.CreateDeliveryStream",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestSWFHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	assert.Equal(t, "SWF", h.Name())
}

func TestSWFHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "RegisterDomain")
	assert.Contains(t, ops, "ListDomains")
	assert.Contains(t, ops, "DeprecateDomain")
	assert.Contains(t, ops, "RegisterWorkflowType")
	assert.Contains(t, ops, "ListWorkflowTypes")
	assert.Contains(t, ops, "StartWorkflowExecution")
	assert.Contains(t, ops, "DescribeWorkflowExecution")
}

func TestSWFHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestSWFHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "WithTarget",
			target: "SimpleWorkflowService.RegisterDomain",
			wantOp: "RegisterDomain",
		},
		{
			name:   "NoTarget",
			target: "",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestSWFHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantResource string
	}{
		{
			name:         "NameField",
			body:         `{"name":"my-domain"}`,
			wantResource: "my-domain",
		},
		{
			name:         "DomainFallback",
			body:         `{"domain":"test-domain"}`,
			wantResource: "test-domain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

func TestSWFProvider(t *testing.T) {
	t.Parallel()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		p := &swf.Provider{}
		assert.Equal(t, "SWF", p.Name())
	})

	t.Run("Init", func(t *testing.T) {
		t.Parallel()

		p := &swf.Provider{}
		ctx := &service.AppContext{Logger: slog.Default()}
		svc, err := p.Init(ctx)
		require.NoError(t, err)
		assert.NotNil(t, svc)
		assert.Equal(t, "SWF", svc.Name())
	})
}

// TestSWFHandler_ErrorTypes verifies that typed SWF faults include __type in the JSON response
// so that the AWS SDK v2 can deserialize them to the correct error types.
func TestSWFHandler_ErrorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantType string
		setup    []setupAction
		wantCode int
	}{
		{
			name:   "DomainAlreadyExistsFault",
			action: "RegisterDomain",
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "dup-domain"}},
			},
			body:     map[string]any{"name": "dup-domain"},
			wantCode: http.StatusBadRequest,
			wantType: "DomainAlreadyExistsFault",
		},
		{
			name:   "DomainDeprecatedFault",
			action: "RegisterDomain",
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "dep-domain"}},
				{action: "DeprecateDomain", body: map[string]any{"name": "dep-domain"}},
			},
			body:     map[string]any{"name": "dep-domain"},
			wantCode: http.StatusBadRequest,
			wantType: "DomainDeprecatedFault",
		},
		{
			name:   "TypeAlreadyExistsFault",
			action: "RegisterWorkflowType",
			setup: []setupAction{
				{action: "RegisterWorkflowType", body: map[string]any{"domain": "d1", "name": "wf1", "version": "1.0"}},
			},
			body:     map[string]any{"domain": "d1", "name": "wf1", "version": "1.0"},
			wantCode: http.StatusBadRequest,
			wantType: "TypeAlreadyExistsFault",
		},
		{
			name:     "UnknownResourceFault",
			action:   "DeprecateDomain",
			body:     map[string]any{"name": "nonexistent"},
			wantCode: http.StatusNotFound,
			wantType: "UnknownResourceFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)

			for _, s := range tt.setup {
				doSWFRequest(t, h, s.action, s.body)
			}

			rec := doSWFRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantType, resp["__type"])
		})
	}
}
