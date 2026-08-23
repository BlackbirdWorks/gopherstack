package cloudformation_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns_cloudformation", want: "CloudFormation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want int
	}{
		{name: "returns_80", want: 80},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			assert.Equal(t, tt.want, h.MatchPriority())
		})
	}
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantOperations []string
	}{
		{
			name: "contains_expected_operations",
			wantOperations: []string{
				"CreateStack",
				"DescribeStacks",
				"DeleteStack",
				"ListStacks",
				"CreateChangeSet",
				"ExecuteChangeSet",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			ops := h.GetSupportedOperations()

			for _, op := range tt.wantOperations {
				assert.Contains(t, ops, op)
			}
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		method      string
		contentType string
		body        string
		parseForm   bool
		wantMatch   bool
	}{
		{
			name:        "match",
			method:      http.MethodPost,
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=CreateStack",
			wantMatch:   true,
		},
		{
			name:        "wrong_method",
			method:      http.MethodGet,
			contentType: "application/x-www-form-urlencoded",
			body:        "",
			wantMatch:   false,
		},
		{
			name:        "wrong_content_type",
			method:      http.MethodPost,
			contentType: "application/json",
			body:        "Action=CreateStack",
			wantMatch:   false,
		},
		{
			name:        "unsupported_action",
			method:      http.MethodPost,
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=UnknownAction",
			parseForm:   true,
			wantMatch:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			e := echo.New()
			matcher := h.RouteMatcher()

			req := httptest.NewRequest(tt.method, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.contentType)

			if tt.parseForm {
				require.NoError(t, req.ParseForm())
			}

			got := matcher(e.NewContext(req, httptest.NewRecorder()))
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		wantOp string
	}{
		{
			name:   "describe_stacks",
			body:   "Action=DescribeStacks",
			wantOp: "DescribeStacks",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantName string
	}{
		{
			name:     "stack_name",
			body:     "Action=DescribeStacks&StackName=my-stack",
			wantName: "my-stack",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantName, h.ExtractResource(c))
		})
	}
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name:     "invalid_action",
			form:     "Action=UnknownAction",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidAction",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns_cloudformation", want: "CloudFormation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &cloudformation.Provider{}
			assert.Equal(t, tt.want, p.Name())
		})
	}
}

func TestHandler_DescribeType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		form         string
		wantContains []string
		wantCode     int
	}{
		{
			name:     "known_type_logs_loggroup",
			form:     "Action=DescribeType&TypeName=AWS::Logs::LogGroup",
			wantCode: http.StatusOK,
			wantContains: []string{
				"AWS::Logs::LogGroup",
				"LogGroupName",
				"RESOURCE",
				"DescribeTypeResponse",
			},
		},
		{
			name:     "unknown_type_uses_generic_id",
			form:     "Action=DescribeType&TypeName=AWS::Custom::Widget",
			wantCode: http.StatusOK,
			wantContains: []string{
				"AWS::Custom::Widget",
				"RESOURCE",
				"DescribeTypeResponse",
			},
		},
		{
			name:     "missing_type_name_returns_error",
			form:     "Action=DescribeType",
			wantCode: http.StatusBadRequest,
			wantContains: []string{
				"CFNRegistryException",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			body := rec.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  any
		wantSvc string
	}{
		{
			name:    "nil_config",
			config:  nil,
			wantSvc: "CloudFormation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &cloudformation.Provider{}
			appCtx := &service.AppContext{
				Logger: slog.Default(),
				Config: tt.config,
			}
			svc, err := p.Init(appCtx)
			require.NoError(t, err)
			require.NotNil(t, svc)
			assert.Equal(t, tt.wantSvc, svc.Name())
		})
	}
}
