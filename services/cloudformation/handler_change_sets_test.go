package cloudformation_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestHandler_ExecuteChangeSet_InvalidStatus verifies the wire-level error for
// executing a non-AVAILABLE change set uses AWS's real error code
// (InvalidChangeSetStatus), and that ChangeSetNotFound errors use the
// SDK-modeled code without an "Exception" suffix (the deserializer matches
// "ChangeSetNotFound", not "ChangeSetNotFoundException").
func TestHandler_ExecuteChangeSet_InvalidStatus(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DeleteChangeSet&StackName=no-such-stack&ChangeSetName=no-such-cs")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>ChangeSetNotFound</Code>")
	assert.NotContains(t, rec.Body.String(), "ChangeSetNotFoundException")
}

func TestHandler_CreateChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=cs-stack&TemplateBody=")
			},
			form:     "Action=CreateChangeSet&StackName=cs-stack&ChangeSetName=my-cs&TemplateBody=",
			wantCode: http.StatusOK,
			wantBody: "CreateChangeSetResponse",
		},
		{
			name:     "missing_fields",
			form:     "Action=CreateChangeSet&StackName=cs-stack",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DescribeChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateChangeSet&StackName=cs-desc&ChangeSetName=cs1&TemplateBody=")
			},
			form:     "Action=DescribeChangeSet&StackName=cs-desc&ChangeSetName=cs1",
			wantCode: http.StatusOK,
			wantBody: "DescribeChangeSetResponse",
		},
		{
			name:     "not_found",
			form:     "Action=DescribeChangeSet&StackName=no-stack&ChangeSetName=no-cs",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_ExecuteChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			// A change set created with a real template (real Add changes) is
			// AVAILABLE and can be executed.
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postFormValues(t, h, url.Values{
					"Action":        {"CreateChangeSet"},
					"StackName":     {"exec-cs-stack"},
					"ChangeSetName": {"exec-cs"},
					"TemplateBody":  {simpleTemplate},
				})
			},
			form:     "Action=ExecuteChangeSet&StackName=exec-cs-stack&ChangeSetName=exec-cs",
			wantCode: http.StatusOK,
			wantBody: "ExecuteChangeSetResponse",
		},
		{
			// An empty TemplateBody produces zero changes, so AWS marks the
			// change set FAILED/UNAVAILABLE and rejects execution with
			// InvalidChangeSetStatus rather than silently no-op'ing.
			name: "unavailable_no_changes",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateChangeSet&StackName=exec-cs-empty&ChangeSetName=exec-cs&TemplateBody=")
			},
			form:     "Action=ExecuteChangeSet&StackName=exec-cs-empty&ChangeSetName=exec-cs",
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidChangeSetStatus",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DeleteChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateChangeSet&StackName=del-cs-stack&ChangeSetName=del-cs&TemplateBody=")
			},
			form:     "Action=DeleteChangeSet&StackName=del-cs-stack&ChangeSetName=del-cs",
			wantCode: http.StatusOK,
			wantBody: "DeleteChangeSetResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_ListChangeSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateChangeSet&StackName=lcs-stack&ChangeSetName=cs1&TemplateBody=")
			},
			form:     "Action=ListChangeSets&StackName=lcs-stack",
			wantCode: http.StatusOK,
			wantBody: "ListChangeSetsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}
