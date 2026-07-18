package cloudformation_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkResponse func(t *testing.T, body []byte)
		name          string
		form          string
		wantBody      string
		wantCode      int
	}{
		{
			name: "success",
			form: "Action=CreateStack&StackName=test-stack&TemplateBody=" +
				`{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"R":{"Type":"AWS::S3::Bucket","Properties":{}}}}`,
			wantCode: http.StatusOK,
			wantBody: "CreateStackResponse",
			checkResponse: func(t *testing.T, body []byte) {
				t.Helper()
				var resp struct {
					XMLName xml.Name `xml:"CreateStackResponse"`
					Result  struct {
						StackID string `xml:"StackId"`
					} `xml:"CreateStackResult"`
					RequestID string `xml:"ResponseMetadata>RequestId"`
				}
				require.NoError(t, xml.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp.Result.StackID)
				assert.NotEmpty(t, resp.RequestID)
			},
		},
		{
			name:     "missing_name",
			form:     "Action=CreateStack",
			wantCode: http.StatusBadRequest,
			wantBody: "ValidationError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}

			if tt.checkResponse != nil {
				tt.checkResponse(t, rec.Body.Bytes())
			}
		})
	}
}

func TestHandler_DescribeStacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, h *cloudformation.Handler)
		checkResponse func(t *testing.T, body []byte)
		name          string
		form          string
		wantBody      string
		wantCode      int
	}{
		{
			name: "all",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=desc-all&TemplateBody=")
			},
			form:     "Action=DescribeStacks",
			wantCode: http.StatusOK,
			wantBody: "DescribeStacksResponse",
		},
		{
			name: "by_name",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=named-stack&TemplateBody=")
			},
			form:     "Action=DescribeStacks&StackName=named-stack",
			wantCode: http.StatusOK,
			wantBody: "named-stack",
		},
		{
			name:     "not_found",
			form:     "Action=DescribeStacks&StackName=no-such-stack",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "xml_response",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=desc-xml-stack&TemplateBody=")
			},
			form:     "Action=DescribeStacks&StackName=desc-xml-stack",
			wantCode: http.StatusOK,
			checkResponse: func(t *testing.T, body []byte) {
				t.Helper()
				var resp struct {
					XMLName xml.Name `xml:"DescribeStacksResponse"`
					Result  struct {
						Stacks []struct {
							StackName   string `xml:"StackName"`
							StackStatus string `xml:"StackStatus"`
						} `xml:"Stacks>member"`
					} `xml:"DescribeStacksResult"`
				}
				require.NoError(t, xml.Unmarshal(body, &resp))
				require.Len(t, resp.Result.Stacks, 1)
				assert.Equal(t, "desc-xml-stack", resp.Result.Stacks[0].StackName)
			},
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

			if tt.checkResponse != nil {
				tt.checkResponse(t, rec.Body.Bytes())
			}
		})
	}
}

func TestHandler_UpdateStack(t *testing.T) {
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
				postForm(t, h, "Action=CreateStack&StackName=upd-stack&TemplateBody=")
			},
			form:     "Action=UpdateStack&StackName=upd-stack&TemplateBody=",
			wantCode: http.StatusOK,
			wantBody: "UpdateStackResponse",
		},
		{
			name:     "missing_name",
			form:     "Action=UpdateStack",
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

func TestHandler_DeleteStack(t *testing.T) {
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
				postForm(t, h, "Action=CreateStack&StackName=del-stack&TemplateBody=")
			},
			form:     "Action=DeleteStack&StackName=del-stack",
			wantCode: http.StatusOK,
			wantBody: "DeleteStackResponse",
		},
		{
			name:     "missing_name",
			form:     "Action=DeleteStack",
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

func TestHandler_ListStacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudformation.Handler)
		name     string
		form     string
		wantBody string
		wantCode int
	}{
		{
			name: "all",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=ls-stack&TemplateBody=")
			},
			form:     "Action=ListStacks",
			wantCode: http.StatusOK,
			wantBody: "ListStacksResponse",
		},
		{
			name: "with_filter",
			setup: func(t *testing.T, h *cloudformation.Handler) {
				t.Helper()
				postForm(t, h, "Action=CreateStack&StackName=filt-stack&TemplateBody=")
			},
			form:     "Action=ListStacks&StackStatusFilter.member.1=CREATE_COMPLETE",
			wantCode: http.StatusOK,
			wantBody: "filt-stack",
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

func TestHandler_DescribeStackEvents(t *testing.T) {
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
				postForm(t, h, "Action=CreateStack&StackName=evt-stack&TemplateBody=")
			},
			form:     "Action=DescribeStackEvents&StackName=evt-stack",
			wantCode: http.StatusOK,
			wantBody: "DescribeStackEventsResponse",
		},
		{
			name:     "missing_name",
			form:     "Action=DescribeStackEvents",
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

// TestHandler_DescribeStackEvents_NextToken verifies the wire-level NextToken
// round-trips: the first response carries a NextToken when more events exist
// than the default page size, and requesting with that token returns the
// remaining (disjoint) events.
func TestHandler_DescribeStackEvents_NextToken(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postForm(t, h, "Action=CreateStack&StackName=evt-tok-stack&TemplateBody="+simpleTemplate)

	for range 40 {
		postForm(t, h, "Action=UpdateStack&StackName=evt-tok-stack&TemplateBody="+simpleTemplate)
	}

	rec := postForm(t, h, "Action=DescribeStackEvents&StackName=evt-tok-stack")
	require.Equal(t, http.StatusOK, rec.Code)

	type eventXML struct {
		EventID string `xml:"EventId"`
	}
	type eventsResult struct {
		NextToken   string     `xml:"NextToken"`
		StackEvents []eventXML `xml:"StackEvents>member"`
	}
	type resp struct {
		Result eventsResult `xml:"DescribeStackEventsResult"`
	}

	var first resp
	require.NoError(t, xml.NewDecoder(strings.NewReader(rec.Body.String())).Decode(&first))
	require.NotEmpty(t, first.Result.NextToken, "expected pagination to kick in past the default page size")

	rec2 := postForm(t, h,
		"Action=DescribeStackEvents&StackName=evt-tok-stack&NextToken="+url.QueryEscape(first.Result.NextToken))
	require.Equal(t, http.StatusOK, rec2.Code)

	var second resp
	require.NoError(t, xml.NewDecoder(strings.NewReader(rec2.Body.String())).Decode(&second))
	require.NotEmpty(t, second.Result.StackEvents)

	firstIDs := make(map[string]bool, len(first.Result.StackEvents))
	for _, e := range first.Result.StackEvents {
		firstIDs[e.EventID] = true
	}
	for _, e := range second.Result.StackEvents {
		assert.False(t, firstIDs[e.EventID], "second page must not repeat first-page events")
	}
}

func TestHandler_GetTemplate(t *testing.T) {
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
				postForm(t, h, "Action=CreateStack&StackName=tmpl-stack&TemplateBody={}")
			},
			form:     "Action=GetTemplate&StackName=tmpl-stack",
			wantCode: http.StatusOK,
			wantBody: "GetTemplateResponse",
		},
		{
			name:     "missing_name",
			form:     "Action=GetTemplate",
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
