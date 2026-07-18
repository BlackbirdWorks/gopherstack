package cloudformation_test

import (
	"errors"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// ---- Backend: DescribeStackResource -----------------------------------------

func TestBackend_DescribeStackResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*cloudformation.InMemoryBackend, string)
		name       string
		stackInput string
		logicalID  string
		wantErr    error
		wantType   string
	}{
		{
			name:       "resource_found",
			stackInput: simpleTemplate,
			logicalID:  "MyBucket",
			wantType:   "AWS::S3::Bucket",
		},
		{
			name:       "resource_not_found",
			stackInput: simpleTemplate,
			logicalID:  "NonExistent",
			wantErr:    cloudformation.ErrResourceNotFound,
		},
		{
			name:      "stack_not_found",
			logicalID: "MyBucket",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stackName := "test-stack-" + tt.name

			if tt.stackInput != "" {
				_, err := b.CreateStack(t.Context(), stackName, tt.stackInput, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			}

			lookupName := stackName
			if errors.Is(tt.wantErr, cloudformation.ErrStackNotFound) {
				lookupName = "no-such-stack"
			}

			res, err := b.DescribeStackResource(lookupName, tt.logicalID)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, tt.logicalID, res.LogicalID)
			assert.Equal(t, tt.wantType, res.Type)
			assert.NotEmpty(t, res.PhysicalID)
			assert.Equal(t, "CREATE_COMPLETE", res.Status)
			assert.False(t, res.Timestamp.IsZero())
		})
	}
}

// ---- Backend: ListStackResources --------------------------------------------

func TestBackend_ListStackResources(t *testing.T) {
	t.Parallel()

	multiTemplate := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"BucketA": {"Type": "AWS::S3::Bucket", "Properties": {}},
			"BucketB": {"Type": "AWS::S3::Bucket", "Properties": {}}
		}
	}`

	tests := []struct {
		wantErr       error
		name          string
		template      string
		wantLogicalID string
		wantCount     int
	}{
		{
			name:      "lists_all_resources",
			template:  multiTemplate,
			wantCount: 2,
		},
		{
			name:     "stack_not_found",
			template: "",
			wantErr:  cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stackName := "res-list-" + tt.name

			if tt.template != "" {
				_, err := b.CreateStack(t.Context(), stackName, tt.template, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			}

			lookupName := stackName
			if errors.Is(tt.wantErr, cloudformation.ErrStackNotFound) {
				lookupName = "no-such-stack"
			}

			p, err := b.ListStackResources(lookupName, "")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, p.Data, tt.wantCount)

			for _, s := range p.Data {
				assert.NotEmpty(t, s.LogicalResourceID)
				assert.NotEmpty(t, s.ResourceType)
				assert.NotEmpty(t, s.ResourceStatus)
				assert.False(t, s.Timestamp.IsZero())
			}
		})
	}
}

// ---- Backend: DescribeStackResources ----------------------------------------

func TestBackend_DescribeStackResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		template  string
		wantCount int
	}{
		{
			name:      "returns_resources",
			template:  simpleTemplate,
			wantCount: 1,
		},
		{
			name:    "stack_not_found",
			wantErr: cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stackName := "desc-res-" + tt.name

			if tt.template != "" {
				_, err := b.CreateStack(t.Context(), stackName, tt.template, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			}

			lookupName := stackName
			if errors.Is(tt.wantErr, cloudformation.ErrStackNotFound) {
				lookupName = "no-such-stack"
			}

			resources, err := b.DescribeStackResources(lookupName)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, resources, tt.wantCount)
		})
	}
}

// ---- Handler: DescribeStackResource -----------------------------------------

func TestHandler_DescribeStackResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*cloudformation.Handler) string
		name      string
		action    string
		logicalID string
		wantElem  string
		wantCode  int
	}{
		{
			name:      "success",
			action:    "DescribeStackResource",
			logicalID: "MyBucket",
			wantCode:  200,
			wantElem:  "DescribeStackResourceResult",
			setup: func(h *cloudformation.Handler) string {
				body := url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {"handler-res-stack"},
					"TemplateBody": {simpleTemplate},
				}.Encode()
				postForm(t, h, body)

				return "handler-res-stack"
			},
		},
		{
			name:      "missing_params",
			action:    "DescribeStackResource",
			logicalID: "",
			wantCode:  400,
			wantElem:  "ErrorResponse",
			setup:     func(_ *cloudformation.Handler) string { return "irrelevant" },
		},
		{
			name:      "resource_not_found",
			action:    "DescribeStackResource",
			logicalID: "NonExistent",
			wantCode:  400,
			wantElem:  "ErrorResponse",
			setup: func(h *cloudformation.Handler) string {
				body := url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {"handler-res-stack-miss"},
					"TemplateBody": {simpleTemplate},
				}.Encode()
				postForm(t, h, body)

				return "handler-res-stack-miss"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			stackName := tt.setup(h)

			var formBody string
			if tt.logicalID == "" {
				formBody = url.Values{
					"Action":    {tt.action},
					"StackName": {stackName},
				}.Encode()
			} else {
				formBody = url.Values{
					"Action":            {tt.action},
					"StackName":         {stackName},
					"LogicalResourceId": {tt.logicalID},
				}.Encode()
			}

			rec := postForm(t, h, formBody)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantElem)
		})
	}
}

// ---- Handler: ListStackResources -------------------------------------------

func TestHandler_ListStackResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantElem string
		wantCode int
		noStack  bool
	}{
		{
			name:     "success",
			wantCode: 200,
			wantElem: "ListStackResourcesResult",
		},
		{
			name:     "missing_stack_name",
			wantCode: 400,
			wantElem: "ErrorResponse",
			noStack:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			stackName := "lsr-stack-" + tt.name

			if !tt.noStack {
				postForm(t, h, url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {stackName},
					"TemplateBody": {simpleTemplate},
				}.Encode())
			}

			var formBody string
			if tt.noStack {
				formBody = url.Values{
					"Action": {"ListStackResources"},
				}.Encode()
			} else {
				formBody = url.Values{
					"Action":    {"ListStackResources"},
					"StackName": {stackName},
				}.Encode()
			}

			rec := postForm(t, h, formBody)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantElem)
		})
	}
}

// ---- Handler: DescribeStackResources ----------------------------------------

func TestHandler_DescribeStackResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantElem string
		wantCode int
		noStack  bool
	}{
		{
			name:     "success",
			wantCode: 200,
			wantElem: "DescribeStackResourcesResult",
		},
		{
			name:     "missing_stack_name",
			wantCode: 400,
			wantElem: "ErrorResponse",
			noStack:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			stackName := "dsr-stack-" + tt.name

			if !tt.noStack {
				postForm(t, h, url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {stackName},
					"TemplateBody": {simpleTemplate},
				}.Encode())
			}

			var formBody string
			if tt.noStack {
				formBody = url.Values{
					"Action": {"DescribeStackResources"},
				}.Encode()
			} else {
				formBody = url.Values{
					"Action":    {"DescribeStackResources"},
					"StackName": {stackName},
				}.Encode()
			}

			rec := postForm(t, h, formBody)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantElem)
		})
	}
}
