package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateIntegration ----

func TestHandler_CreateIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CreateIntegration&" +
				"Version=2012-12-01&IntegrationName=my-integration" +
				"&SourceArn=arn:aws:redshift:us-east-1:123:cluster/src" +
				"&TargetArn=arn:aws:redshift:us-east-1:123:namespace/tgt",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateIntegrationResponse", "my-integration", "active"},
		},
		{
			// Real aws-sdk-go-v2 clients send the KMS key as "KMSKeyId" (verified
			// against CreateIntegrationInput's query-protocol serializer), not
			// "KmsKeyId" -- this deliberately uses the real casing so the test
			// actually exercises production request parsing.
			name: "success_with_kms",
			body: "Action=CreateIntegration&" +
				"Version=2012-12-01&IntegrationName=kms-integration" +
				"&SourceArn=arn:aws:redshift:us-east-1:123:cluster/src" +
				"&TargetArn=arn:aws:redshift:us-east-1:123:namespace/tgt&KMSKeyId=key123",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateIntegrationResponse", "kms-integration", "key123"},
		},
		{
			// TagList is the real request field name for tags on CreateIntegration
			// (unlike most other Create* ops in this service, which use "Tags").
			name: "success_with_tags",
			body: "Action=CreateIntegration&" +
				"Version=2012-12-01&IntegrationName=tagged-integration" +
				"&SourceArn=arn:aws:redshift:us-east-1:123:cluster/src" +
				"&TargetArn=arn:aws:redshift:us-east-1:123:namespace/tgt" +
				"&TagList.Tag.1.Key=env&TagList.Tag.1.Value=prod",
			wantCode: http.StatusOK,
			wantContains: []string{
				"CreateIntegrationResponse", "tagged-integration", "<Key>env</Key>", "<Value>prod</Value>",
			},
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateIntegration&Version=2012-12-01&IntegrationName=dup-integration&SourceArn=arn:src&TargetArn=arn:tgt",
				)
			},
			body: "Action=CreateIntegration&" +
				"Version=2012-12-01&IntegrationName=dup-integration&SourceArn=arn:src&TargetArn=arn:tgt",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IntegrationAlreadyExists"},
		},
		{
			name:         "missing_name",
			body:         "Action=CreateIntegration&Version=2012-12-01&IntegrationName=&SourceArn=arn:src&TargetArn=arn:tgt",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_source_arn",
			body:         "Action=CreateIntegration&Version=2012-12-01&IntegrationName=test&TargetArn=arn:tgt",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_target_arn",
			body:         "Action=CreateIntegration&Version=2012-12-01&IntegrationName=test&SourceArn=arn:src",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteIntegration ----

func TestHandler_DeleteIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				rec := postRedshiftForm(
					t,
					h,
					"Action=CreateIntegration&Version=2012-12-01&IntegrationName=del-integration&SourceArn=arn:src&TargetArn=arn:tgt",
				)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: "Action=DeleteIntegration&" +
				"Version=2012-12-01&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/del-integration",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteIntegrationResponse"},
		},
		{
			name: "not_found",
			body: "Action=DeleteIntegration&" +
				"Version=2012-12-01&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IntegrationNotFound"},
		},
		{
			name:         "missing_arn",
			body:         "Action=DeleteIntegration&Version=2012-12-01&IntegrationArn=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeIntegrations ----

func TestHandler_DescribeIntegrations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "list_all",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateIntegration&Version=2012-12-01&IntegrationName=ig-a&SourceArn=arn:src&TargetArn=arn:tgt",
				)
				postRedshiftForm(
					t,
					h,
					"Action=CreateIntegration&Version=2012-12-01&IntegrationName=ig-b&SourceArn=arn:src&TargetArn=arn:tgt",
				)
			},
			body:         "Action=DescribeIntegrations&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeIntegrationsResponse", "ig-a", "ig-b"},
		},
		{
			name:         "empty",
			body:         "Action=DescribeIntegrations&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeIntegrationsResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestHandler_DescribeIntegrations_StatusFilter verifies the "status" filter
// name (DescribeIntegrationsFilterName, redshift@v1.65.4 types/enums.go:194-202)
// actually narrows results. Every integration this backend creates gets
// Status=active (integrations.go CreateIntegration) and nothing ever changes
// it, so a filter for any other status must exclude every integration.
func TestHandler_DescribeIntegrations_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h,
		"Action=CreateIntegration&Version=2012-12-01&IntegrationName=ig-status-a&SourceArn=arn:src&TargetArn=arn:tgt")
	postRedshiftForm(t, h,
		"Action=CreateIntegration&Version=2012-12-01&IntegrationName=ig-status-b&SourceArn=arn:src&TargetArn=arn:tgt")

	rec := postRedshiftForm(t, h,
		"Action=DescribeIntegrations&Version=2012-12-01"+
			"&Filters.DescribeIntegrationsFilter.1.Name=status"+
			"&Filters.DescribeIntegrationsFilter.1.Values.Value.1=creating")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "ig-status-a", "status=creating must exclude every active integration")
	assert.NotContains(t, body, "ig-status-b", "status=creating must exclude every active integration")
}

// ---- ModifyIntegration ----

func TestHandler_ModifyIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateIntegration&"+
						"Version=2012-12-01&IntegrationName=mod-integration&SourceArn=arn:src&TargetArn=arn:tgt&Description=old",
				)
			},
			body: "Action=ModifyIntegration&" +
				"Version=2012-12-01" +
				"&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/mod-integration" +
				"&Description=new-desc",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyIntegrationResponse", "new-desc"},
		},
		{
			// Real ModifyIntegrationInput also supports renaming via IntegrationName.
			name: "success_rename",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateIntegration&"+
						"Version=2012-12-01&IntegrationName=rename-integration&SourceArn=arn:src&TargetArn=arn:tgt",
				)
			},
			body: "Action=ModifyIntegration&" +
				"Version=2012-12-01" +
				"&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/rename-integration" +
				"&IntegrationName=renamed-integration",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyIntegrationResponse", "renamed-integration"},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyIntegration&Version=2012-12-01&IntegrationArn=arn:missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IntegrationNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- Backend: Integration count tracking ----

func TestBackend_Integration_Count(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	assert.Equal(t, 0, redshift.IntegrationCount(b))

	postRedshiftForm(t, h,
		"Action=CreateIntegration&Version=2012-12-01&IntegrationName=ig1&SourceArn=arn:src&TargetArn=arn:tgt")

	assert.Equal(t, 1, redshift.IntegrationCount(b))
}

// ---- CRUD Lifecycle ----

func TestHandler_Integration_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	// Create
	rec := postRedshiftForm(
		t,
		h,
		"Action=CreateIntegration&"+
			"Version=2012-12-01&IntegrationName=lc-integration"+
			"&SourceArn=arn:aws:redshift:us-east-1:123:cluster/src"+
			"&TargetArn=arn:aws:redshift:us-east-1:123:namespace/tgt&Description=initial",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lc-integration")
	assert.Contains(t, rec.Body.String(), "active")

	// Describe — should appear
	rec = postRedshiftForm(t, h,
		"Action=DescribeIntegrations&Version=2012-12-01")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lc-integration")

	// Modify description
	rec = postRedshiftForm(
		t,
		h,
		"Action=ModifyIntegration&"+
			"Version=2012-12-01"+
			"&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/lc-integration"+
			"&Description=updated",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "updated")

	// Delete
	rec = postRedshiftForm(
		t,
		h,
		"Action=DeleteIntegration&"+
			"Version=2012-12-01&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/lc-integration",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete — not found
	rec = postRedshiftForm(
		t,
		h,
		"Action=DescribeIntegrations&"+
			"Version=2012-12-01&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/lc-integration",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "IntegrationNotFound")
}
