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
			name: "success_with_kms",
			body: "Action=CreateIntegration&" +
				"Version=2012-12-01&IntegrationName=kms-integration" +
				"&SourceArn=arn:aws:redshift:us-east-1:123:cluster/src" +
				"&TargetArn=arn:aws:redshift:us-east-1:123:namespace/tgt&KmsKeyId=key123",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateIntegrationResponse", "kms-integration", "key123"},
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
