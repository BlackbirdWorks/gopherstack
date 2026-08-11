package cloudformation_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStackInstances_AccountFilterType covers DeploymentTargets.AccountFilterType
// across CreateStackInstances, UpdateStackInstances, and DeleteStackInstances: only
// the unset/NONE case (union of Accounts and OrganizationalUnitIds) is implemented,
// so INTERSECTION/DIFFERENCE/UNION must be rejected explicitly rather than silently
// computed as NONE.
func TestStackInstances_AccountFilterType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filterType string
		wantReject bool
	}{
		{name: "unset", filterType: "", wantReject: false},
		{name: "none", filterType: "NONE", wantReject: false},
		{name: "intersection", filterType: "INTERSECTION", wantReject: true},
		{name: "difference", filterType: "DIFFERENCE", wantReject: true},
		{name: "union", filterType: "UNION", wantReject: true},
	}

	for _, tt := range tests {
		t.Run("create_"+tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			postForm(t, h, url.Values{
				"Action":       {"CreateStackSet"},
				"StackSetName": {"filter-ss-" + tt.name},
				"TemplateBody": {simpleTemplate},
			}.Encode())

			form := url.Values{
				"Action":            {"CreateStackInstances"},
				"StackSetName":      {"filter-ss-" + tt.name},
				"Accounts.member.1": {"111111111111"},
				"Regions.member.1":  {"us-east-1"},
			}
			if tt.filterType != "" {
				form.Set("DeploymentTargets.AccountFilterType", tt.filterType)
			}
			rec := postForm(t, h, form.Encode())

			if tt.wantReject {
				assert.NotEqual(t, 200, rec.Code, "body: %s", rec.Body.String())
				assert.Contains(t, rec.Body.String(), "AccountFilterType")
			} else {
				require.Equal(t, 200, rec.Code, "body: %s", rec.Body.String())
			}
		})

		t.Run("update_"+tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			postForm(t, h, url.Values{
				"Action":       {"CreateStackSet"},
				"StackSetName": {"filter-upd-ss-" + tt.name},
				"TemplateBody": {simpleTemplate},
			}.Encode())

			form := url.Values{
				"Action":            {"UpdateStackInstances"},
				"StackSetName":      {"filter-upd-ss-" + tt.name},
				"Accounts.member.1": {"111111111111"},
				"Regions.member.1":  {"us-east-1"},
			}
			if tt.filterType != "" {
				form.Set("DeploymentTargets.AccountFilterType", tt.filterType)
			}
			rec := postForm(t, h, form.Encode())

			if tt.wantReject {
				assert.NotEqual(t, 200, rec.Code, "body: %s", rec.Body.String())
				assert.Contains(t, rec.Body.String(), "AccountFilterType")
			} else {
				require.Equal(t, 200, rec.Code, "body: %s", rec.Body.String())
			}
		})

		t.Run("delete_"+tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			postForm(t, h, url.Values{
				"Action":       {"CreateStackSet"},
				"StackSetName": {"filter-del-ss-" + tt.name},
				"TemplateBody": {simpleTemplate},
			}.Encode())

			form := url.Values{
				"Action":            {"DeleteStackInstances"},
				"StackSetName":      {"filter-del-ss-" + tt.name},
				"Accounts.member.1": {"111111111111"},
				"Regions.member.1":  {"us-east-1"},
				"RetainStacks":      {"false"},
			}
			if tt.filterType != "" {
				form.Set("DeploymentTargets.AccountFilterType", tt.filterType)
			}
			rec := postForm(t, h, form.Encode())

			if tt.wantReject {
				assert.NotEqual(t, 200, rec.Code, "body: %s", rec.Body.String())
				assert.Contains(t, rec.Body.String(), "AccountFilterType")
			} else {
				require.Equal(t, 200, rec.Code, "body: %s", rec.Body.String())
			}
		})
	}
}
