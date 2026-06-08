package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func accountSingularPath(sub string) string {
	return fmt.Sprintf("/account/%s%s", testAccountID, sub)
}

// ---- Folder tests ---- //nolint:godot // existing issue.
func TestQuickSight_Folders(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		body       any
		setup      func(h any)
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create folder",
			method:     http.MethodPost,
			path:       accountPath("/folders/f1"),
			body:       map[string]any{"Name": "MyFolder"},
			wantStatus: http.StatusOK,
			wantKey:    "FolderId",
		},
		{
			name:       "describe folder",
			method:     http.MethodGet,
			path:       accountPath("/folders/f1"),
			wantStatus: http.StatusOK,
			wantKey:    "Folder",
		},
		{
			name:       "update folder",
			method:     http.MethodPut,
			path:       accountPath("/folders/f1"),
			body:       map[string]any{"Name": "Renamed"},
			wantStatus: http.StatusOK,
			wantKey:    "FolderId",
		},
		{
			name:       "list folders",
			method:     http.MethodGet,
			path:       accountPath("/folders"),
			wantStatus: http.StatusOK,
			wantKey:    "FolderSummaryList",
		},
		{
			name:       "describe folder permissions",
			method:     http.MethodGet,
			path:       accountPath("/folders/f1/permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "FolderId",
		},
		{
			name:       "update folder permissions",
			method:     http.MethodPut,
			path:       accountPath("/folders/f1/permissions"),
			body:       map[string]any{"GrantPermissions": []any{}, "RevokePermissions": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "FolderId",
		},
		{
			name:       "describe folder resolved permissions",
			method:     http.MethodGet,
			path:       accountPath("/folders/f1/resolved-permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "FolderId",
		},
		{
			name:       "create folder membership",
			method:     http.MethodPut,
			path:       accountPath("/folders/f1/members/DATASET/ds1"),
			wantStatus: http.StatusOK,
			wantKey:    "FolderMember",
		},
		{
			name:       "list folder members",
			method:     http.MethodGet,
			path:       accountPath("/folders/f1/members"),
			wantStatus: http.StatusOK,
			wantKey:    "FolderMemberList",
		},
		{
			name:       "list folders for resource",
			method:     http.MethodGet,
			path:       accountPath("/resource/ds1resarn/folders"),
			wantStatus: http.StatusOK,
			wantKey:    "Folders",
		},
		{
			name:       "delete folder membership",
			method:     http.MethodDelete,
			path:       accountPath("/folders/f1/members/DATASET/ds1"),
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete folder",
			method:     http.MethodDelete,
			path:       accountPath("/folders/f1"),
			wantStatus: http.StatusOK,
			wantKey:    "FolderId",
		},
	}

	h := newTestHandler(t)

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- Template tests ---- //nolint:godot // existing issue.
func TestQuickSight_Templates(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create template",
			method:     http.MethodPost,
			path:       accountPath("/templates/t1"),
			body:       map[string]any{"Name": "Tpl1"},
			wantStatus: http.StatusOK,
			wantKey:    "TemplateId",
		},
		{
			name:       "describe template",
			method:     http.MethodGet,
			path:       accountPath("/templates/t1"),
			wantStatus: http.StatusOK,
			wantKey:    "Template",
		},
		{
			name:       "update template",
			method:     http.MethodPut,
			path:       accountPath("/templates/t1"),
			body:       map[string]any{"Name": "TplRenamed"},
			wantStatus: http.StatusOK,
			wantKey:    "TemplateId",
		},
		{
			name:       "list templates",
			method:     http.MethodGet,
			path:       accountPath("/templates"),
			wantStatus: http.StatusOK,
			wantKey:    "TemplateSummaryList",
		},
		{
			name:       "describe template definition",
			method:     http.MethodGet,
			path:       accountPath("/templates/t1/definition"),
			wantStatus: http.StatusOK,
			wantKey:    "TemplateId",
		},
		{
			name:       "describe template permissions",
			method:     http.MethodGet,
			path:       accountPath("/templates/t1/permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "TemplateId",
		},
		{
			name:       "update template permissions",
			method:     http.MethodPut,
			path:       accountPath("/templates/t1/permissions"),
			body:       map[string]any{"GrantPermissions": []any{}, "RevokePermissions": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "TemplateId",
		},
		{
			name:       "create template alias",
			method:     http.MethodPost,
			path:       accountPath("/templates/t1/aliases/LATEST"),
			body:       map[string]any{"TemplateVersionNumber": float64(1)},
			wantStatus: http.StatusOK,
			wantKey:    "TemplateAlias",
		},
		{
			name:       "describe template alias",
			method:     http.MethodGet,
			path:       accountPath("/templates/t1/aliases/LATEST"),
			wantStatus: http.StatusOK,
			wantKey:    "TemplateAlias",
		},
		{
			name:       "list template aliases",
			method:     http.MethodGet,
			path:       accountPath("/templates/t1/aliases"),
			wantStatus: http.StatusOK,
			wantKey:    "TemplateAliasList",
		},
		{
			name:       "list template versions",
			method:     http.MethodGet,
			path:       accountPath("/templates/t1/versions"),
			wantStatus: http.StatusOK,
			wantKey:    "TemplateVersionSummaryList",
		},
		{
			name:       "update template alias",
			method:     http.MethodPut,
			path:       accountPath("/templates/t1/aliases/LATEST"),
			body:       map[string]any{"TemplateVersionNumber": float64(1)},
			wantStatus: http.StatusOK,
			wantKey:    "TemplateAlias",
		},
		{
			name:       "delete template alias",
			method:     http.MethodDelete,
			path:       accountPath("/templates/t1/aliases/LATEST"),
			wantStatus: http.StatusOK,
			wantKey:    "TemplateId",
		},
		{
			name:       "delete template",
			method:     http.MethodDelete,
			path:       accountPath("/templates/t1"),
			wantStatus: http.StatusOK,
			wantKey:    "TemplateId",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- Theme tests ---- //nolint:godot // existing issue.
func TestQuickSight_Themes(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create theme",
			method:     http.MethodPost,
			path:       accountPath("/themes/th1"),
			body:       map[string]any{"Name": "Theme1"},
			wantStatus: http.StatusOK,
			wantKey:    "ThemeId",
		},
		{
			name:       "describe theme",
			method:     http.MethodGet,
			path:       accountPath("/themes/th1"),
			wantStatus: http.StatusOK,
			wantKey:    "Theme",
		},
		{
			name:       "update theme",
			method:     http.MethodPut,
			path:       accountPath("/themes/th1"),
			body:       map[string]any{"Name": "ThemeRenamed"},
			wantStatus: http.StatusOK,
			wantKey:    "ThemeId",
		},
		{
			name:       "list themes",
			method:     http.MethodGet,
			path:       accountPath("/themes"),
			wantStatus: http.StatusOK,
			wantKey:    "ThemeSummaryList",
		},
		{
			name:       "describe theme permissions",
			method:     http.MethodGet,
			path:       accountPath("/themes/th1/permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "ThemeId",
		},
		{
			name:       "update theme permissions",
			method:     http.MethodPut,
			path:       accountPath("/themes/th1/permissions"),
			body:       map[string]any{"GrantPermissions": []any{}, "RevokePermissions": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "ThemeId",
		},
		{
			name:       "create theme alias",
			method:     http.MethodPost,
			path:       accountPath("/themes/th1/aliases/CURRENT"),
			body:       map[string]any{"ThemeVersionNumber": float64(1)},
			wantStatus: http.StatusOK,
			wantKey:    "ThemeAlias",
		},
		{
			name:       "describe theme alias",
			method:     http.MethodGet,
			path:       accountPath("/themes/th1/aliases/CURRENT"),
			wantStatus: http.StatusOK,
			wantKey:    "ThemeAlias",
		},
		{
			name:       "list theme aliases",
			method:     http.MethodGet,
			path:       accountPath("/themes/th1/aliases"),
			wantStatus: http.StatusOK,
			wantKey:    "ThemeAliasList",
		},
		{
			name:       "list theme versions",
			method:     http.MethodGet,
			path:       accountPath("/themes/th1/versions"),
			wantStatus: http.StatusOK,
			wantKey:    "ThemeVersionSummaryList",
		},
		{
			name:       "update theme alias",
			method:     http.MethodPut,
			path:       accountPath("/themes/th1/aliases/CURRENT"),
			body:       map[string]any{"ThemeVersionNumber": float64(1)},
			wantStatus: http.StatusOK,
			wantKey:    "ThemeAlias",
		},
		{
			name:       "delete theme alias",
			method:     http.MethodDelete,
			path:       accountPath("/themes/th1/aliases/CURRENT"),
			wantStatus: http.StatusOK,
			wantKey:    "ThemeId",
		},
		{
			name:       "delete theme",
			method:     http.MethodDelete,
			path:       accountPath("/themes/th1"),
			wantStatus: http.StatusOK,
			wantKey:    "ThemeId",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- Topic tests ---- //nolint:godot // existing issue.
func TestQuickSight_Topics(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create topic",
			method:     http.MethodPost,
			path:       accountPath("/topics"),
			body:       map[string]any{"TopicId": "top1", "Name": "Topic1"},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "describe topic",
			method:     http.MethodGet,
			path:       accountPath("/topics/top1"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "update topic",
			method:     http.MethodPut,
			path:       accountPath("/topics/top1"),
			body:       map[string]any{"Name": "Renamed"},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "list topics",
			method:     http.MethodGet,
			path:       accountPath("/topics"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicsSummaries",
		},
		{
			name:       "describe topic permissions",
			method:     http.MethodGet,
			path:       accountPath("/topics/top1/permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "update topic permissions",
			method:     http.MethodPut,
			path:       accountPath("/topics/top1/permissions"),
			body:       map[string]any{"GrantPermissions": []any{}, "RevokePermissions": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "describe topic refresh",
			method:     http.MethodGet,
			path:       accountPath("/topics/top1/refresh/ref1"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "create topic refresh schedule",
			method:     http.MethodPost,
			path:       accountPath("/topics/top1/schedules"),
			body:       map[string]any{"DatasetId": "ds1"},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "describe topic refresh schedule",
			method:     http.MethodGet,
			path:       accountPath("/topics/top1/schedules/ds1"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "list topic refresh schedules",
			method:     http.MethodGet,
			path:       accountPath("/topics/top1/schedules"),
			wantStatus: http.StatusOK,
			wantKey:    "RefreshSchedules",
		},
		{
			name:       "update topic refresh schedule",
			method:     http.MethodPut,
			path:       accountPath("/topics/top1/schedules/ds1"),
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "batch create reviewed answers",
			method:     http.MethodPost,
			path:       accountPath("/topics/top1/batch-create-reviewed-answers"),
			body:       map[string]any{"Answers": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "batch delete reviewed answers",
			method:     http.MethodPost,
			path:       accountPath("/topics/top1/batch-delete-reviewed-answers"),
			body:       map[string]any{"AnswerIds": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "list topic reviewed answers",
			method:     http.MethodGet,
			path:       accountPath("/topics/top1/reviewed-answers"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "delete topic refresh schedule",
			method:     http.MethodDelete,
			path:       accountPath("/topics/top1/schedules/ds1"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "delete topic",
			method:     http.MethodDelete,
			path:       accountPath("/topics/top1"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- VPC Connection tests ---- //nolint:godot // existing issue.
func TestQuickSight_VPCConnections(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create vpc connection",
			method:     http.MethodPost,
			path:       accountPath("/vpc-connections"),
			body:       map[string]any{"VPCConnectionId": "vpc1", "Name": "VPC1"},
			wantStatus: http.StatusOK,
			wantKey:    "VPCConnectionId",
		},
		{
			name:       "describe vpc connection",
			method:     http.MethodGet,
			path:       accountPath("/vpc-connections/vpc1"),
			wantStatus: http.StatusOK,
			wantKey:    "VPCConnection",
		},
		{
			name:       "update vpc connection",
			method:     http.MethodPut,
			path:       accountPath("/vpc-connections/vpc1"),
			body:       map[string]any{"Name": "VPC1Updated"},
			wantStatus: http.StatusOK,
			wantKey:    "VPCConnectionId",
		},
		{
			name:       "list vpc connections",
			method:     http.MethodGet,
			path:       accountPath("/vpc-connections"),
			wantStatus: http.StatusOK,
			wantKey:    "VPCConnectionSummaries",
		},
		{
			name:       "delete vpc connection",
			method:     http.MethodDelete,
			path:       accountPath("/vpc-connections/vpc1"),
			wantStatus: http.StatusOK,
			wantKey:    "VPCConnectionId",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- IAM Policy Assignment tests ---- //nolint:godot // existing issue.
func TestQuickSight_IAMPolicyAssignments(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create iam policy assignment",
			method:     http.MethodPost,
			path:       nsPath("/iam-policy-assignments/"),
			body:       map[string]any{"AssignmentName": "assign1", "AssignmentStatus": "ENABLED"},
			wantStatus: http.StatusOK,
			wantKey:    "AssignmentName",
		},
		{
			name:       "describe iam policy assignment",
			method:     http.MethodGet,
			path:       nsPath("/iam-policy-assignments/assign1"),
			wantStatus: http.StatusOK,
			wantKey:    "IAMPolicyAssignment",
		},
		{
			name:       "update iam policy assignment",
			method:     http.MethodPut,
			path:       nsPath("/iam-policy-assignments/assign1"),
			body:       map[string]any{"AssignmentStatus": "DISABLED"},
			wantStatus: http.StatusOK,
			wantKey:    "AssignmentName",
		},
		{
			name:       "list iam policy assignments",
			method:     http.MethodGet,
			path:       nsPath("/iam-policy-assignments"),
			wantStatus: http.StatusOK,
			wantKey:    "IAMPolicyAssignments",
		},
		{
			name:       "list iam policy assignments for user",
			method:     http.MethodGet,
			path:       nsPath("/v2/iam-policy-assignments"),
			wantStatus: http.StatusOK,
			wantKey:    "IAMPolicyAssignments",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- Custom Permissions tests ---- //nolint:godot // existing issue.
func TestQuickSight_CustomPermissions(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create custom permissions",
			method:     http.MethodPost,
			path:       accountPath("/custom-permissions"),
			body:       map[string]any{"CustomPermissionsName": "cp1"},
			wantStatus: http.StatusOK,
			wantKey:    "Arn",
		},
		{
			name:       "describe custom permissions",
			method:     http.MethodGet,
			path:       accountPath("/custom-permissions/cp1"),
			wantStatus: http.StatusOK,
			wantKey:    "CustomPermissions",
		},
		{
			name:       "update custom permissions",
			method:     http.MethodPut,
			path:       accountPath("/custom-permissions/cp1"),
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantKey:    "Arn",
		},
		{
			name:       "list custom permissions",
			method:     http.MethodGet,
			path:       accountPath("/custom-permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "CustomPermissionsList",
		},
		{
			name:       "delete custom permissions",
			method:     http.MethodDelete,
			path:       accountPath("/custom-permissions/cp1"),
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- Role Membership tests ---- //nolint:godot // existing issue.
func TestQuickSight_RoleMemberships(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create role membership",
			method:     http.MethodPost,
			path:       nsPath("/roles/ADMIN/members/user1"),
			wantStatus: http.StatusOK,
		},
		{
			name:       "list role memberships",
			method:     http.MethodGet,
			path:       nsPath("/roles/ADMIN/members"),
			wantStatus: http.StatusOK,
			wantKey:    "MembersList",
		},
		{
			name:       "describe role custom permission",
			method:     http.MethodGet,
			path:       nsPath("/roles/ADMIN/custom-permission"),
			wantStatus: http.StatusOK,
			wantKey:    "CustomPermissionsName",
		},
		{
			name:       "update role custom permission",
			method:     http.MethodPut,
			path:       nsPath("/roles/ADMIN/custom-permission"),
			body:       map[string]any{"CustomPermissionsName": "cp1"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete role custom permission",
			method:     http.MethodDelete,
			path:       nsPath("/roles/ADMIN/custom-permission"),
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete role membership",
			method:     http.MethodDelete,
			path:       nsPath("/roles/ADMIN/members/user1"),
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- DataSet extras tests ---- //nolint:godot // existing issue.
func TestQuickSight_DataSetExtras(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Need a dataset to exist first
	rec := doRequest(t, h, http.MethodPost, accountPath("/data-sets"), map[string]any{
		"DataSetId": "ds1", "Name": "Dataset1", "ImportMode": "SPICE",
	})
	require.True(t, rec.Code == http.StatusOK || rec.Code == http.StatusCreated, "create dataset: %d", rec.Code)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "describe dataset permissions",
			method:     http.MethodGet,
			path:       accountPath("/data-sets/ds1/permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "DataSetId",
		},
		{
			name:       "update dataset permissions",
			method:     http.MethodPost,
			path:       accountPath("/data-sets/ds1/permissions"),
			body:       map[string]any{"GrantPermissions": []any{}, "RevokePermissions": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "DataSetId",
		},
		{
			name:       "put dataset refresh properties",
			method:     http.MethodPut,
			path:       accountPath("/data-sets/ds1/refresh-properties"),
			body:       map[string]any{"DataSetRefreshProperties": map[string]any{}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe dataset refresh properties",
			method:     http.MethodGet,
			path:       accountPath("/data-sets/ds1/refresh-properties"),
			wantStatus: http.StatusOK,
			wantKey:    "DataSetRefreshProperties",
		},
		{
			name:       "create refresh schedule",
			method:     http.MethodPost,
			path:       accountPath("/data-sets/ds1/refresh-schedules"),
			body:       map[string]any{"Schedule": map[string]any{"ScheduleId": "sched1"}},
			wantStatus: http.StatusOK,
			wantKey:    "ScheduleId",
		},
		{
			name:       "describe refresh schedule",
			method:     http.MethodGet,
			path:       accountPath("/data-sets/ds1/refresh-schedules/sched1"),
			wantStatus: http.StatusOK,
			wantKey:    "RefreshSchedule",
		},
		{
			name:       "list refresh schedules",
			method:     http.MethodGet,
			path:       accountPath("/data-sets/ds1/refresh-schedules"),
			wantStatus: http.StatusOK,
			wantKey:    "RefreshSchedules",
		},
		{
			name:       "update refresh schedule",
			method:     http.MethodPut,
			path:       accountPath("/data-sets/ds1/refresh-schedules"),
			body:       map[string]any{"Schedule": map[string]any{"ScheduleId": "sched1"}},
			wantStatus: http.StatusOK,
			wantKey:    "ScheduleId",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- DataSource extras tests ---- //nolint:godot // existing issue.
func TestQuickSight_DataSourceExtras(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Need a data source
	rec := doRequest(t, h, http.MethodPost, accountPath("/data-sources"), map[string]any{
		"DataSourceId": "src1", "Name": "Source1", "Type": "S3",
	})
	require.True(t, rec.Code == http.StatusOK || rec.Code == http.StatusCreated, "create data source: %d", rec.Code)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "describe data source permissions",
			method:     http.MethodGet,
			path:       accountPath("/data-sources/src1/permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "DataSourceId",
		},
		{
			name:       "update data source permissions",
			method:     http.MethodPost,
			path:       accountPath("/data-sources/src1/permissions"),
			body:       map[string]any{"GrantPermissions": []any{}, "RevokePermissions": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "DataSourceId",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- Dashboard extras tests ---- //nolint:godot // existing issue.
func TestQuickSight_DashboardExtras(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Need a dashboard
	rec := doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash1"), map[string]any{
		"Name": "Dashboard1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "describe dashboard definition",
			method:     http.MethodGet,
			path:       accountPath("/dashboards/dash1/definition"),
			wantStatus: http.StatusOK,
			wantKey:    "DashboardId",
		},
		{
			name:       "describe dashboard permissions",
			method:     http.MethodGet,
			path:       accountPath("/dashboards/dash1/permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "DashboardId",
		},
		{
			name:       "update dashboard permissions",
			method:     http.MethodPut,
			path:       accountPath("/dashboards/dash1/permissions"),
			body:       map[string]any{"GrantPermissions": []any{}, "RevokePermissions": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "DashboardId",
		},
		{
			name:       "update dashboard published version",
			method:     http.MethodPut,
			path:       accountPath("/dashboards/dash1/versions/1"),
			wantStatus: http.StatusOK,
			wantKey:    "DashboardId",
		},
		{
			name:       "update dashboard links",
			method:     http.MethodPut,
			path:       accountPath("/dashboards/dash1/linked-entities"),
			body:       map[string]any{"LinkEntities": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "DashboardId",
		},
		{
			name:       "start dashboard snapshot job",
			method:     http.MethodPost,
			path:       accountPath("/dashboards/dash1/snapshot-jobs"),
			body:       map[string]any{"SnapshotJobId": "snap1"},
			wantStatus: http.StatusOK,
			wantKey:    "SnapshotJobId",
		},
		{
			name:       "describe dashboard snapshot job",
			method:     http.MethodGet,
			path:       accountPath("/dashboards/dash1/snapshot-jobs/snap1"),
			wantStatus: http.StatusOK,
			wantKey:    "SnapshotJob",
		},
		{
			name:       "describe dashboard snapshot job result",
			method:     http.MethodGet,
			path:       accountPath("/dashboards/dash1/snapshot-jobs/snap1/result"),
			wantStatus: http.StatusOK,
			wantKey:    "JobResult",
		},
		{
			name:       "get dashboard embed url",
			method:     http.MethodGet,
			path:       accountPath("/dashboards/dash1/embed-url"),
			wantStatus: http.StatusOK,
			wantKey:    "EmbedUrl",
		},
		{
			name:       "start dashboard snapshot job schedule",
			method:     http.MethodPost,
			path:       accountPath("/dashboards/dash1/schedules/sched1"),
			wantStatus: http.StatusOK,
			wantKey:    "DashboardId",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- Brand tests ---- //nolint:godot // existing issue.
func TestQuickSight_Brands(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create brand",
			method:     http.MethodPost,
			path:       accountPath("/brands/brand1"),
			body:       map[string]any{"BrandName": "MyBrand"},
			wantStatus: http.StatusOK,
			wantKey:    "BrandId",
		},
		{
			name:       "describe brand",
			method:     http.MethodGet,
			path:       accountPath("/brands/brand1"),
			wantStatus: http.StatusOK,
			wantKey:    "Brand",
		},
		{
			name:       "update brand",
			method:     http.MethodPut,
			path:       accountPath("/brands/brand1"),
			body:       map[string]any{"BrandName": "RenamedBrand"},
			wantStatus: http.StatusOK,
			wantKey:    "BrandId",
		},
		{
			name:       "list brands",
			method:     http.MethodGet,
			path:       accountPath("/brands"),
			wantStatus: http.StatusOK,
			wantKey:    "Brands",
		},
		{
			name:       "update brand assignment",
			method:     http.MethodPut,
			path:       accountPath("/brandassignments"),
			body:       map[string]any{"BrandArn": "arn:aws:quicksight:us-east-1:000000000000:brand/brand1"},
			wantStatus: http.StatusOK,
			wantKey:    "BrandAssignment",
		},
		{
			name:       "describe brand assignment",
			method:     http.MethodGet,
			path:       accountPath("/brandassignments"),
			wantStatus: http.StatusOK,
			wantKey:    "BrandAssignment",
		},
		{
			name:       "describe brand published version",
			method:     http.MethodGet,
			path:       accountPath("/brands/brand1/publishedversion"),
			wantStatus: http.StatusOK,
			wantKey:    "BrandDefinition",
		},
		{
			name:       "delete brand assignment",
			method:     http.MethodDelete,
			path:       accountPath("/brandassignments"),
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete brand",
			method:     http.MethodDelete,
			path:       accountPath("/brands/brand1"),
			wantStatus: http.StatusOK,
			wantKey:    "BrandId",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- OAuth Client App tests ---- //nolint:godot // existing issue.
func TestQuickSight_OAuthClientApps(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create oauth app",
			method:     http.MethodPost,
			path:       accountPath("/oauth-client-applications"),
			body:       map[string]any{"ApplicationId": "app1", "ApplicationName": "App1"},
			wantStatus: http.StatusOK,
			wantKey:    "OAuthClientApplication",
		},
		{
			name:       "describe oauth app",
			method:     http.MethodGet,
			path:       accountPath("/oauth-client-applications/app1"),
			wantStatus: http.StatusOK,
			wantKey:    "OAuthClientApplication",
		},
		{
			name:       "update oauth app",
			method:     http.MethodPut,
			path:       accountPath("/oauth-client-applications/app1"),
			body:       map[string]any{"ApplicationName": "App1Renamed"},
			wantStatus: http.StatusOK,
			wantKey:    "OAuthClientApplication",
		},
		{
			name:       "list oauth apps",
			method:     http.MethodGet,
			path:       accountPath("/oauth-client-applications"),
			wantStatus: http.StatusOK,
			wantKey:    "OAuthClientApplications",
		},
		{
			name:       "delete oauth app",
			method:     http.MethodDelete,
			path:       accountPath("/oauth-client-applications/app1"),
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- Account-level settings tests ---- //nolint:godot // existing issue.
func TestQuickSight_AccountSettings(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create account customization",
			method:     http.MethodPost,
			path:       accountPath("/customizations"),
			body:       map[string]any{"AccountCustomization": map[string]any{}},
			wantStatus: http.StatusOK,
			wantKey:    "AccountCustomization",
		},
		{
			name:       "describe account customization",
			method:     http.MethodGet,
			path:       accountPath("/customizations"),
			wantStatus: http.StatusOK,
			wantKey:    "AccountCustomization",
		},
		{
			name:       "update account customization",
			method:     http.MethodPut,
			path:       accountPath("/customizations"),
			body:       map[string]any{"DefaultTheme": "arn:aws:quicksight::aws:theme/MIDNIGHT"},
			wantStatus: http.StatusOK,
			wantKey:    "AccountCustomization",
		},
		{
			name:       "describe account settings",
			method:     http.MethodGet,
			path:       accountPath("/settings"),
			wantStatus: http.StatusOK,
			wantKey:    "AccountSettings",
		},
		{
			name:       "update account settings",
			method:     http.MethodPut,
			path:       accountPath("/settings"),
			body:       map[string]any{"NotificationEmail": "test@example.com"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe ip restriction",
			method:     http.MethodGet,
			path:       accountPath("/ip-restriction"),
			wantStatus: http.StatusOK,
			wantKey:    "IpRestrictionRuleMap",
		},
		{
			name:       "update ip restriction",
			method:     http.MethodPost,
			path:       accountPath("/ip-restriction"),
			body:       map[string]any{"Enabled": true},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe key registration",
			method:     http.MethodGet,
			path:       accountPath("/key-registration"),
			wantStatus: http.StatusOK,
			wantKey:    "RegisteredCustomerManagedKeys",
		},
		{
			name:       "update key registration",
			method:     http.MethodPost,
			path:       accountPath("/key-registration"),
			body:       map[string]any{"KeyRegistration": []any{}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update public sharing settings",
			method:     http.MethodPut,
			path:       accountPath("/public-sharing-settings"),
			body:       map[string]any{"PublicSharingEnabled": true},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe q personalization",
			method:     http.MethodGet,
			path:       accountPath("/q-personalization-configuration"),
			wantStatus: http.StatusOK,
			wantKey:    "PersonalizationMode",
		},
		{
			name:       "update q personalization",
			method:     http.MethodPut,
			path:       accountPath("/q-personalization-configuration"),
			body:       map[string]any{"PersonalizationMode": "DISABLED"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe q search config",
			method:     http.MethodGet,
			path:       accountPath("/quicksight-q-search-configuration"),
			wantStatus: http.StatusOK,
			wantKey:    "QSearchStatus",
		},
		{
			name:       "update q search config",
			method:     http.MethodPut,
			path:       accountPath("/quicksight-q-search-configuration"),
			body:       map[string]any{"QSearchStatus": "DISABLED"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update spice capacity",
			method:     http.MethodPost,
			path:       accountPath("/spice-capacity-configuration"),
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe default qbusiness",
			method:     http.MethodGet,
			path:       accountPath("/default-qbusiness-application"),
			wantStatus: http.StatusOK,
			wantKey:    "DefaultQBusinessApplication",
		},
		{
			name:       "update default qbusiness",
			method:     http.MethodPut,
			path:       accountPath("/default-qbusiness-application"),
			body:       map[string]any{"ApplicationId": "qbiz1"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update app token grant",
			method:     http.MethodPut,
			path:       accountPath("/application-with-token-exchange-grant"),
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "get identity context",
			method:     http.MethodPost,
			path:       accountPath("/identity-context"),
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantKey:    "IdentityContextDomains",
		},
		{
			name:       "delete account customization",
			method:     http.MethodDelete,
			path:       accountPath("/customizations"),
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status for "+tc.name)
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- Account Subscription tests ---- //nolint:godot // existing issue.
func TestQuickSight_AccountSubscription(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create account subscription",
			method:     http.MethodPost,
			path:       accountSingularPath(""),
			body:       map[string]any{"Edition": "ENTERPRISE"},
			wantStatus: http.StatusOK,
			wantKey:    "SignupResponse",
		},
		{
			name:       "describe account subscription",
			method:     http.MethodGet,
			path:       accountSingularPath(""),
			wantStatus: http.StatusOK,
			wantKey:    "AccountInfo",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- Embed URL tests ---- //nolint:godot // existing issue.
func TestQuickSight_EmbedURLs(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "generate embed url for anonymous user",
			method:     http.MethodPost,
			path:       accountPath("/embed-url/anonymous-user"),
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantKey:    "EmbedUrl",
		},
		{
			name:       "generate embed url for registered user",
			method:     http.MethodPost,
			path:       accountPath("/embed-url/registered-user"),
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantKey:    "EmbedUrl",
		},
		{
			name:       "generate embed url for registered user with identity",
			method:     http.MethodPost,
			path:       accountPath("/embed-url/registered-user-with-identity"),
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantKey:    "EmbedUrl",
		},
		{
			name:       "get session embed url",
			method:     http.MethodGet,
			path:       accountPath("/session-embed-url"),
			wantStatus: http.StatusOK,
			wantKey:    "EmbedUrl",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- Search tests ---- //nolint:godot // existing issue.
func TestQuickSight_Search(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "search analyses",
			method:     http.MethodPost,
			path:       accountPath("/search/analyses"),
			body:       map[string]any{"Filters": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "AnalysisSummaryList",
		},
		{
			name:       "search dashboards",
			method:     http.MethodPost,
			path:       accountPath("/search/dashboards"),
			body:       map[string]any{"Filters": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "DashboardSummaryList",
		},
		{
			name:       "search datasets",
			method:     http.MethodPost,
			path:       accountPath("/search/data-sets"),
			body:       map[string]any{"Filters": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "DataSetSummaries",
		},
		{
			name:       "search data sources",
			method:     http.MethodPost,
			path:       accountPath("/search/data-sources"),
			body:       map[string]any{"Filters": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "DataSources",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		}()
	}
}

// ---- User custom permission tests ---- //nolint:godot // existing issue.
func TestQuickSight_UserCustomPermission(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "update user custom permission",
			method:     http.MethodPut,
			path:       nsPath("/users/user1/custom-permission"),
			body:       map[string]any{"CustomPermissionsName": "cp1"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete user custom permission",
			method:     http.MethodDelete,
			path:       nsPath("/users/user1/custom-permission"),
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
		}()
	}
}
