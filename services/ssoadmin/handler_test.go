package ssoadmin_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

func newTestHandler() *ssoadmin.Handler {
	backend := ssoadmin.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return ssoadmin.NewHandler(backend)
}

func doRequest(
	t *testing.T,
	h *ssoadmin.Handler,
	op string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "SWBExternalService."+op)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

func createInstance(t *testing.T, h *ssoadmin.Handler, name string) string {
	t.Helper()
	rec := doRequest(t, h, "CreateInstance", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	arn, ok := resp["InstanceArn"].(string)
	require.True(t, ok, "expected InstanceArn in response")
	require.NotEmpty(t, arn)

	return arn
}

func createPermissionSet(t *testing.T, h *ssoadmin.Handler, instanceArn, name string) string {
	t.Helper()
	rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
		"InstanceArn": instanceArn,
		"Name":        name,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	ps, ok := resp["PermissionSet"].(map[string]any)
	require.True(t, ok)
	arn, ok := ps["PermissionSetArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

func TestHandlerMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "Name", want: "SsoAdmin"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

func TestGetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()

	tests := []struct {
		name string
		op   string
	}{
		{name: "ListInstances", op: "ListInstances"},
		{name: "CreateInstance", op: "CreateInstance"},
		{name: "DescribeInstance", op: "DescribeInstance"},
		{name: "DeleteInstance", op: "DeleteInstance"},
		{name: "CreatePermissionSet", op: "CreatePermissionSet"},
		{name: "DescribePermissionSet", op: "DescribePermissionSet"},
		{name: "ListPermissionSets", op: "ListPermissionSets"},
		{name: "DeletePermissionSet", op: "DeletePermissionSet"},
		{name: "UpdatePermissionSet", op: "UpdatePermissionSet"},
		{name: "CreateAccountAssignment", op: "CreateAccountAssignment"},
		{name: "DeleteAccountAssignment", op: "DeleteAccountAssignment"},
		{name: "ListAccountAssignments", op: "ListAccountAssignments"},
		{name: "TagResource", op: "TagResource"},
		{name: "UntagResource", op: "UntagResource"},
		{name: "ListTagsForResource", op: "ListTagsForResource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, ops, tt.op)
		})
	}
}

func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matches ssoadmin prefix",
			target:    "SWBExternalService.ListInstances",
			wantMatch: true,
		},
		{
			name:      "matches ssoadmin prefix other op",
			target:    "SWBExternalService.CreatePermissionSet",
			wantMatch: true,
		},
		{
			name:      "does not match different prefix",
			target:    "AWSIdentityStore.ListUsers",
			wantMatch: false,
		},
		{
			name:      "does not match empty",
			target:    "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestInstanceCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createName string
		wantName   string
		wantStatus int
	}{
		{
			name:       "create and describe instance",
			createName: "test-instance",
			wantStatus: http.StatusOK,
			wantName:   "test-instance",
		},
		{
			name:       "create instance empty name",
			createName: "",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			rec := doRequest(t, h, "CreateInstance", map[string]any{"Name": tt.createName})
			require.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			instanceArn, ok := resp["InstanceArn"].(string)
			require.True(t, ok)
			require.NotEmpty(t, instanceArn)

			if tt.wantName != "" {
				descRec := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": instanceArn})
				require.Equal(t, http.StatusOK, descRec.Code)
				descResp := parseResponse(t, descRec)
				assert.Equal(t, tt.wantName, descResp["Name"])
				assert.Equal(t, instanceArn, descResp["InstanceArn"])
				assert.Equal(t, "ACTIVE", descResp["Status"])
			}
		})
	}
}

func TestListInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createNames  []string
		wantMinCount int
	}{
		{
			name:         "empty list",
			createNames:  nil,
			wantMinCount: 0,
		},
		{
			name:         "lists all instances",
			createNames:  []string{"inst-a", "inst-b"},
			wantMinCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			for _, n := range tt.createNames {
				createInstance(t, h, n)
			}

			rec := doRequest(t, h, "ListInstances", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			instances, ok := resp["Instances"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(instances), tt.wantMinCount)
		})
	}
}

func TestDeleteInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		instanceArn string
		wantStatus  int
	}{
		{
			name:        "delete existing instance",
			instanceArn: "",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "delete non-existing instance",
			instanceArn: "arn:aws:sso:::instance/ssoins-nonexistent",
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			instanceArn := tt.instanceArn
			if instanceArn == "" {
				instanceArn = createInstance(t, h, "delete-me")
			}

			rec := doRequest(t, h, "DeleteInstance", map[string]any{"InstanceArn": instanceArn})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestPermissionSetCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		psName          string
		description     string
		sessionDuration string
		wantStatus      int
	}{
		{
			name:            "create and describe permission set",
			psName:          "ReadOnly",
			description:     "Read only access",
			sessionDuration: "PT2H",
			wantStatus:      http.StatusOK,
		},
		{
			name:       "create permission set default session duration",
			psName:     "AdminAccess",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "test-inst")

			rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
				"InstanceArn":     instanceArn,
				"Name":            tt.psName,
				"Description":     tt.description,
				"SessionDuration": tt.sessionDuration,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			ps, ok := resp["PermissionSet"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.psName, ps["Name"])
			assert.NotEmpty(t, ps["PermissionSetArn"])

			psArn := ps["PermissionSetArn"].(string)

			descRec := doRequest(t, h, "DescribePermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, descRec.Code)
			descResp := parseResponse(t, descRec)
			descPS, ok := descResp["PermissionSet"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.psName, descPS["Name"])
		})
	}
}

func TestPermissionSetConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "duplicate permission set name",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			createPermissionSet(t, h, instanceArn, "DuplicateName")

			rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
				"InstanceArn": instanceArn,
				"Name":        "DuplicateName",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestListPermissionSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		psNames      []string
		wantMinCount int
	}{
		{
			name:         "empty list",
			psNames:      nil,
			wantMinCount: 0,
		},
		{
			name:         "lists created permission sets",
			psNames:      []string{"PS1", "PS2", "PS3"},
			wantMinCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")

			for _, n := range tt.psNames {
				createPermissionSet(t, h, instanceArn, n)
			}

			rec := doRequest(t, h, "ListPermissionSets", map[string]any{"InstanceArn": instanceArn})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			psList, ok := resp["PermissionSets"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(psList), tt.wantMinCount)
		})
	}
}

func TestDeletePermissionSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useInvalid bool
		wantStatus int
	}{
		{
			name:       "delete existing permission set",
			useInvalid: false,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete non-existing permission set",
			useInvalid: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")

			psArn := "arn:aws:sso:::permissionSet/ssoins-bad/badid"
			if !tt.useInvalid {
				psArn = createPermissionSet(t, h, instanceArn, "ToDelete")
			}

			rec := doRequest(t, h, "DeletePermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestUpdatePermissionSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		newDescription  string
		newSession      string
		wantDescription string
		wantSession     string
	}{
		{
			name:            "update description and session duration",
			newDescription:  "Updated description",
			newSession:      "PT4H",
			wantDescription: "Updated description",
			wantSession:     "PT4H",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "UpdateMe")

			rec := doRequest(t, h, "UpdatePermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"Description":      tt.newDescription,
				"SessionDuration":  tt.newSession,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			descRec := doRequest(t, h, "DescribePermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, descRec.Code)
			descResp := parseResponse(t, descRec)
			ps, ok := descResp["PermissionSet"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantDescription, ps["Description"])
			assert.Equal(t, tt.wantSession, ps["SessionDuration"])
		})
	}
}

func TestAccountAssignmentCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		accountID     string
		principalID   string
		principalType string
		wantStatus    int
	}{
		{
			name:          "create and list assignment",
			accountID:     "123456789012",
			principalID:   "user-abc",
			principalType: "USER",
			wantStatus:    http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "PS")

			rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetId":         tt.accountID,
				"TargetType":       "AWS_ACCOUNT",
				"PrincipalId":      tt.principalID,
				"PrincipalType":    tt.principalType,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			creationStatus, ok := resp["AccountAssignmentCreationStatus"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "SUCCEEDED", creationStatus["Status"])
			requestID := creationStatus["RequestId"].(string)
			assert.NotEmpty(t, requestID)

			descRec := doRequest(t, h, "DescribeAccountAssignmentCreationStatus", map[string]any{
				"InstanceArn":                        instanceArn,
				"AccountAssignmentCreationRequestId": requestID,
			})
			require.Equal(t, http.StatusOK, descRec.Code)
			descResp := parseResponse(t, descRec)
			status, ok := descResp["AccountAssignmentCreationStatus"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "SUCCEEDED", status["Status"])

			listRec := doRequest(t, h, "ListAccountAssignments", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)
			listResp := parseResponse(t, listRec)
			assignments, ok := listResp["AccountAssignments"].([]any)
			require.True(t, ok)
			assert.Len(t, assignments, 1)

			delRec := doRequest(t, h, "DeleteAccountAssignment", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetId":         tt.accountID,
				"TargetType":       "AWS_ACCOUNT",
				"PrincipalId":      tt.principalID,
				"PrincipalType":    tt.principalType,
			})
			require.Equal(t, http.StatusOK, delRec.Code)
			delResp := parseResponse(t, delRec)
			delStatus, ok := delResp["AccountAssignmentDeletionStatus"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "SUCCEEDED", delStatus["Status"])
		})
	}
}

func TestManagedPolicyCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		managedPolicyArn string
		wantPolicyName   string
	}{
		{
			name:             "attach, list, and detach managed policy",
			managedPolicyArn: "arn:aws:iam::aws:policy/ReadOnlyAccess",
			wantPolicyName:   "ReadOnlyAccess",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "PS")

			attachRec := doRequest(t, h, "AttachManagedPolicyToPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"ManagedPolicyArn": tt.managedPolicyArn,
			})
			require.Equal(t, http.StatusOK, attachRec.Code)

			listRec := doRequest(t, h, "ListManagedPoliciesInPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)
			listResp := parseResponse(t, listRec)
			policies, ok := listResp["AttachedManagedPolicies"].([]any)
			require.True(t, ok)
			require.Len(t, policies, 1)
			policy := policies[0].(map[string]any)
			assert.Equal(t, tt.wantPolicyName, policy["Name"])
			assert.Equal(t, tt.managedPolicyArn, policy["Arn"])

			detachRec := doRequest(t, h, "DetachManagedPolicyFromPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"ManagedPolicyArn": tt.managedPolicyArn,
			})
			require.Equal(t, http.StatusOK, detachRec.Code)

			listRec2 := doRequest(t, h, "ListManagedPoliciesInPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, listRec2.Code)
			listResp2 := parseResponse(t, listRec2)
			policies2, ok := listResp2["AttachedManagedPolicies"].([]any)
			require.True(t, ok)
			assert.Empty(t, policies2)
		})
	}
}

func TestInlinePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		inlinePolicy string
		wantPolicy   string
	}{
		{
			name:         "put, get, and delete inline policy",
			inlinePolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			wantPolicy:   `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "PS")

			putRec := doRequest(t, h, "PutInlinePolicyToPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"InlinePolicy":     tt.inlinePolicy,
			})
			require.Equal(t, http.StatusOK, putRec.Code)

			getRec := doRequest(t, h, "GetInlinePolicyForPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, getRec.Code)
			getResp := parseResponse(t, getRec)
			assert.Equal(t, tt.wantPolicy, getResp["InlinePolicy"])

			delRec := doRequest(t, h, "DeleteInlinePolicyFromPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, delRec.Code)

			getRec2 := doRequest(t, h, "GetInlinePolicyForPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, getRec2.Code)
			getResp2 := parseResponse(t, getRec2)
			assert.Empty(t, getResp2["InlinePolicy"])
		})
	}
}

func TestProvisionPermissionSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		targetType string
		wantStatus string
	}{
		{
			name:       "provision permission set succeeds",
			targetType: "ALL_PROVISIONED_ACCOUNTS",
			wantStatus: "SUCCEEDED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "PS")

			rec := doRequest(t, h, "ProvisionPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetType":       tt.targetType,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			status, ok := resp["PermissionSetProvisioningStatus"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantStatus, status["Status"])
			requestID := status["RequestId"].(string)
			assert.NotEmpty(t, requestID)

			descRec := doRequest(t, h, "DescribePermissionSetProvisioningStatus", map[string]any{
				"InstanceArn":                     instanceArn,
				"ProvisionPermissionSetRequestId": requestID,
			})
			require.Equal(t, http.StatusOK, descRec.Code)
			descResp := parseResponse(t, descRec)
			descStatus, ok := descResp["PermissionSetProvisioningStatus"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantStatus, descStatus["Status"])
		})
	}
}

func TestTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags   map[string]string
		wantAfterU map[string]string
		name       string
		tags       []map[string]string
		untagKeys  []string
	}{
		{
			name: "tag, list, and untag resource",
			tags: []map[string]string{
				{"Key": "env", "Value": "prod"},
				{"Key": "team", "Value": "platform"},
			},
			untagKeys:  []string{"env"},
			wantTags:   map[string]string{"env": "prod", "team": "platform"},
			wantAfterU: map[string]string{"team": "platform"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "PS")

			tagData := make([]any, 0, len(tt.tags))
			for _, tag := range tt.tags {
				tagData = append(tagData, tag)
			}

			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": psArn,
				"Tags":        tagData,
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": psArn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)
			listResp := parseResponse(t, listRec)
			tagList, ok := listResp["Tags"].([]any)
			require.True(t, ok)
			gotTags := make(map[string]string, len(tagList))
			for _, item := range tagList {
				t2 := item.(map[string]any)
				gotTags[t2["Key"].(string)] = t2["Value"].(string)
			}
			assert.Equal(t, tt.wantTags, gotTags)

			untagRec := doRequest(t, h, "UntagResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": psArn,
				"TagKeys":     tt.untagKeys,
			})
			require.Equal(t, http.StatusOK, untagRec.Code)

			listRec2 := doRequest(t, h, "ListTagsForResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": psArn,
			})
			require.Equal(t, http.StatusOK, listRec2.Code)
			listResp2 := parseResponse(t, listRec2)
			tagList2, ok := listResp2["Tags"].([]any)
			require.True(t, ok)
			gotTags2 := make(map[string]string, len(tagList2))
			for _, item := range tagList2 {
				t2 := item.(map[string]any)
				gotTags2[t2["Key"].(string)] = t2["Value"].(string)
			}
			assert.Equal(t, tt.wantAfterU, gotTags2)
		})
	}
}

func TestUnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "unknown operation returns bad request",
			op:         "UnknownOp",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, tt.op, map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
			resp := parseResponse(t, rec)
			assert.Equal(t, "UnknownOperationException", resp["__type"])
		})
	}
}

func TestErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "describe non-existent instance",
			op:         "DescribeInstance",
			body:       map[string]any{"InstanceArn": "arn:aws:sso:::instance/ssoins-bad"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "delete non-existent permission set",
			op:   "DeletePermissionSet",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-bad",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-bad/badid",
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "create permission set missing instance arn",
			op:         "CreatePermissionSet",
			body:       map[string]any{"Name": "PS"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create permission set missing name",
			op:         "CreatePermissionSet",
			body:       map[string]any{"InstanceArn": "arn:aws:sso:::instance/ssoins-bad"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "describe instance missing arn",
			op:         "DescribeInstance",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestMissingTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		target     string
		wantStatus int
	}{
		{
			name:       "missing X-Amz-Target returns 400",
			target:     "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "wrong prefix returns 400",
			target:     "OtherService.ListInstances",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("{}")))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			rec := httptest.NewRecorder()

			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, "UnrecognizedClientException", resp["__type"])
		})
	}
}
