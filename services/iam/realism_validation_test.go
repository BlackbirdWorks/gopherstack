package iam_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const realismTrustDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

func TestCreateEntity_NameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params   map[string]string
		name     string
		action   string
		wantCode string
		wantHTTP int
	}{
		{
			name: "role bad chars", action: "CreateRole",
			params:   map[string]string{"RoleName": "bad name!", "AssumeRolePolicyDocument": realismTrustDoc},
			wantHTTP: http.StatusBadRequest, wantCode: "ValidationError",
		},
		{
			name:   "role too long",
			action: "CreateRole",
			params: map[string]string{
				"RoleName":                 strings.Repeat("a", 65),
				"AssumeRolePolicyDocument": realismTrustDoc,
			},
			wantHTTP: http.StatusBadRequest,
			wantCode: "ValidationError",
		},
		{
			name: "user bad chars", action: "CreateUser", params: map[string]string{"UserName": "a b"},
			wantHTTP: http.StatusBadRequest, wantCode: "ValidationError",
		},
		{
			name: "group bad chars", action: "CreateGroup", params: map[string]string{"GroupName": "g/1"},
			wantHTTP: http.StatusBadRequest, wantCode: "ValidationError",
		},
		{
			name: "user ok", action: "CreateUser", params: map[string]string{"UserName": "a.b+c=d,e@f-g_h"},
			wantHTTP: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := callIAM(t, h, tt.action, tt.params)
			assert.Equal(t, tt.wantHTTP, rec.Code, rec.Body.String())

			if tt.wantCode != "" {
				assert.Contains(t, rec.Body.String(), "<Code>"+tt.wantCode+"</Code>")
			}
		})
	}
}

func TestCreateRole_InvalidSessionDurationLeavesNoRole(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	rec := callIAM(t, h, "CreateRole", map[string]string{
		"RoleName": "r1", "AssumeRolePolicyDocument": realismTrustDoc, "MaxSessionDuration": "100",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	_, err := b.GetRole("r1")
	require.Error(t, err)
}

func TestGetRole_DefaultMaxSessionDuration(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := callIAM(t, h, "CreateRole", map[string]string{"RoleName": "r1", "AssumeRolePolicyDocument": realismTrustDoc})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = callIAM(t, h, "GetRole", map[string]string{"RoleName": "r1"})
	assert.Contains(t, rec.Body.String(), "<MaxSessionDuration>3600</MaxSessionDuration>")
}

func TestAttachPolicy_MissingPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		policyArn string
		wantHTTP  int
	}{
		{name: "customer missing", policyArn: "arn:aws:iam::000000000000:policy/nope", wantHTTP: http.StatusNotFound},
		{name: "aws managed", policyArn: "arn:aws:iam::aws:policy/AdministratorAccess", wantHTTP: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			_, err := b.CreateUser("u1", "/", "")
			require.NoError(t, err)

			rec := callIAM(t, h, "AttachUserPolicy", map[string]string{"UserName": "u1", "PolicyArn": tt.policyArn})
			assert.Equal(t, tt.wantHTTP, rec.Code, rec.Body.String())
		})
	}
}

func TestTagUser_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		count    int
		wantHTTP int
	}{
		{name: "at limit", count: 50, wantHTTP: http.StatusOK},
		{name: "over limit", count: 51, wantHTTP: http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			_, err := b.CreateUser("u1", "/", "")
			require.NoError(t, err)

			params := map[string]string{"UserName": "u1"}
			for i := 1; i <= tt.count; i++ {
				params[fmt.Sprintf("Tags.member.%d.Key", i)] = fmt.Sprintf("k%d", i)
				params[fmt.Sprintf("Tags.member.%d.Value", i)] = "v"
			}

			rec := callIAM(t, h, "TagUser", params)
			assert.Equal(t, tt.wantHTTP, rec.Code, rec.Body.String())
		})
	}
}

func TestList_InvalidMarker(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := callIAM(t, h, "ListUsers", map[string]string{"Marker": "garbage"})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>ValidationError</Code>")
}

func TestErrorMessage_OmitsCodePrefix(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := callIAM(t, h, "GetUser", map[string]string{"UserName": "nope"})

	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.NotContains(t, rec.Body.String(), "<Message>NoSuchEntity")
}

const allowAllPolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
