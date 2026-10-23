package iam_test

import (
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestIAMPartitionARNs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		region    string
		partition string
	}{
		{"us_east_1", "us-east-1", "aws"},
		{"cn_north_1", "cn-north-1", "aws-cn"},
		{"us_gov_west_1", "us-gov-west-1", "aws-us-gov"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			do := func(action string, params map[string]string) string {
				req := iamRequest(action, params)
				req = req.WithContext(awsmeta.WithRegion(req.Context(), tt.region))
				rec := httptest.NewRecorder()
				require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
				require.Equal(t, 200, rec.Code, rec.Body.String())

				return rec.Body.String()
			}

			p := "arn:" + tt.partition + ":"
			trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"` +
				p + "iam::" + iam.IAMAccountID + `:root"},"Action":"sts:AssumeRole"}]}`

			assert.Contains(t, do("CreateUser", map[string]string{"UserName": "u"}), p+"iam::")
			assert.Contains(t, do("CreateGroup", map[string]string{"GroupName": "g"}), p+"iam::")
			assert.Contains(t, do("CreateInstanceProfile", map[string]string{"InstanceProfileName": "ip"}), p+"iam::")
			assert.Contains(t, do("CreateRole", map[string]string{
				"RoleName": "r", "AssumeRolePolicyDocument": trust,
			}), p+"iam::")

			pol := do("CreatePolicy", map[string]string{
				"PolicyName": "pol",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*",` +
					`"Resource":"` + p + `s3:::b/*"}]}`,
			})
			assert.Contains(t, pol, p+"iam::")

			polArn := p + "iam::" + iam.IAMAccountID + ":policy/pol"
			do("AttachRolePolicy", map[string]string{"RoleName": "r", "PolicyArn": polArn})
			assert.Contains(t, do("ListAttachedRolePolicies", map[string]string{"RoleName": "r"}), polArn)

			managed := p + "iam::aws:policy/ReadOnlyAccess"
			do("AttachRolePolicy", map[string]string{"RoleName": "r", "PolicyArn": managed})
			assert.Contains(t, do("ListAttachedRolePolicies", map[string]string{"RoleName": "r"}), managed)

			role := do("GetRole", map[string]string{"RoleName": "r"})
			assert.Contains(t, role, p+"iam::")
			if tt.partition != "aws" {
				assert.NotContains(t, role, "arn:aws:")
				assert.NotContains(t, role, "arn%3Aaws%3A")
			}
		})
	}
}

func TestIAMBackendStaysCanonical(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	req := iamRequest("CreateUser", map[string]string{"UserName": "u"})
	req = req.WithContext(awsmeta.WithRegion(req.Context(), "cn-north-1"))
	require.NoError(t, h.Handler()(echo.New().NewContext(req, httptest.NewRecorder())))

	u, err := b.GetUser("u")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::"+iam.IAMAccountID+":user/u", u.Arn)
}
