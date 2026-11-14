package iam_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	rcpDenyAllOutsideOrg = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":["%s"],` +
		`"Resource":"*","Condition":{"StringNotEqualsIfExists":{"aws:PrincipalOrgID":"o-test"}}}]}`
)

const openResourcePolicy = `{"Version":"2012-10-17","Statement":` +
	`[{"Effect":"Allow","Principal":"*","Action":"*","Resource":"*"}]}`

type openResources struct{}

func (openResources) GetResourcePolicy(context.Context, string) (string, error) {
	return openResourcePolicy, nil
}

type exemptKeys map[string]bool

func (e exemptKeys) RCPExempt(_ context.Context, resourceARN string) bool { return e[resourceARN] }

func rcpDoc(actions string) string {
	return strings.Replace(rcpDenyAllOutsideOrg, "%s", actions, 1)
}

func TestEnforcementMiddleware_RCPAcrossServices(t *testing.T) {
	t.Parallel()

	const (
		keyARN    = "arn:aws:kms:us-east-1:" + acctMember + ":key/1234"
		managed   = "arn:aws:kms:us-east-1:" + acctMember + ":key/aws-managed"
		secretARN = "arn:aws:secretsmanager:us-east-1:" + acctMember + ":secret:db-AbCdEf"
		roleARN   = "arn:aws:iam::" + acctMember + ":role/target"
	)

	org := func(actions string) fakeOrg {
		return fakeOrg{
			orgIDs: map[string]string{acctMember: "o-test", acctPeer: "o-test"},
			rcps:   map[string][]iam.SCPLevel{acctMember: levels(rcpAllowAll, rcpDoc(actions))},
		}
	}

	jsonReq := func(target, body string) func() *http.Request {
		return func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			r.Header.Set("X-Amz-Target", target)
			r.Header.Set("Content-Type", "application/x-amz-json-1.1")

			return r
		}
	}

	sqsReq := func(account string) func() *http.Request {
		return jsonReq("AmazonSQS.SendMessage",
			`{"QueueUrl":"http://localhost:4566/`+account+`/q","MessageBody":"x"}`)
	}

	stsReq := func() *http.Request {
		form := url.Values{
			"Action": {"AssumeRole"}, "RoleArn": {roleARN}, "RoleSessionName": {"s"}, "Version": {"2011-06-15"},
		}
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		return r
	}

	user := func(acct string) *awsmeta.Principal {
		return &awsmeta.Principal{
			Kind: awsmeta.PrincipalKindUser, Arn: "arn:aws:iam::" + acct + ":user/bob", AccountID: acct,
		}
	}

	tests := []struct {
		principal  *awsmeta.Principal
		exempt     exemptKeys
		req        func() *http.Request
		name       string
		actions    string
		wantStatus int
	}{
		{
			name: "sqs_outsider_denied", principal: user(acctOutside), actions: "sqs:*",
			req: sqsReq(acctMember), wantStatus: http.StatusBadRequest,
		},
		{
			name: "sqs_same_account_allowed", principal: user(acctMember), actions: "sqs:*",
			req: sqsReq(acctMember), wantStatus: http.StatusOK,
		},
		{
			name: "sqs_in_org_peer_allowed", principal: user(acctPeer), actions: "sqs:*",
			req: sqsReq(acctMember), wantStatus: http.StatusOK,
		},
		{
			name: "sqs_management_owner_exempt", principal: user(acctOutside), actions: "sqs:*",
			req: sqsReq(acctMgmt), wantStatus: http.StatusOK,
		},
		{
			name: "kms_outsider_denied", principal: user(acctOutside), actions: "kms:*",
			wantStatus: http.StatusBadRequest,
			req:        jsonReq("TrentService.DescribeKey", `{"KeyId":"`+keyARN+`"}`),
		},
		{
			name: "kms_same_account_allowed", principal: user(acctMember), actions: "kms:*", wantStatus: http.StatusOK,
			req: jsonReq("TrentService.DescribeKey", `{"KeyId":"`+keyARN+`"}`),
		},
		{
			name: "kms_aws_managed_key_exempt", principal: user(acctOutside), actions: "kms:*",
			wantStatus: http.StatusOK,
			exempt:     exemptKeys{managed: true},
			req:        jsonReq("TrentService.DescribeKey", `{"KeyId":"`+managed+`"}`),
		},
		{
			name: "kms_retire_grant_exempt", principal: user(acctOutside), actions: "kms:*", wantStatus: http.StatusOK,
			req: jsonReq("TrentService.RetireGrant", `{"KeyId":"`+keyARN+`","GrantToken":"t"}`),
		},
		{
			name: "secretsmanager_outsider_denied", principal: user(acctOutside), actions: "secretsmanager:*",
			wantStatus: http.StatusBadRequest,
			req:        jsonReq("secretsmanager.GetSecretValue", `{"SecretId":"`+secretARN+`"}`),
		},
		{
			name: "secretsmanager_same_account_allowed", principal: user(acctMember), actions: "secretsmanager:*",
			wantStatus: http.StatusOK,
			req:        jsonReq("secretsmanager.GetSecretValue", `{"SecretId":"`+secretARN+`"}`),
		},
		{
			name:       "sts_outsider_denied",
			principal:  user(acctOutside),
			actions:    "sts:*",
			req:        stsReq,
			wantStatus: http.StatusForbidden,
		},
		{
			name: "sts_same_account_allowed", principal: user(acctMember), actions: "sts:*",
			req: stsReq, wantStatus: http.StatusOK,
		},
		{
			name: "service_linked_role_exempt",
			principal: &awsmeta.Principal{
				Kind: awsmeta.PrincipalKindAssumedRole, AccountID: acctOutside,
				Arn: "arn:aws:sts::" + acctOutside + ":assumed-role/AWSServiceRoleForX/session",
			},
			actions: "sqs:*", req: sqsReq(acctMember), wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newMockEnforcementBackend()
			backend.policies["bob"] = []string{rcpAllowAll}
			backend.policies["AWSServiceRoleForX"] = []string{rcpAllowAll}

			e := echo.New()
			e.Use(principalSeedingMiddleware(tt.principal))
			e.Use(iam.EnforcementMiddleware(backend, iam.EnforcementConfig{
				SCPs: org(tt.actions), ResourceProviders: []iam.ResourcePolicyProvider{openResources{}},
				RCPExemptions: []iam.RCPExemptionResolver{tt.exempt},
			}))
			e.Any("/*", func(c *echo.Context) error { return c.String(http.StatusOK, "ok") })

			req := tt.req()
			req.Header.Set("Authorization",
				"AWS4-HMAC-SHA256 Credential=ASIAFAKESESSIONKEY/20230101/us-east-1/svc/aws4_request")

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus != http.StatusOK {
				assert.Contains(t, rec.Body.String(), "resource control policy")
			}
		})
	}
}
