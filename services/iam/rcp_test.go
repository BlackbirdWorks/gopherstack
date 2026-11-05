package iam_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	rcpAllowAll  = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	rcpPerimeter = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:*",` +
		`"Resource":"*","Condition":{"StringNotEqualsIfExists":{"aws:PrincipalOrgID":"o-test"}}}]}`
	rcpDenyEC2 = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"ec2:*",` +
		`"Resource":"*"}]}`
	scpDenyS3 = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:*","Resource":"*"}]}`

	acctMember  = "111111111111"
	acctPeer    = "333333333333"
	acctOutside = "222222222222"
	acctMgmt    = "000000000000"
)

type fakeOrg struct {
	scps   map[string][]iam.SCPLevel
	rcps   map[string][]iam.SCPLevel
	orgIDs map[string]string
}

func (f fakeOrg) SCPsForAccount(a string) ([]iam.SCPLevel, bool) {
	l, ok := f.scps[a]

	return l, ok
}

func (f fakeOrg) RCPsForAccount(a string) ([]iam.SCPLevel, bool) {
	l, ok := f.rcps[a]

	return l, ok
}

func (f fakeOrg) OrgIDForAccount(a string) string { return f.orgIDs[a] }

type fakeBucketOwners map[string]string

func (f fakeBucketOwners) ResourceOwnerAccount(_ context.Context, resourceARN string) (string, bool) {
	rest, ok := strings.CutPrefix(resourceARN, "arn:aws:s3:::")
	if !ok {
		return "", false
	}

	bucket, _, _ := strings.Cut(rest, "/")
	a, found := f.get(bucket)

	return a, found
}

func (f fakeBucketOwners) get(k string) (string, bool) { a, ok := f[k]; return a, ok }

// levels returns one hierarchy level holding every doc.
func levels(docs ...string) []iam.SCPLevel {
	lvl := iam.SCPLevel{TargetID: "t"}

	for i, d := range docs {
		lvl.Policies = append(lvl.Policies, iam.SCPPolicy{
			ARN:      "arn:aws:organizations::" + acctMgmt + ":policy/o-test/resource_control_policy/p-" + string(rune('a'+i)),
			Document: d,
		})
	}

	return []iam.SCPLevel{lvl}
}

func TestEnforcementMiddleware_OrgPolicies(t *testing.T) {
	t.Parallel()

	org := fakeOrg{
		orgIDs: map[string]string{acctMember: "o-test", acctPeer: "o-test", acctMgmt: "o-test"},
		rcps: map[string][]iam.SCPLevel{
			acctMember: levels(rcpAllowAll, rcpPerimeter),
			acctPeer:   levels(rcpAllowAll),
		},
		scps: map[string][]iam.SCPLevel{
			acctMember: levels(rcpAllowAll, scpDenyS3),
			acctPeer:   levels(rcpAllowAll),
		},
	}

	user := func(acct string) *awsmeta.Principal {
		return &awsmeta.Principal{
			Kind: awsmeta.PrincipalKindUser, Arn: "arn:aws:iam::" + acct + ":user/bob", AccountID: acct,
		}
	}

	tests := []struct {
		principal  *awsmeta.Principal
		owners     fakeBucketOwners
		org        fakeOrg
		name       string
		bucket     string
		wantBody   string
		wantStatus int
	}{
		{
			name: "rcp_denies_outside_account", principal: user(acctOutside), owners: fakeBucketOwners{"b": acctPeer},
			org: fakeOrg{
				orgIDs: org.orgIDs,
				rcps:   map[string][]iam.SCPLevel{acctPeer: levels(rcpAllowAll, rcpPerimeter)},
			},
			bucket: "b", wantStatus: http.StatusForbidden,
			wantBody: "with an explicit deny in a resource control policy: arn:aws:organizations::",
		},
		{
			name: "rcp_allows_in_org_principal", principal: user(acctPeer), owners: fakeBucketOwners{"b": acctMember},
			org: fakeOrg{orgIDs: org.orgIDs, rcps: org.rcps}, bucket: "b", wantStatus: http.StatusOK,
		},
		{
			name: "rcp_skips_management_owned_resource", principal: user(acctOutside),
			owners: fakeBucketOwners{"b": acctMgmt}, org: org, bucket: "b", wantStatus: http.StatusOK,
		},
		{
			name: "rcp_skips_service_linked_role",
			principal: &awsmeta.Principal{
				Kind: awsmeta.PrincipalKindAssumedRole, AccountID: acctOutside,
				Arn: "arn:aws:sts::" + acctOutside + ":assumed-role/AWSServiceRoleForX/session",
			},
			owners: fakeBucketOwners{"b": acctMember}, org: org, bucket: "b", wantStatus: http.StatusOK,
		},
		{
			name: "rcp_ignores_unsupported_service", principal: user(acctOutside),
			owners: fakeBucketOwners{"b": acctMember},
			org:    fakeOrg{rcps: map[string][]iam.SCPLevel{acctMember: levels(rcpAllowAll, rcpDenyEC2)}},
			bucket: "b", wantStatus: http.StatusOK,
		},
		{
			name: "rcp_missing_allow_is_implicit_deny", principal: user(acctPeer),
			owners: fakeBucketOwners{"b": acctMember},
			org: fakeOrg{
				orgIDs: org.orgIDs,
				rcps:   map[string][]iam.SCPLevel{acctMember: {{TargetID: "r"}}},
			},
			bucket: "b", wantStatus: http.StatusForbidden,
			wantBody: "because no resource control policy allows the s3:GetObject action",
		},
		{
			name: "unknown_bucket_owner_skips_rcp", principal: user(acctOutside), owners: fakeBucketOwners{},
			org: org, bucket: "b", wantStatus: http.StatusOK,
		},
		{
			name: "scp_restricts_member_root",
			principal: &awsmeta.Principal{
				Kind: awsmeta.PrincipalKindUser, Arn: "arn:aws:iam::" + acctMember + ":root", AccountID: acctMember,
			},
			owners: fakeBucketOwners{}, org: org, bucket: "b", wantStatus: http.StatusForbidden,
			wantBody: "with an explicit deny in a service control policy",
		},
		{
			name: "scp_does_not_restrict_management_root",
			principal: &awsmeta.Principal{
				Kind: awsmeta.PrincipalKindUser, Arn: "arn:aws:iam::" + acctMgmt + ":root", AccountID: acctMgmt,
			},
			owners: fakeBucketOwners{}, org: org, bucket: "b", wantStatus: http.StatusOK,
		},
		{
			name: "member_root_allowed_without_scp_deny",
			principal: &awsmeta.Principal{
				Kind: awsmeta.PrincipalKindUser, Arn: "arn:aws:iam::" + acctPeer + ":root", AccountID: acctPeer,
			},
			owners: fakeBucketOwners{}, org: org, bucket: "b", wantStatus: http.StatusOK,
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
				SCPs: tt.org, ResourceOwners: []iam.ResourceOwnerResolver{tt.owners},
			}))
			e.Any("/*", func(c *echo.Context) error { return c.String(http.StatusOK, "ok") })

			req := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"/key", strings.NewReader(""))
			req.Header.Set("Authorization",
				"AWS4-HMAC-SHA256 Credential=ASIAFAKESESSIONKEY/20230101/us-east-1/s3/aws4_request")

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}
