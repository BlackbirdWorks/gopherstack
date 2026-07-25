package ssoadmin_test

// Tests for NextToken pagination on SSO Admin list ops. Previously these ops
// hardcoded NextToken to null, so a client could never page past the first
// MaxResults results.

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListPermissionSets_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "pagination-inst")

	for _, name := range []string{"ps-a", "ps-b", "ps-c", "ps-d", "ps-e"} {
		createPermissionSet(t, h, instanceArn, name)
	}

	collectPage := func(token any) ([]any, any) {
		body := map[string]any{"InstanceArn": instanceArn, "MaxResults": 2}
		if token != nil {
			body["NextToken"] = token
		}

		rec := doRequest(t, h, "ListPermissionSets", body)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		resp := parseResponse(t, rec)
		sets, ok := resp["PermissionSets"].([]any)
		require.True(t, ok)

		return sets, resp["NextToken"]
	}

	page1, next1 := collectPage(nil)
	assert.Len(t, page1, 2)
	require.NotNil(t, next1)

	page2, next2 := collectPage(next1)
	assert.Len(t, page2, 2)
	require.NotNil(t, next2)

	page3, next3 := collectPage(next2)
	assert.Len(t, page3, 1)
	assert.Nil(t, next3)

	seen := map[string]bool{}
	for _, page := range [][]any{page1, page2, page3} {
		for _, arn := range page {
			s, ok := arn.(string)
			require.True(t, ok)
			assert.False(t, seen[s], "duplicate %s across pages", s)
			seen[s] = true
		}
	}

	assert.Len(t, seen, 5)
}

// TestListManagedPoliciesInPermissionSet_Pagination locks in MaxResults +
// NextToken pagination on ListManagedPoliciesInPermissionSet, which
// previously ignored MaxResults entirely and always returned a nil NextToken.
func TestListManagedPoliciesInPermissionSet_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "mp-pagination-inst")
	psArn := createPermissionSet(t, h, instanceArn, "MPPaginationPS")

	for _, arn := range []string{
		"arn:aws:iam::aws:policy/AlphaPolicy",
		"arn:aws:iam::aws:policy/BetaPolicy",
		"arn:aws:iam::aws:policy/GammaPolicy",
	} {
		rec := doRequest(t, h, "AttachManagedPolicyToPermissionSet", map[string]any{
			"InstanceArn":      instanceArn,
			"PermissionSetArn": psArn,
			"ManagedPolicyArn": arn,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	var token any

	seen := map[string]bool{}

	for {
		body := map[string]any{"InstanceArn": instanceArn, "PermissionSetArn": psArn, "MaxResults": 2}
		if token != nil {
			body["NextToken"] = token
		}

		rec := doRequest(t, h, "ListManagedPoliciesInPermissionSet", body)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		resp := parseResponse(t, rec)
		policies, ok := resp["AttachedManagedPolicies"].([]any)
		require.True(t, ok)
		assert.LessOrEqual(t, len(policies), 2)

		for _, p := range policies {
			m, mOK := p.(map[string]any)
			require.True(t, mOK)
			arn, arnOK := m["Arn"].(string)
			require.True(t, arnOK)
			seen[arn] = true
		}

		token = resp["NextToken"]
		if token == nil {
			break
		}
	}

	assert.Len(t, seen, 3)
}

// TestListCustomerManagedPolicyReferencesInPermissionSet_Pagination locks in
// MaxResults + NextToken pagination, previously ignored entirely.
func TestListCustomerManagedPolicyReferencesInPermissionSet_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "cmpr-pagination-inst")
	psArn := createPermissionSet(t, h, instanceArn, "CMPRPaginationPS")

	for _, name := range []string{"AlphaRef", "BetaRef", "GammaRef"} {
		rec := doRequest(t, h, "AttachCustomerManagedPolicyReferenceToPermissionSet", map[string]any{
			"InstanceArn":      instanceArn,
			"PermissionSetArn": psArn,
			"CustomerManagedPolicyReference": map[string]any{
				"Name": name,
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListCustomerManagedPolicyReferencesInPermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"MaxResults":       2,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	refs, ok := resp["CustomerManagedPolicyReferences"].([]any)
	require.True(t, ok)
	assert.Len(t, refs, 2, "MaxResults must cap the page size")
	require.NotNil(t, resp["NextToken"])
}

// TestListApplicationAccessScopes_Pagination locks in MaxResults + NextToken
// pagination, previously ignored entirely.
func TestListApplicationAccessScopes_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "aas-pagination-inst")
	appArn := createApplication(t, h, instanceArn, "AASPaginationApp")

	for _, scope := range []string{"scope:alpha", "scope:beta", "scope:gamma"} {
		rec := doRequest(t, h, "PutApplicationAccessScope", map[string]any{
			"ApplicationArn": appArn,
			"Scope":          scope,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListApplicationAccessScopes", map[string]any{
		"ApplicationArn": appArn,
		"MaxResults":     2,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	scopes, ok := resp["Scopes"].([]any)
	require.True(t, ok)
	assert.Len(t, scopes, 2, "MaxResults must cap the page size")
	require.NotNil(t, resp["NextToken"])
}

// TestListTrustedTokenIssuers_Pagination locks in MaxResults + NextToken
// pagination, previously ignored entirely.
func TestListTrustedTokenIssuers_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tti-pagination-inst")

	for _, name := range []string{"IssuerAlpha", "IssuerBeta", "IssuerGamma"} {
		rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
			"InstanceArn":            instanceArn,
			"Name":                   name,
			"TrustedTokenIssuerType": "OIDC_JWT",
			"TrustedTokenIssuerConfiguration": map[string]any{
				"OidcJwtConfiguration": map[string]any{
					"IssuerUrl":                  "https://issuer.example.com/" + name,
					"ClaimAttributePath":         "email",
					"IdentityStoreAttributePath": "emails.value",
					"JwksRetrievalOption":        "OPEN_ID_DISCOVERY",
				},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	}

	rec := doRequest(t, h, "ListTrustedTokenIssuers", map[string]any{
		"InstanceArn": instanceArn,
		"MaxResults":  2,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	issuers, ok := resp["TrustedTokenIssuers"].([]any)
	require.True(t, ok)
	assert.Len(t, issuers, 2, "MaxResults must cap the page size")
	require.NotNil(t, resp["NextToken"])
}

// TestListRegions_Pagination locks in MaxResults + NextToken pagination on
// ListRegions, previously ignored entirely.
func TestListRegions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "region-pagination-inst")

	for _, region := range []string{"us-west-2", "eu-west-1", "ap-south-1"} {
		rec := doRequest(t, h, "AddRegion", map[string]any{
			"InstanceArn": instanceArn,
			"RegionName":  region,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListRegions", map[string]any{
		"InstanceArn": instanceArn,
		"MaxResults":  2,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	regions, ok := resp["Regions"].([]any)
	require.True(t, ok)
	assert.Len(t, regions, 2, "MaxResults must cap the page size")
	require.NotNil(t, resp["NextToken"])
}

// TestListApplicationProviders_Pagination locks in MaxResults + NextToken
// pagination on ListApplicationProviders (previously ignored entirely) and
// the FederationProtocol wire field (previously silently dropped even though
// populated in every seeded catalog entry).
func TestListApplicationProviders_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doRequest(t, h, "ListApplicationProviders", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	providers, ok := resp["ApplicationProviders"].([]any)
	require.True(t, ok)
	assert.Len(t, providers, 2, "MaxResults must cap the page size")
	require.NotNil(t, resp["NextToken"])

	for _, p := range providers {
		m, mOK := p.(map[string]any)
		require.True(t, mOK)
		assert.Equal(t, "SAML", m["FederationProtocol"], "FederationProtocol must be present on the wire")
	}
}

// TestListApplicationAssignments_Pagination locks in MaxResults + NextToken
// pagination, previously ignored entirely.
func TestListApplicationAssignments_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "appassign-pagination-inst")
	appArn := createApplication(t, h, instanceArn, "AppAssignPaginationApp")

	for _, id := range []string{"user-a", "user-b", "user-c"} {
		rec := doRequest(t, h, "CreateApplicationAssignment", map[string]any{
			"ApplicationArn": appArn,
			"PrincipalId":    id,
			"PrincipalType":  "USER",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListApplicationAssignments", map[string]any{
		"ApplicationArn": appArn,
		"MaxResults":     2,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assignments, ok := resp["ApplicationAssignments"].([]any)
	require.True(t, ok)
	assert.Len(t, assignments, 2, "MaxResults must cap the page size")
	require.NotNil(t, resp["NextToken"])
}

// TestListApplicationAssignmentsForPrincipal_FilterAndPagination locks in
// both the Filter.ApplicationArn support and MaxResults + NextToken
// pagination on ListApplicationAssignmentsForPrincipal, previously ignored
// entirely (always returned every assignment for the principal in one page).
func TestListApplicationAssignmentsForPrincipal_FilterAndPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "appassign-principal-inst")
	app1 := createApplication(t, h, instanceArn, "FilterApp1")
	app2 := createApplication(t, h, instanceArn, "FilterApp2")

	for _, appArn := range []string{app1, app2} {
		rec := doRequest(t, h, "CreateApplicationAssignment", map[string]any{
			"ApplicationArn": appArn,
			"PrincipalId":    "shared-user",
			"PrincipalType":  "USER",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Filter to just app1.
	rec := doRequest(t, h, "ListApplicationAssignmentsForPrincipal", map[string]any{
		"InstanceArn":   instanceArn,
		"PrincipalId":   "shared-user",
		"PrincipalType": "USER",
		"Filter":        map[string]any{"ApplicationArn": app1},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assignments, ok := resp["ApplicationAssignments"].([]any)
	require.True(t, ok)
	require.Len(t, assignments, 1)
	first, ok := assignments[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, app1, first["ApplicationArn"])

	// No filter + MaxResults=1 must paginate across both.
	rec = doRequest(t, h, "ListApplicationAssignmentsForPrincipal", map[string]any{
		"InstanceArn":   instanceArn,
		"PrincipalId":   "shared-user",
		"PrincipalType": "USER",
		"MaxResults":    1,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseResponse(t, rec)
	assignments, ok = resp["ApplicationAssignments"].([]any)
	require.True(t, ok)
	assert.Len(t, assignments, 1, "MaxResults must cap the page size")
	require.NotNil(t, resp["NextToken"])
}

// TestListAccountAssignmentsForPrincipal_FilterAndPagination locks in both
// the Filter.AccountId support and MaxResults + NextToken pagination on
// ListAccountAssignmentsForPrincipal, previously ignored entirely.
func TestListAccountAssignmentsForPrincipal_FilterAndPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "acctassign-principal-inst")
	psArn := createPermissionSet(t, h, instanceArn, "AcctAssignPrincipalPS")

	for _, account := range []string{"111111111111", "222222222222"} {
		rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
			"InstanceArn":      instanceArn,
			"PermissionSetArn": psArn,
			"TargetId":         account,
			"TargetType":       "AWS_ACCOUNT",
			"PrincipalId":      "shared-user",
			"PrincipalType":    "USER",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Filter to just the first account.
	rec := doRequest(t, h, "ListAccountAssignmentsForPrincipal", map[string]any{
		"InstanceArn":   instanceArn,
		"PrincipalId":   "shared-user",
		"PrincipalType": "USER",
		"Filter":        map[string]any{"AccountId": "111111111111"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assignments, ok := resp["AccountAssignments"].([]any)
	require.True(t, ok)
	require.Len(t, assignments, 1)
	first, ok := assignments[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "111111111111", first["AccountId"])

	// No filter + MaxResults=1 must paginate across both.
	rec = doRequest(t, h, "ListAccountAssignmentsForPrincipal", map[string]any{
		"InstanceArn":   instanceArn,
		"PrincipalId":   "shared-user",
		"PrincipalType": "USER",
		"MaxResults":    1,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp = parseResponse(t, rec)
	assignments, ok = resp["AccountAssignments"].([]any)
	require.True(t, ok)
	assert.Len(t, assignments, 1, "MaxResults must cap the page size")
	require.NotNil(t, resp["NextToken"])
}

// TestListPermissionSetsProvisionedToAccount_Pagination locks in MaxResults +
// NextToken pagination, previously ignored entirely.
func TestListPermissionSetsProvisionedToAccount_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "pstpa-pagination-inst")

	for _, name := range []string{"PSAlpha", "PSBeta", "PSGamma"} {
		psArn := createPermissionSet(t, h, instanceArn, name)
		rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
			"InstanceArn":      instanceArn,
			"PermissionSetArn": psArn,
			"TargetId":         "333333333333",
			"TargetType":       "AWS_ACCOUNT",
			"PrincipalId":      "user-x",
			"PrincipalType":    "USER",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListPermissionSetsProvisionedToAccount", map[string]any{
		"InstanceArn": instanceArn,
		"AccountId":   "333333333333",
		"MaxResults":  2,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	sets, ok := resp["PermissionSets"].([]any)
	require.True(t, ok)
	assert.Len(t, sets, 2, "MaxResults must cap the page size")
	require.NotNil(t, resp["NextToken"])
}

// TestListAccountsForProvisionedPermissionSet_Pagination locks in MaxResults
// + NextToken pagination, previously ignored entirely.
func TestListAccountsForProvisionedPermissionSet_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "afpps-pagination-inst")
	psArn := createPermissionSet(t, h, instanceArn, "AFPPSPaginationPS")

	for _, account := range []string{"111111111111", "222222222222", "333333333333"} {
		rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
			"InstanceArn":      instanceArn,
			"PermissionSetArn": psArn,
			"TargetId":         account,
			"TargetType":       "AWS_ACCOUNT",
			"PrincipalId":      "user-y",
			"PrincipalType":    "USER",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListAccountsForProvisionedPermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"MaxResults":       2,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	accounts, ok := resp["AccountIds"].([]any)
	require.True(t, ok)
	assert.Len(t, accounts, 2, "MaxResults must cap the page size")
	require.NotNil(t, resp["NextToken"])
}

func TestListInstances_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	for _, name := range []string{"inst-a", "inst-b", "inst-c"} {
		createInstance(t, h, name)
	}

	// Count total instances (a default instance may be seeded by the backend).
	allRec := doRequest(t, h, "ListInstances", nil)
	all, ok := parseResponse(t, allRec)["Instances"].([]any)
	require.True(t, ok)
	total := len(all)
	require.GreaterOrEqual(t, total, 3)

	// Page with MaxResults=2 and walk all pages, ensuring no duplicates and
	// that NextToken is nil exactly on the final page.
	var token any
	seen := map[string]bool{}
	pages := 0

	for {
		body := map[string]any{"MaxResults": 2}
		if token != nil {
			body["NextToken"] = token
		}

		rec := doRequest(t, h, "ListInstances", body)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		resp := parseResponse(t, rec)
		insts, instsOK := resp["Instances"].([]any)
		require.True(t, instsOK)
		assert.LessOrEqual(t, len(insts), 2)

		for _, inst := range insts {
			m, mOK := inst.(map[string]any)
			require.True(t, mOK)
			arn, arnOK := m["InstanceArn"].(string)
			require.True(t, arnOK)
			assert.False(t, seen[arn], "duplicate %s", arn)
			seen[arn] = true
		}

		pages++
		require.Less(t, pages, 100, "pagination did not terminate")

		token = resp["NextToken"]
		if token == nil {
			break
		}
	}

	assert.Len(t, seen, total)
}

// TestListPermissionSets_NoPaginationReturnsAll verifies that without MaxResults
// the op returns every item and a nil NextToken (back-compat with callers that
// never paginate).
func TestListPermissionSets_NoPaginationReturnsAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "all-inst")

	for _, name := range []string{"x", "y", "z"} {
		createPermissionSet(t, h, instanceArn, name)
	}

	rec := doRequest(t, h, "ListPermissionSets", map[string]any{"InstanceArn": instanceArn})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	sets, ok := resp["PermissionSets"].([]any)
	require.True(t, ok)
	assert.Len(t, sets, 3)
	assert.Nil(t, resp["NextToken"])
}

// TestOpaqueNextToken verifies that NextToken returned by list ops is
// base64-encoded and NOT equal to the raw cursor value (e.g. a permission-set
// ARN), making it opaque to callers.
func TestOpaqueNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		makeReq func(instanceArn string) map[string]any
		items   []string
	}{
		{
			name:  "ListPermissionSets",
			op:    "ListPermissionSets",
			items: []string{"alpha", "beta", "gamma"},
			makeReq: func(instanceArn string) map[string]any {
				return map[string]any{"InstanceArn": instanceArn, "MaxResults": 1}
			},
		},
		{
			name:  "ListInstances",
			op:    "ListInstances",
			items: []string{"inst-x", "inst-y"},
			makeReq: func(_ string) map[string]any {
				return map[string]any{"MaxResults": 1}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "opaque-test")
			for _, name := range tt.items {
				if tt.op == "ListPermissionSets" {
					createPermissionSet(t, h, instanceArn, name)
				} else {
					createInstance(t, h, name)
				}
			}

			rec := doRequest(t, h, tt.op, tt.makeReq(instanceArn))
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)

			token, hasToken := resp["NextToken"]
			if !hasToken || token == nil {
				t.Skip("no NextToken returned — not enough items for pagination")
			}

			tokenStr, ok := token.(string)
			require.True(t, ok, "NextToken should be a string")
			require.NotEmpty(t, tokenStr)

			// Must be valid base64.
			decoded, err := base64.StdEncoding.DecodeString(tokenStr)
			require.NoError(t, err, "NextToken should be base64-encoded, got %q", tokenStr)

			// Decoded value must not equal the token itself (i.e. was actually encoded).
			assert.NotEqual(t, tokenStr, string(decoded),
				"NextToken should be opaque (base64), not the raw cursor")

			// The decoded value should NOT look like a raw ARN or plain name
			// sitting unencoded in the token string.
			assert.False(t, strings.HasPrefix(tokenStr, "arn:"),
				"NextToken should not expose raw ARN as cursor")

			// Second page should be reachable using the token.
			req2 := tt.makeReq(instanceArn)
			req2["NextToken"] = tokenStr
			rec2 := doRequest(t, h, tt.op, req2)
			require.Equal(t, http.StatusOK, rec2.Code)
		})
	}
}
