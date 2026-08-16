package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// ============================================================
// AddPermission / RemovePermission / GetPolicy
// ============================================================

func TestPermission_FullLifecycle(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "perm-fn")

	// Add first permission
	addRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/perm-fn/policy",
		`{"StatementId":"AllowS3","Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`)
	require.Equal(t, http.StatusCreated, addRec.Code)

	var addOut lambda.AddPermissionOutput
	require.NoError(t, json.NewDecoder(addRec.Body).Decode(&addOut))
	require.NotNil(t, addOut.Statement)
	assert.NotEmpty(t, *addOut.Statement)

	// Add second permission
	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/perm-fn/policy",
		`{"StatementId":"AllowSNS","Action":"lambda:InvokeFunction","Principal":"sns.amazonaws.com"}`)

	// Get policy
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/perm-fn/policy", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var pol lambda.GetPolicyOutput
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&pol))
	require.NotNil(t, pol.Policy)
	assert.Contains(t, *pol.Policy, "AllowS3")
	assert.Contains(t, *pol.Policy, "AllowSNS")

	// Remove permission. Real Lambda sends StatementId as a URI path segment
	// (/policy/{StatementId}), never as a "?StatementId=" query parameter —
	// this used to be a query string here, which encoded the pre-fix (wrong)
	// wire shape and could never be hit by a real aws-sdk-go-v2 client.
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/perm-fn/policy/AllowS3", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get policy after remove
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/perm-fn/policy", "")
	require.Equal(t, http.StatusOK, getRec2.Code)

	var pol2 lambda.GetPolicyOutput
	require.NoError(t, json.NewDecoder(getRec2.Body).Decode(&pol2))
	assert.NotContains(t, *pol2.Policy, "AllowS3")
	assert.Contains(t, *pol2.Policy, "AllowSNS")
}

// Test_Permission_ErrorCases covers GetPolicy/RemovePermission error paths that
// don't fit the full add/get/remove lifecycle above.
func Test_Permission_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		fnName     string
		method     string
		pathSuffix string
		wantStatus int
	}{
		{
			name:       "GetPolicy on a function with no permissions returns 404",
			fnName:     "nopolicy-fn",
			method:     http.MethodGet,
			pathSuffix: "/policy",
			wantStatus: http.StatusNotFound,
		},
		{
			// Wire format: StatementId is a path segment, not a query param.
			name:       "RemovePermission for a nonexistent statement returns 404",
			fnName:     "rm-404-fn",
			method:     http.MethodDelete,
			pathSuffix: "/policy/nonexistent-stmt",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, tc.fnName)

			rec := callInMemoryHandler(t, h, tc.method,
				"/2015-03-31/functions/"+tc.fnName+tc.pathSuffix, "")
			assert.Equal(t, tc.wantStatus, rec.Code)
		})
	}
}

func TestPermission_SourceArn(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "src-arn-fn")

	body := `{
		"StatementId":"AllowSpecificBucket",
		"Action":"lambda:InvokeFunction",
		"Principal":"s3.amazonaws.com",
		"SourceArn":"arn:aws:s3:::my-bucket",
		"SourceAccount":"000000000000"
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/src-arn-fn/policy", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var addOut lambda.AddPermissionOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&addOut))
	require.NotNil(t, addOut.Statement)
	assert.NotEmpty(t, *addOut.Statement)
	// Statement must include the StatementId
	assert.Contains(t, *addOut.Statement, "AllowSpecificBucket")
}

// Test_AddPermission_Qualifier verifies real Lambda's per-qualifier
// resource-based policies (parity-sweep-3 fix): a statement added with
// Qualifier=<version> is stored separately from the unqualified policy and
// only visible via GetPolicy scoped to that same qualifier, and AddPermission
// rejects Qualifier=$LATEST exactly as AWS does ("Lambda does not support
// adding policies to version $LATEST"). Each case builds its own handler,
// backend, and function from scratch — no case depends on state left behind
// by another, and every case is safe to run in parallel.
func Test_AddPermission_Qualifier(t *testing.T) {
	t.Parallel()

	const fnName = "qual-fn"

	tests := []struct {
		name           string
		qualifier      string
		publishVersion bool
		wantAddStatus  int
	}{
		{
			name:          "rejects Qualifier=$LATEST",
			qualifier:     "$LATEST",
			wantAddStatus: http.StatusBadRequest,
		},
		{
			name:          "rejects an unknown qualifier",
			qualifier:     "99",
			wantAddStatus: http.StatusNotFound,
		},
		{
			name:           "scoped to a published version succeeds and is isolated from the unqualified policy",
			qualifier:      "1",
			publishVersion: true,
			wantAddStatus:  http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			createFunctionForTest(t, h, fnName)

			if tc.publishVersion {
				_, pubErr := bk.PublishVersion(fnName, "")
				require.NoError(t, pubErr)
			}

			addRec := callInMemoryHandler(t, h, http.MethodPost,
				"/2015-03-31/functions/"+fnName+"/policy?Qualifier="+tc.qualifier,
				`{"StatementId":"s-stmt","Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`)
			require.Equal(t, tc.wantAddStatus, addRec.Code)

			if tc.wantAddStatus != http.StatusCreated {
				return
			}

			// The qualified statement must not leak into the unqualified policy:
			// no unqualified statement was ever added, so GetPolicy 404s.
			unqualRec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/"+fnName+"/policy", "")
			assert.Equal(t, http.StatusNotFound, unqualRec.Code)

			// ...but it is visible when GetPolicy is scoped to the same qualifier,
			// and the Resource ARN in the statement carries the qualifier suffix.
			qualRec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/"+fnName+"/policy?Qualifier="+tc.qualifier, "")
			require.Equal(t, http.StatusOK, qualRec.Code)

			qualOut := lambdaParseBody(t, qualRec)
			policyStr, _ := qualOut["Policy"].(string)
			assert.Contains(t, policyStr, "s-stmt")
			assert.Contains(t, policyStr, "arn:aws:lambda:us-east-1:000000000000:function:qual-fn:1")
		})
	}
}

// TestPolicyJSON_SourceArnCondition verifies that AddPermission and GetPolicy
// include Condition blocks when SourceArn/SourceAccount are provided.
func TestPolicyJSON_SourceArnCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		addInput       string
		wantSourceArn  string
		wantSourceAcct string
		wantCondition  bool
	}{
		{
			name: "no source constraint — no Condition block",
			addInput: `{"StatementId":"s1","Action":"lambda:InvokeFunction",` +
				`"Principal":"events.amazonaws.com"}`,
			wantCondition: false,
		},
		{
			name: "SourceArn produces ArnLike condition",
			addInput: `{"StatementId":"s1","Action":"lambda:InvokeFunction",` +
				`"Principal":"events.amazonaws.com",` +
				`"SourceArn":"arn:aws:events:us-east-1:000000000000:rule/r"}`,
			wantCondition: true,
			wantSourceArn: "arn:aws:events:us-east-1:000000000000:rule/r",
		},
		{
			name: "SourceAccount produces StringEquals condition",
			addInput: `{"StatementId":"s1","Action":"lambda:InvokeFunction",` +
				`"Principal":"events.amazonaws.com","SourceAccount":"123456789012"}`,
			wantCondition:  true,
			wantSourceAcct: "123456789012",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "policy-fn")

			rec := lambdaCall(t, h, http.MethodPost,
				"/2015-03-31/functions/policy-fn/policy", nil, tc.addInput)
			require.Equal(t, http.StatusCreated, rec.Code, "AddPermission must succeed")

			addOut := lambdaParseBody(t, rec)
			stmtRaw, _ := addOut["Statement"].(string)
			require.NotEmpty(t, stmtRaw, "AddPermission must return Statement")

			// GetPolicy should return the same statement in the policy.
			policyRec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/policy-fn/policy", "")
			require.Equal(t, http.StatusOK, policyRec.Code)

			policyOut := lambdaParseBody(t, policyRec)
			policyStr, _ := policyOut["Policy"].(string)
			require.NotEmpty(t, policyStr)

			var policy map[string]any
			require.NoError(t, json.Unmarshal([]byte(policyStr), &policy))
			stmts, _ := policy["Statement"].([]any)
			require.Len(t, stmts, 1)
			stmt := stmts[0].(map[string]any)

			if !tc.wantCondition {
				_, hasCondition := stmt["Condition"]
				assert.False(t, hasCondition, "no Condition block expected")
			} else {
				condition, condOk := stmt["Condition"].(map[string]any)
				require.True(t, condOk, "Condition block must be present")

				if tc.wantSourceArn != "" {
					arnLike, arnOk := condition["ArnLike"].(map[string]any)
					require.True(t, arnOk, "ArnLike must be in Condition")
					assert.Equal(t, tc.wantSourceArn, arnLike["AWS:SourceArn"])
				}
				if tc.wantSourceAcct != "" {
					strEq, acctOk := condition["StringEquals"].(map[string]any)
					require.True(t, acctOk, "StringEquals must be in Condition")
					assert.Equal(t, tc.wantSourceAcct, strEq["AWS:SourceAccount"])
				}
			}
		})
	}
}

// --- AddPermission tests ---

func TestAddPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		fn         string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			fn:         "add-perm-fn",
			body:       `{"StatementId":"AllowS3","Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "missing_statement_id",
			fn:         "add-perm-fn",
			body:       `{"Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_action",
			fn:         "add-perm-fn",
			body:       `{"StatementId":"s1","Principal":"s3.amazonaws.com"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_principal",
			fn:         "add-perm-fn",
			body:       `{"StatementId":"s1","Action":"lambda:InvokeFunction"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "function_not_found",
			fn:         "nonexistent",
			body:       `{"StatementId":"s1","Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_json",
			fn:         "add-perm-fn",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "add-perm-fn")

			rec := callInMemoryHandler(
				t, h,
				http.MethodPost,
				fmt.Sprintf("/2015-03-31/functions/%s/policy", tt.fn),
				tt.body,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				var out lambda.AddPermissionOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				require.NotNil(t, out.Statement)
				assert.NotEmpty(t, *out.Statement)
			}
		})
	}
}

func TestAddPermission_Duplicate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "dup-fn")

	body := `{"StatementId":"stmt1","Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`
	path := "/2015-03-31/functions/dup-fn/policy"

	rec1 := callInMemoryHandler(t, h, http.MethodPost, path, body)
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := callInMemoryHandler(t, h, http.MethodPost, path, body)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// TestAddPermission_FunctionUrlConditions locks in the FunctionUrlAuthType /
// InvokedViaFunctionUrl deferred item (PARITY.md): AddPermission must accept
// both fields and render them as IAM policy Condition entries — a
// StringEquals on "lambda:FunctionUrlAuthType" and a Bool on
// "lambda:InvokedViaFunctionUrl" — exactly as real Lambda does for public
// function URL access statements.
func TestAddPermission_FunctionUrlConditions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-perm-fn")

	body := `{
		"StatementId":"FunctionURLAllowPublicAccess",
		"Action":"lambda:InvokeFunctionUrl",
		"Principal":"*",
		"FunctionUrlAuthType":"NONE",
		"InvokedViaFunctionUrl":true
	}`

	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/url-perm-fn/policy", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out lambda.AddPermissionOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotNil(t, out.Statement)
	assert.Contains(t, *out.Statement, `"StringEquals":{"lambda:FunctionUrlAuthType":"NONE"}`)
	assert.Contains(t, *out.Statement, `"Bool":{"lambda:InvokedViaFunctionUrl":"true"}`)

	getRec := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/functions/url-perm-fn/policy", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var pol lambda.GetPolicyOutput
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&pol))
	require.NotNil(t, pol.Policy)
	assert.Contains(t, *pol.Policy, "lambda:FunctionUrlAuthType")
	assert.Contains(t, *pol.Policy, "lambda:InvokedViaFunctionUrl")
}

// TestAddPermission_RevisionId locks in the RevisionId optimistic-concurrency
// deferred item: GetPolicy's RevisionId must change across a real mutation
// (add or remove a statement) and stay stable otherwise, and AddPermission /
// RemovePermission must reject a stale RevisionId with
// PreconditionFailedException (412) without mutating the policy.
func TestAddPermission_RevisionId(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rev-fn")

	policyPath := "/2015-03-31/functions/rev-fn/policy"

	// No policy yet -> GetPolicy 404s, nothing to assert a revision against.
	rec := callInMemoryHandler(t, h, http.MethodPost, policyPath,
		`{"StatementId":"s1","Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	getRec := callInMemoryHandler(t, h, http.MethodGet, policyPath, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var pol lambda.GetPolicyOutput
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&pol))
	require.NotNil(t, pol.RevisionID)
	rev1 := *pol.RevisionID
	assert.NotEmpty(t, rev1)

	// A stale RevisionId on AddPermission is rejected without mutating the policy.
	staleAddRec := callInMemoryHandler(t, h, http.MethodPost, policyPath,
		`{"StatementId":"s2","Action":"lambda:InvokeFunction","Principal":"sns.amazonaws.com",`+
			`"RevisionId":"not-the-real-revision"}`)
	assert.Equal(t, http.StatusPreconditionFailed, staleAddRec.Code)

	getRec2 := callInMemoryHandler(t, h, http.MethodGet, policyPath, "")
	var pol2 lambda.GetPolicyOutput
	require.NoError(t, json.NewDecoder(getRec2.Body).Decode(&pol2))
	assert.Equal(t, rev1, *pol2.RevisionID, "rejected AddPermission must not change the revision")
	assert.NotContains(t, *pol2.Policy, "s2")

	// The correct RevisionId succeeds and produces a new revision.
	okAddRec := callInMemoryHandler(t, h, http.MethodPost, policyPath,
		fmt.Sprintf(`{"StatementId":"s2","Action":"lambda:InvokeFunction","Principal":"sns.amazonaws.com",`+
			`"RevisionId":%q}`, rev1))
	require.Equal(t, http.StatusCreated, okAddRec.Code)

	getRec3 := callInMemoryHandler(t, h, http.MethodGet, policyPath, "")
	var pol3 lambda.GetPolicyOutput
	require.NoError(t, json.NewDecoder(getRec3.Body).Decode(&pol3))
	rev3 := *pol3.RevisionID
	assert.NotEqual(t, rev1, rev3, "a real mutation must change the revision")

	// RemovePermission with a stale RevisionId (query param) is rejected.
	staleDelRec := callInMemoryHandler(t, h, http.MethodDelete,
		policyPath+"/s1?RevisionId=not-the-real-revision", "")
	assert.Equal(t, http.StatusPreconditionFailed, staleDelRec.Code)

	getRec4 := callInMemoryHandler(t, h, http.MethodGet, policyPath, "")
	var pol4 lambda.GetPolicyOutput
	require.NoError(t, json.NewDecoder(getRec4.Body).Decode(&pol4))
	assert.Contains(t, *pol4.Policy, "s1", "rejected RemovePermission must not delete the statement")

	// RemovePermission with the correct RevisionId succeeds.
	okDelRec := callInMemoryHandler(t, h, http.MethodDelete,
		policyPath+"/s1?RevisionId="+rev3, "")
	assert.Equal(t, http.StatusNoContent, okDelRec.Code)
}
