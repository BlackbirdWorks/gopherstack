package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// lambdaCall is a thin wrapper around callInMemoryHandler that also accepts headers.
func lambdaCall(
	t *testing.T,
	h *lambda.Handler,
	method, path string,
	headers map[string]string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func lambdaParseBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

// TestAuditLambda_LoggingConfig_DefaultOnCreate verifies GetFunction returns
// LoggingConfig with format=Text and the correct log group.
func TestAuditLambda_LoggingConfig_DefaultOnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fnName       string
		wantLogGroup string
		wantFormat   string
	}{
		{
			name:         "basic function gets default logging config",
			fnName:       "log-fn",
			wantLogGroup: "/aws/lambda/log-fn",
			wantFormat:   "Text",
		},
		{
			name:         "different function name reflects in log group",
			fnName:       "my-other-fn",
			wantLogGroup: "/aws/lambda/my-other-fn",
			wantFormat:   "Text",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, tc.fnName)

			rec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/"+tc.fnName, "")
			require.Equal(t, http.StatusOK, rec.Code)

			body := lambdaParseBody(t, rec)
			cfg, ok := body["Configuration"].(map[string]any)
			require.True(t, ok, "response must have Configuration key")

			logCfg, ok := cfg["LoggingConfig"].(map[string]any)
			require.True(t, ok, "LoggingConfig must be present in Configuration")
			assert.Equal(t, tc.wantFormat, logCfg["LogFormat"], "LogFormat must be Text")
			assert.Equal(t, tc.wantLogGroup, logCfg["LogGroup"], "LogGroup must be /aws/lambda/{name}")
		})
	}
}

// TestAuditLambda_ExecutedVersionHeader verifies X-Amz-Executed-Version is set on invoke responses.
func TestAuditLambda_ExecutedVersionHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		qualifier      string
		wantVersion    string
		publishVersion bool
	}{
		{
			name:        "no qualifier returns $LATEST",
			qualifier:   "",
			wantVersion: "$LATEST",
		},
		{
			name:        "$LATEST qualifier returns $LATEST",
			qualifier:   "$LATEST",
			wantVersion: "$LATEST",
		},
		{
			name:           "version number qualifier returns that version",
			qualifier:      "1",
			wantVersion:    "1",
			publishVersion: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			createFunctionForTest(t, h, "exec-ver-fn")

			if tc.publishVersion {
				_, pubErr := bk.PublishVersion("exec-ver-fn", "")
				require.NoError(t, pubErr)
			}

			invPath := "/2015-03-31/functions/exec-ver-fn/invocations"
			if tc.qualifier != "" {
				invPath += "?Qualifier=" + url.QueryEscape(tc.qualifier)
			}

			rec := lambdaCall(t, h, http.MethodPost, invPath,
				map[string]string{"X-Amz-Invocation-Type": "DryRun"},
				`{}`)

			executedVer := rec.Header().Get("X-Amz-Executed-Version")
			assert.Equal(t, tc.wantVersion, executedVer,
				"X-Amz-Executed-Version must reflect resolved version")
		})
	}
}

// TestAuditLambda_ListESM_EventSourceArnFilter verifies ListEventSourceMappings
// filters by EventSourceArn query parameter.
func TestAuditLambda_ListESM_EventSourceArnFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    string
		wantCount int
	}{
		{
			name:      "no filter returns all",
			filter:    "",
			wantCount: 2,
		},
		{
			name:      "exact ARN match returns one",
			filter:    "arn:aws:sqs:us-east-1:000000000000:queue-a",
			wantCount: 1,
		},
		{
			name:      "non-matching ARN returns none",
			filter:    "arn:aws:sqs:us-east-1:000000000000:no-such-queue",
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			createFunctionForTest(t, h, "esm-filter-fn")

			// Seed two ESMs with different source ARNs.
			for _, queueSuffix := range []string{"queue-a", "queue-b"} {
				sourceARN := fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:%s", queueSuffix)
				_, esmErr := bk.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
					EventSourceARN:   sourceARN,
					FunctionName:     "esm-filter-fn",
					StartingPosition: "TRIM_HORIZON",
					Enabled:          true,
				})
				require.NoError(t, esmErr)
			}

			listPath := "/2015-03-31/event-source-mappings/"
			if tc.filter != "" {
				listPath += "?EventSourceArn=" + url.QueryEscape(tc.filter)
			}

			rec := callInMemoryHandler(t, h, http.MethodGet, listPath, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			esms, _ := out["EventSourceMappings"].([]any)
			assert.Len(t, esms, tc.wantCount,
				"EventSourceMappings count mismatch for filter=%q", tc.filter)
		})
	}
}

// TestAuditLambda_PolicyJSON_SourceArnCondition verifies that AddPermission and GetPolicy
// include Condition blocks when SourceArn/SourceAccount are provided.
func TestAuditLambda_PolicyJSON_SourceArnCondition(t *testing.T) {
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

// TestAuditLambda_ListLayers_CompatibleRuntimeFilter verifies CompatibleRuntime filtering.
func TestAuditLambda_ListLayers_CompatibleRuntimeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		compatibleRuntime string
		wantCount         int
	}{
		{
			name:      "no filter returns all layers",
			wantCount: 2,
		},
		{
			name:              "filter python3.12 returns one",
			compatibleRuntime: "python3.12",
			wantCount:         1,
		},
		{
			name:              "filter nodejs20.x returns one",
			compatibleRuntime: "nodejs20.x",
			wantCount:         1,
		},
		{
			name:              "filter no-match returns none",
			compatibleRuntime: "ruby3.3",
			wantCount:         0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)

			// Publish two layers with different runtimes.
			_, err := bk.PublishLayerVersion(&lambda.PublishLayerVersionInput{
				LayerName:          "python-layer",
				CompatibleRuntimes: []string{"python3.12"},
				Content:            &lambda.LayerVersionContentInput{},
			})
			require.NoError(t, err)

			_, err = bk.PublishLayerVersion(&lambda.PublishLayerVersionInput{
				LayerName:          "node-layer",
				CompatibleRuntimes: []string{"nodejs20.x"},
				Content:            &lambda.LayerVersionContentInput{},
			})
			require.NoError(t, err)

			listPath := "/2018-10-31/layers"
			if tc.compatibleRuntime != "" {
				listPath += "?CompatibleRuntime=" + tc.compatibleRuntime
			}

			rec := callInMemoryHandler(t, h, http.MethodGet, listPath, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			layers, _ := out["Layers"].([]any)
			assert.Len(t, layers, tc.wantCount, "layer count mismatch for runtime=%q", tc.compatibleRuntime)
		})
	}
}

// TestAuditLambda_GetAccountSettings_UnreservedConcurrency verifies
// UnreservedConcurrentExecutions decreases when reserved concurrency is set.
func TestAuditLambda_GetAccountSettings_UnreservedConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		reservePerFn   int
		numFunctions   int
		wantUnreserved int
	}{
		{
			name:           "no reserved concurrency — all unreserved",
			numFunctions:   2,
			reservePerFn:   0,
			wantUnreserved: 1000,
		},
		{
			name:           "100 reserved → 900 unreserved",
			numFunctions:   1,
			reservePerFn:   100,
			wantUnreserved: 900,
		},
		{
			name:           "two functions each reserving 200 → 600 unreserved",
			numFunctions:   2,
			reservePerFn:   200,
			wantUnreserved: 600,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)

			for i := range tc.numFunctions {
				fnName := fmt.Sprintf("concurrency-fn-%d", i)
				createFunctionForTest(t, h, fnName)

				if tc.reservePerFn > 0 {
					_, putErr := bk.PutFunctionConcurrency(fnName, tc.reservePerFn)
					require.NoError(t, putErr)
				}
			}

			rec := callInMemoryHandler(t, h, http.MethodGet, "/2016-08-19/account-settings", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			limit, ok := out["AccountLimit"].(map[string]any)
			require.True(t, ok, "AccountLimit must be present")

			got, ok := limit["UnreservedConcurrentExecutions"].(float64)
			require.True(t, ok, "UnreservedConcurrentExecutions must be numeric")
			assert.Equal(t, tc.wantUnreserved, int(got))
		})
	}
}

// TestAuditLambda_ListFunctions_FunctionVersionAll verifies that ListFunctions?FunctionVersion=ALL
// returns published versions alongside $LATEST.
func TestAuditLambda_ListFunctions_FunctionVersionAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fnName       string
		publishCount int
		wantMinCount int // at minimum: $LATEST + published versions
	}{
		{
			name:         "no published versions — only $LATEST",
			fnName:       "fn-no-ver",
			publishCount: 0,
			wantMinCount: 1,
		},
		{
			name:         "two published versions — $LATEST + 2",
			fnName:       "fn-two-ver",
			publishCount: 2,
			wantMinCount: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			createFunctionForTest(t, h, tc.fnName)

			for range tc.publishCount {
				_, pubErr := bk.PublishVersion(tc.fnName, "")
				require.NoError(t, pubErr)
			}

			rec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions?FunctionVersion=ALL", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			fns, _ := out["Functions"].([]any)
			assert.GreaterOrEqual(t, len(fns), tc.wantMinCount,
				"FunctionVersion=ALL must include $LATEST and all published versions")

			// Verify versions present in response.
			versions := make(map[string]bool)
			for _, f := range fns {
				fm := f.(map[string]any)
				v, _ := fm["Version"].(string)
				versions[v] = true
			}
			assert.True(t, versions["$LATEST"], "$LATEST must appear in FunctionVersion=ALL")

			for i := 1; i <= tc.publishCount; i++ {
				assert.True(t, versions[strconv.Itoa(i)],
					"version %d must appear in FunctionVersion=ALL", i)
			}
		})
	}
}
