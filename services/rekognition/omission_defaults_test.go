package rekognition_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListProjectPolicies_DefaultPageSize locks in the real SDK's documented
// MaxResults default for this operation: "The maximum number of results to
// return per paginated call. The largest value you can specify is 5. ...
// The default value is 5." (api_op_ListProjectPolicies.go) -- distinct from
// every other List/Describe op in this service, which default to 100.
func TestListProjectPolicies_DefaultPageSize(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "policy-page-size-proj"})
	require.Equal(t, http.StatusOK, rec.Code)

	var projResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &projResp))
	projectARN := projResp["ProjectArn"].(string) //nolint:forcetypeassert // test

	for i := range 6 {
		rec = doRequest(t, h, "PutProjectPolicy", map[string]any{
			"ProjectArn":     projectARN,
			"PolicyName":     fmt.Sprintf("policy-%d", i),
			"PolicyDocument": `{"Version":"2012-10-17"}`,
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	}

	rec = doRequest(t, h, "ListProjectPolicies", map[string]any{"ProjectArn": projectARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	policies, _ := resp["ProjectPolicies"].([]any)
	assert.Len(t, policies, 5, "ListProjectPolicies omits MaxResults => real SDK defaults to 5 per response")
	assert.NotEmpty(t, resp["NextToken"], "a 6th policy must page off, proving the cap was applied")
}
