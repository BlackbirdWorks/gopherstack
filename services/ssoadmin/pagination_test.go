package ssoadmin_test

// Tests for NextToken pagination on SSO Admin list ops. Previously these ops
// hardcoded NextToken to null, so a client could never page past the first
// MaxResults results.

import (
	"net/http"
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
