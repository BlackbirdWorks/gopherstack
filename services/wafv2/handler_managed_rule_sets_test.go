package wafv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagedRuleSet_PutAndGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const msID = "managed-ruleset-id-001"

	// PutManagedRuleSetVersions creates the managed rule set on first call.
	putRec := doWafv2Request(t, h, "PutManagedRuleSetVersions", map[string]any{
		"Id":                 msID,
		"Name":               "my-managed-ruleset",
		"Scope":              "REGIONAL",
		"LockToken":          "",
		"RecommendedVersion": "Version_1.0",
		"VersionsToPublish": map[string]any{
			"Version_1.0": map[string]any{
				"AssociatedRuleGroupArn": "arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/my-rg/abc",
				"Capacity":               100,
			},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code, "PutManagedRuleSetVersions: %s", putRec.Body.String())

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))
	lockToken1, _ := putResp["NextLockToken"].(string)
	assert.NotEmpty(t, lockToken1, "PutManagedRuleSetVersions should return NextLockToken")

	// GetManagedRuleSet returns the stored state.
	getRec := doWafv2Request(t, h, "GetManagedRuleSet", map[string]any{
		"Id":    msID,
		"Name":  "my-managed-ruleset",
		"Scope": "REGIONAL",
	})
	require.Equal(t, http.StatusOK, getRec.Code, "GetManagedRuleSet: %s", getRec.Body.String())

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	ms := getResp["ManagedRuleSet"].(map[string]any)
	assert.Equal(t, msID, ms["Id"])
	assert.Equal(t, "my-managed-ruleset", ms["Name"])
	assert.Equal(t, "Version_1.0", ms["RecommendedVersion"])

	versions, ok := ms["PublishedVersions"].(map[string]any)
	require.True(t, ok, "PublishedVersions should be present")
	assert.Contains(t, versions, "Version_1.0", "Version_1.0 should be in PublishedVersions")

	version := versions["Version_1.0"].(map[string]any)
	assert.Equal(
		t,
		"arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/my-rg/abc",
		version["AssociatedRuleGroupArn"],
	)
}

func TestManagedRuleSet_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetManagedRuleSet", map[string]any{
		"Id":    "does-not-exist",
		"Name":  "does-not-exist",
		"Scope": "REGIONAL",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestManagedRuleSet_GetMissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetManagedRuleSet", map[string]any{
		"Scope": "REGIONAL",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFInvalidParameterException", errResp["__type"])
}

func TestManagedRuleSet_UpdateExpiryDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const msID = "ms-expiry-test"

	// Create the managed rule set with a version to expire.
	putRec := doWafv2Request(t, h, "PutManagedRuleSetVersions", map[string]any{
		"Id":                 msID,
		"Name":               "expiry-test-ruleset",
		"Scope":              "REGIONAL",
		"RecommendedVersion": "Version_1.0",
		"VersionsToPublish": map[string]any{
			"Version_1.0": map[string]any{
				"AssociatedRuleGroupArn": "arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/rg/abc",
			},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))
	lockToken := putResp["NextLockToken"].(string)

	expiry := int64(9999999999)

	// Update the expiry date for Version_1.0.
	updateRec := doWafv2Request(t, h, "UpdateManagedRuleSetVersionExpiryDate", map[string]any{
		"Id":              msID,
		"Name":            "expiry-test-ruleset",
		"Scope":           "REGIONAL",
		"LockToken":       lockToken,
		"VersionToExpire": "Version_1.0",
		"ExpiryTimestamp": expiry,
	})
	require.Equal(
		t,
		http.StatusOK,
		updateRec.Code,
		"UpdateManagedRuleSetVersionExpiryDate: %s",
		updateRec.Body.String(),
	)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	assert.NotEmpty(t, updateResp["NextLockToken"])
	assert.Equal(t, "Version_1.0", updateResp["ExpiringVersion"])
	assert.InDelta(t, float64(expiry), updateResp["ExpiryTimestamp"], 0)

	// Verify the expiry is stored: Get returns it.
	getRec := doWafv2Request(t, h, "GetManagedRuleSet", map[string]any{
		"Id":    msID,
		"Name":  "expiry-test-ruleset",
		"Scope": "REGIONAL",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	ms := getResp["ManagedRuleSet"].(map[string]any)
	versions := ms["PublishedVersions"].(map[string]any)
	v10 := versions["Version_1.0"].(map[string]any)
	assert.InDelta(t, float64(expiry), v10["ExpiryTimestamp"], 0)
}

func TestManagedRuleSet_UpdateExpiryNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Update on non-existent managed rule set.
	rec := doWafv2Request(t, h, "UpdateManagedRuleSetVersionExpiryDate", map[string]any{
		"Id":              "does-not-exist",
		"Name":            "does-not-exist",
		"Scope":           "REGIONAL",
		"LockToken":       "tok",
		"VersionToExpire": "Version_1.0",
		"ExpiryTimestamp": 9999999999,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestManagedRuleSet_UpdateExpiryVersionNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const msID = "ms-no-version"

	putRec := doWafv2Request(t, h, "PutManagedRuleSetVersions", map[string]any{
		"Id":    msID,
		"Name":  "no-version-ruleset",
		"Scope": "REGIONAL",
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))
	lockToken := putResp["NextLockToken"].(string)

	// Attempt to expire a version that was never published.
	rec := doWafv2Request(t, h, "UpdateManagedRuleSetVersionExpiryDate", map[string]any{
		"Id":              msID,
		"Name":            "no-version-ruleset",
		"Scope":           "REGIONAL",
		"LockToken":       lockToken,
		"VersionToExpire": "Version_9.9",
		"ExpiryTimestamp": 9999999999,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestManagedRuleSet_PutLockTokenEnforcement(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const msID = "ms-lock-test"

	putRec := doWafv2Request(t, h, "PutManagedRuleSetVersions", map[string]any{
		"Id":    msID,
		"Name":  "lock-test-ruleset",
		"Scope": "REGIONAL",
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	// Second put with wrong lock token should fail.
	rec := doWafv2Request(t, h, "PutManagedRuleSetVersions", map[string]any{
		"Id":        msID,
		"Name":      "lock-test-ruleset",
		"Scope":     "REGIONAL",
		"LockToken": "wrong-token",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFOptimisticLockException", errResp["__type"])
}

func TestListManagedRuleSets_ScopeFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create one REGIONAL and one CLOUDFRONT managed rule set.
	for _, tc := range []struct {
		id    string
		name  string
		scope string
	}{
		{"ms-regional", "regional-ms", "REGIONAL"},
		{"ms-cloudfront", "cloudfront-ms", "CLOUDFRONT"},
	} {
		rec := doWafv2Request(t, h, "PutManagedRuleSetVersions", map[string]any{
			"Id":    tc.id,
			"Name":  tc.name,
			"Scope": tc.scope,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Scope is required on ListManagedRuleSetsInput (api_op_ListManagedRuleSets.go); there
	// is no "list every scope" call in the real API, so REGIONAL and CLOUDFRONT are listed
	// separately and each must return only its own scope's entry.
	filtRec := doWafv2Request(t, h, "ListManagedRuleSets", map[string]any{"Scope": "REGIONAL"})
	require.Equal(t, http.StatusOK, filtRec.Code)

	var filtResp map[string]any
	require.NoError(t, json.Unmarshal(filtRec.Body.Bytes(), &filtResp))
	filt, _ := filtResp["ManagedRuleSets"].([]any)
	assert.Len(t, filt, 1)
	assert.Equal(t, "regional-ms", filt[0].(map[string]any)["Name"])

	cfRec := doWafv2Request(t, h, "ListManagedRuleSets", map[string]any{"Scope": "CLOUDFRONT"})
	require.Equal(t, http.StatusOK, cfRec.Code)

	var cfResp map[string]any
	require.NoError(t, json.Unmarshal(cfRec.Body.Bytes(), &cfResp))
	cf, _ := cfResp["ManagedRuleSets"].([]any)
	assert.Len(t, cf, 1)
	assert.Equal(t, "cloudfront-ms", cf[0].(map[string]any)["Name"])
}

func TestManagedRuleSet_RequiredFieldValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		body   map[string]any
		name   string
		target string
	}{
		{
			name:   "get missing name",
			target: "GetManagedRuleSet",
			body:   map[string]any{"Id": "ms-x", "Scope": "REGIONAL"},
		},
		{
			name:   "get missing scope",
			target: "GetManagedRuleSet",
			body:   map[string]any{"Id": "ms-x", "Name": "ms-name"},
		},
		{
			name:   "list missing scope",
			target: "ListManagedRuleSets",
			body:   map[string]any{},
		},
		{
			name:   "put missing name",
			target: "PutManagedRuleSetVersions",
			body:   map[string]any{"Id": "ms-x", "Scope": "REGIONAL"},
		},
		{
			name:   "put missing scope",
			target: "PutManagedRuleSetVersions",
			body:   map[string]any{"Id": "ms-x", "Name": "ms-name"},
		},
		{
			name:   "put invalid scope",
			target: "PutManagedRuleSetVersions",
			body:   map[string]any{"Id": "ms-x", "Name": "ms-name", "Scope": "BOGUS"},
		},
		{
			name:   "update missing name",
			target: "UpdateManagedRuleSetVersionExpiryDate",
			body: map[string]any{
				"Id": "ms-x", "Scope": "REGIONAL", "LockToken": "tok",
				"VersionToExpire": "v1", "ExpiryTimestamp": 9999999999,
			},
		},
		{
			name:   "update missing scope",
			target: "UpdateManagedRuleSetVersionExpiryDate",
			body: map[string]any{
				"Id": "ms-x", "Name": "ms-name", "LockToken": "tok",
				"VersionToExpire": "v1", "ExpiryTimestamp": 9999999999,
			},
		},
		{
			name:   "update invalid scope",
			target: "UpdateManagedRuleSetVersionExpiryDate",
			body: map[string]any{
				"Id": "ms-x", "Name": "ms-name", "Scope": "BOGUS", "LockToken": "tok",
				"VersionToExpire": "v1", "ExpiryTimestamp": 9999999999,
			},
		},
		{
			name:   "update missing lock token",
			target: "UpdateManagedRuleSetVersionExpiryDate",
			body: map[string]any{
				"Id": "ms-x", "Name": "ms-name", "Scope": "REGIONAL",
				"VersionToExpire": "v1", "ExpiryTimestamp": 9999999999,
			},
		},
		{
			name:   "update missing version to expire",
			target: "UpdateManagedRuleSetVersionExpiryDate",
			body: map[string]any{
				"Id": "ms-x", "Name": "ms-name", "Scope": "REGIONAL", "LockToken": "tok",
				"ExpiryTimestamp": 9999999999,
			},
		},
		{
			name:   "update missing expiry timestamp",
			target: "UpdateManagedRuleSetVersionExpiryDate",
			body: map[string]any{
				"Id": "ms-x", "Name": "ms-name", "Scope": "REGIONAL", "LockToken": "tok",
				"VersionToExpire": "v1",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, tc.target, tc.body)

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "WAFInvalidParameterException", errResp["__type"])
		})
	}
}

// ---- Mobile SDK release catalog ---------------------------------------------

// TestListManagedRuleSets_DuplicateNamePagination proves that
// handleListManagedRuleSets can drop a record when two ManagedRuleSets share
// a Name: PutManagedRuleSetVersions keys strictly on the caller-supplied Id
// (managed_rule_sets.go's PutManagedRuleSetVersions has no "name already
// exists" check, unlike CreateWebACL/CreateIPSet/CreateRegexPatternSet/
// CreateRuleGroup's webACLsByNameScope-style dedup), so Name is not a total
// order the way it is for those four families. handleListManagedRuleSets
// still paginates with paginateByName -- an equality/marker cursor that
// skips every item whose name is <= the marker (handler.go's
// skipToLoggingConfigMarker sibling) -- so once a page boundary falls inside
// a same-name tie group, every remaining item in that tie group is skipped
// on the next page, deterministically, not just map-order-dependently: one
// walk is enough to prove it (see CLAUDE.md's pagination-audit "HOW TO
// PROVE A BUG" note on marker cursors).
func TestListManagedRuleSets_DuplicateNamePagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const dupName = "dup-managed-ruleset"

	for _, id := range []string{"ms-1", "ms-2", "ms-3"} {
		rec := doWafv2Request(t, h, "PutManagedRuleSetVersions", map[string]any{
			"Id":    id,
			"Name":  dupName,
			"Scope": "REGIONAL",
		})
		require.Equal(t, http.StatusOK, rec.Code, "PutManagedRuleSetVersions(%s): %s", id, rec.Body.String())
	}

	seen := map[string]bool{}
	nextMarker := ""

	for range 5 {
		req := map[string]any{"Scope": "REGIONAL", "Limit": 2}
		if nextMarker != "" {
			req["NextMarker"] = nextMarker
		}

		rec := doWafv2Request(t, h, "ListManagedRuleSets", req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		items, _ := resp["ManagedRuleSets"].([]any)
		for _, item := range items {
			id, _ := item.(map[string]any)["Id"].(string)
			seen[id] = true
		}

		nextMarker, _ = resp["NextMarker"].(string)
		if nextMarker == "" {
			break
		}
	}

	assert.ElementsMatch(
		t, []string{"ms-1", "ms-2", "ms-3"}, mapKeys(seen),
		"paginating through all pages must reproduce every ManagedRuleSet exactly once, "+
			"even when several share a Name",
	)
}

func mapKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}
