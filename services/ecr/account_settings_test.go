package ecr_test

// account_settings_test.go — verifies account_settings.go: Put/GetAccountSetting
// round-trip and independence between distinct setting keys.

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountSetting_Put_Get_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	putRec := doAccuracy(t, h, "PutAccountSetting", map[string]any{
		"name":  "BASIC_SCAN_TYPE_VERSION",
		"value": "AWS_NATIVE",
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doAccuracy(t, h, "GetAccountSetting", map[string]any{
		"name": "BASIC_SCAN_TYPE_VERSION",
	})
	require.Equal(t, http.StatusOK, getRec.Code)
	out := parseAccuracy(t, getRec)
	assert.Equal(t, "AWS_NATIVE", out["value"])
}

func TestAccountSetting_EnhancedScanVersion(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	for _, val := range []string{"AWS_NATIVE", "CLAIR"} {
		doAccuracy(t, h, "PutAccountSetting", map[string]any{
			"name":  "BASIC_SCAN_TYPE_VERSION",
			"value": val,
		})

		getRec := doAccuracy(t, h, "GetAccountSetting", map[string]any{
			"name": "BASIC_SCAN_TYPE_VERSION",
		})
		require.Equal(t, http.StatusOK, getRec.Code)
		out := parseAccuracy(t, getRec)
		assert.Equal(t, val, out["value"], "account setting must round-trip for value %q", val)
	}
}

func TestAccountSetting_MultipleKeys_Independent(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	doAccuracy(t, h, "PutAccountSetting", map[string]any{
		"name":  "SETTING_A",
		"value": "val-a",
	})
	doAccuracy(t, h, "PutAccountSetting", map[string]any{
		"name":  "SETTING_B",
		"value": "val-b",
	})

	for name, want := range map[string]string{"SETTING_A": "val-a", "SETTING_B": "val-b"} {
		rec := doAccuracy(t, h, "GetAccountSetting", map[string]any{"name": name})
		require.Equal(t, http.StatusOK, rec.Code)
		out := parseAccuracy(t, rec)
		assert.Equal(t, want, out["value"], "setting %q must be independent", name)
	}
}

func TestPullTimeUpdateExclusion_Register_List_Deregister(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	arn := "arn:aws:iam::123456789012:role/MyRole"

	regRec := doAccuracy(t, h, "RegisterPullTimeUpdateExclusion", map[string]any{
		"principalArn": arn,
	})
	require.Equal(t, http.StatusOK, regRec.Code)
	regOut := parseAccuracy(t, regRec)
	assert.Equal(t, arn, regOut["principalArn"])
	assert.Greater(t, regOut["createdAt"].(float64), float64(0))

	listRec := doAccuracy(t, h, "ListPullTimeUpdateExclusions", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)
	listOut := parseAccuracy(t, listRec)
	exclusions, _ := listOut["pullTimeUpdateExclusions"].([]any)
	assert.Len(t, exclusions, 1)
	assert.Equal(t, arn, exclusions[0])

	deregRec := doAccuracy(t, h, "DeregisterPullTimeUpdateExclusion", map[string]any{
		"principalArn": arn,
	})
	require.Equal(t, http.StatusOK, deregRec.Code)

	listRec2 := doAccuracy(t, h, "ListPullTimeUpdateExclusions", map[string]any{})
	listOut2 := parseAccuracy(t, listRec2)
	exclusions2, _ := listOut2["pullTimeUpdateExclusions"].([]any)
	assert.Empty(t, exclusions2, "exclusion must be removed after deregister")
}

func TestPullTimeUpdateExclusion_MultipleRoles(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	arns := []string{
		"arn:aws:iam::123456789012:role/Role1",
		"arn:aws:iam::123456789012:role/Role2",
	}

	for _, arn := range arns {
		rec := doAccuracy(t, h, "RegisterPullTimeUpdateExclusion", map[string]any{
			"principalArn": arn,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doAccuracy(t, h, "ListPullTimeUpdateExclusions", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)
	out := parseAccuracy(t, listRec)
	exclusions, _ := out["pullTimeUpdateExclusions"].([]any)
	assert.Len(t, exclusions, 2)
}

func TestPullTimeUpdateExclusion_Deregister_Gone(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	arn := "arn:aws:iam::123456789012:role/TempRole"

	doAccuracy(t, h, "RegisterPullTimeUpdateExclusion", map[string]any{"principalArn": arn})

	deregRec := doAccuracy(t, h, "DeregisterPullTimeUpdateExclusion", map[string]any{
		"principalArn": arn,
	})
	require.Equal(t, http.StatusOK, deregRec.Code)

	listRec := doAccuracy(t, h, "ListPullTimeUpdateExclusions", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)
	out := parseAccuracy(t, listRec)
	exclusions, _ := out["pullTimeUpdateExclusions"].([]any)
	assert.Empty(t, exclusions)
}

// TestPullTimeUpdateExclusion_Pagination locks the real API's
// maxResults/nextToken request parameters, previously ignored (gopherstack
// always returned the full unpaginated exclusion list).
func TestPullTimeUpdateExclusion_Pagination(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	arns := []string{
		"arn:aws:iam::123456789012:role/RoleA",
		"arn:aws:iam::123456789012:role/RoleB",
		"arn:aws:iam::123456789012:role/RoleC",
	}
	for _, arn := range arns {
		rec := doAccuracy(t, h, "RegisterPullTimeUpdateExclusion", map[string]any{"principalArn": arn})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	firstRec := doAccuracy(t, h, "ListPullTimeUpdateExclusions", map[string]any{"maxResults": 1})
	require.Equal(t, http.StatusOK, firstRec.Code)
	firstOut := parseAccuracy(t, firstRec)
	firstPage, _ := firstOut["pullTimeUpdateExclusions"].([]any)
	require.Len(t, firstPage, 1, "maxResults=1 must return exactly one entry")
	nextToken, ok := firstOut["nextToken"].(string)
	require.True(t, ok, "nextToken must be present when more results remain")
	require.NotEmpty(t, nextToken)

	seen := map[string]bool{firstPage[0].(string): true}

	for range 2 {
		rec := doAccuracy(t, h, "ListPullTimeUpdateExclusions", map[string]any{
			"maxResults": 1,
			"nextToken":  nextToken,
		})
		require.Equal(t, http.StatusOK, rec.Code)
		out := parseAccuracy(t, rec)
		page, _ := out["pullTimeUpdateExclusions"].([]any)
		require.Len(t, page, 1)
		seen[page[0].(string)] = true
		nextToken, _ = out["nextToken"].(string)
	}

	assert.Len(t, seen, 3, "paginating through maxResults=1 pages must cover all 3 exclusions exactly once")
	assert.Empty(t, nextToken, "nextToken must be empty once the last page has been returned")
}
