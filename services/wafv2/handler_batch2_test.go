package wafv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- ManagedRuleSet: full lifecycle ------------------------------------------

func TestBatch2_ManagedRuleSet_PutAndGet(t *testing.T) {
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
	assert.Equal(t,
		"arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/my-rg/abc",
		version["AssociatedRuleGroupArn"],
	)
}

func TestBatch2_ManagedRuleSet_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetManagedRuleSet", map[string]any{
		"Id":    "does-not-exist",
		"Scope": "REGIONAL",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestBatch2_ManagedRuleSet_GetMissingID(t *testing.T) {
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

func TestBatch2_ManagedRuleSet_UpdateExpiryDate(t *testing.T) {
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

func TestBatch2_ManagedRuleSet_UpdateExpiryNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Update on non-existent managed rule set.
	rec := doWafv2Request(t, h, "UpdateManagedRuleSetVersionExpiryDate", map[string]any{
		"Id":              "does-not-exist",
		"Scope":           "REGIONAL",
		"VersionToExpire": "Version_1.0",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestBatch2_ManagedRuleSet_UpdateExpiryVersionNotFound(t *testing.T) {
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
		"LockToken":       lockToken,
		"VersionToExpire": "Version_9.9",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestBatch2_ManagedRuleSet_PutLockTokenEnforcement(t *testing.T) {
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

func TestBatch2_ListManagedRuleSets_ScopeFilter(t *testing.T) {
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

	// List all — should return 2.
	allRec := doWafv2Request(t, h, "ListManagedRuleSets", map[string]any{})
	require.Equal(t, http.StatusOK, allRec.Code)

	var allResp map[string]any
	require.NoError(t, json.Unmarshal(allRec.Body.Bytes(), &allResp))
	all, _ := allResp["ManagedRuleSets"].([]any)
	assert.Len(t, all, 2)

	// Filter by REGIONAL — should return 1.
	filtRec := doWafv2Request(t, h, "ListManagedRuleSets", map[string]any{"Scope": "REGIONAL"})
	require.Equal(t, http.StatusOK, filtRec.Code)

	var filtResp map[string]any
	require.NoError(t, json.Unmarshal(filtRec.Body.Bytes(), &filtResp))
	filt, _ := filtResp["ManagedRuleSets"].([]any)
	assert.Len(t, filt, 1)
	assert.Equal(t, "regional-ms", filt[0].(map[string]any)["Name"])
}

// ---- Mobile SDK release catalog ---------------------------------------------

func TestBatch2_GetMobileSdkRelease_KnownVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetMobileSdkRelease", map[string]any{
		"Platform":       "Android",
		"ReleaseVersion": "3.1.0",
	})
	require.Equal(t, http.StatusOK, rec.Code, "GetMobileSdkRelease Android/3.1.0: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	release := resp["MobileSdkRelease"].(map[string]any)
	assert.Equal(t, "3.1.0", release["ReleaseVersion"])
	assert.NotEmpty(t, release["ReleaseNotes"])
	assert.NotNil(t, release["Timestamp"])
	_, tsOk := release["Timestamp"].(float64)
	assert.True(t, tsOk, "Timestamp should be a number")
}

func TestBatch2_GetMobileSdkRelease_IOSVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetMobileSdkRelease", map[string]any{
		"Platform":       "iOS",
		"ReleaseVersion": "3.0.0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	release := resp["MobileSdkRelease"].(map[string]any)
	assert.Equal(t, "3.0.0", release["ReleaseVersion"])
}

func TestBatch2_GetMobileSdkRelease_UnknownVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetMobileSdkRelease", map[string]any{
		"Platform":       "Android",
		"ReleaseVersion": "99.0.0",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestBatch2_GetMobileSdkRelease_UnknownPlatform(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetMobileSdkRelease", map[string]any{
		"Platform":       "WindowsPhone",
		"ReleaseVersion": "3.1.0",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch2_GetMobileSdkRelease_MissingPlatform(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetMobileSdkRelease", map[string]any{
		"ReleaseVersion": "3.1.0",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFInvalidParameterException", errResp["__type"])
}

func TestBatch2_ListMobileSdkReleases_Android(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "ListMobileSdkReleases", map[string]any{
		"Platform": "Android",
		"Scope":    "REGIONAL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	releases, ok := resp["ReleaseSummaries"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, releases, "should return at least one Android release")

	for _, r := range releases {
		rm := r.(map[string]any)
		assert.NotEmpty(t, rm["ReleaseVersion"])
		assert.NotNil(t, rm["Timestamp"])
	}
}

func TestBatch2_ListMobileSdkReleases_iOS(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "ListMobileSdkReleases", map[string]any{
		"Platform": "iOS",
		"Scope":    "REGIONAL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	releases, _ := resp["ReleaseSummaries"].([]any)
	assert.NotEmpty(t, releases, "should return at least one iOS release")
}

func TestBatch2_ListMobileSdkReleases_UnknownPlatform(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "ListMobileSdkReleases", map[string]any{
		"Platform": "Blackberry",
		"Scope":    "REGIONAL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	releases, _ := resp["ReleaseSummaries"].([]any)
	assert.Empty(t, releases, "unknown platform should return empty list")
}

func TestBatch2_GenerateMobileSdkReleaseUrl_Shape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GenerateMobileSdkReleaseUrl", map[string]any{
		"Platform":       "Android",
		"ReleaseVersion": "3.1.0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	url, ok := resp["Url"].(string)
	require.True(t, ok, "Url field should be present")
	assert.NotEmpty(t, url)
}

// ---- Logging configuration: all 3 destination types -------------------------

func TestBatch2_LoggingConfig_FirehoseDestination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, webACLARN := createWebACLHelper(t, h, "log-firehose-acl", "REGIONAL")

	rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": map[string]any{
			"ResourceArn":           webACLARN,
			"LogDestinationConfigs": []string{"arn:aws:firehose:us-east-1:000000000000:deliverystream/waf-logs"},
			"RedactedFields":        []any{},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "Firehose destination: %s", rec.Body.String())

	getRec := doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	cfg := resp["LoggingConfiguration"].(map[string]any)
	dests := cfg["LogDestinationConfigs"].([]any)
	require.Len(t, dests, 1)
	assert.Contains(t, dests[0].(string), "arn:aws:firehose:")
}

func TestBatch2_LoggingConfig_S3Destination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, webACLARN := createWebACLHelper(t, h, "log-s3-acl", "REGIONAL")

	rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": map[string]any{
			"ResourceArn":           webACLARN,
			"LogDestinationConfigs": []string{"arn:aws:s3:::my-waf-log-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "S3 destination: %s", rec.Body.String())

	getRec := doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	cfg := resp["LoggingConfiguration"].(map[string]any)
	dests := cfg["LogDestinationConfigs"].([]any)
	require.Len(t, dests, 1)
	assert.Contains(t, dests[0].(string), "arn:aws:s3:::")
}

func TestBatch2_LoggingConfig_CloudWatchDestination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, webACLARN := createWebACLHelper(t, h, "log-cw-acl", "REGIONAL")

	rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": map[string]any{
			"ResourceArn":           webACLARN,
			"LogDestinationConfigs": []string{"arn:aws:logs:us-east-1:000000000000:log-group:aws-waf-logs-test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "CloudWatch destination: %s", rec.Body.String())

	getRec := doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	cfg := resp["LoggingConfiguration"].(map[string]any)
	dests := cfg["LogDestinationConfigs"].([]any)
	require.Len(t, dests, 1)
	assert.Contains(t, dests[0].(string), "arn:aws:logs:")
}

func TestBatch2_LoggingConfig_InvalidDestinationRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dest string
	}{
		{name: "dynamodb", dest: "arn:aws:dynamodb:us-east-1:000000000000:table/WafLogs"},
		{name: "sns", dest: "arn:aws:sns:us-east-1:000000000000:waf-alerts"},
		{name: "sqs", dest: "arn:aws:sqs:us-east-1:000000000000:waf-queue"},
		{name: "plain_string", dest: "not-an-arn"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, webACLARN := createWebACLHelper(t, h, "log-invalid-acl-"+tt.name, "REGIONAL")

			rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
				"LoggingConfiguration": map[string]any{
					"ResourceArn":           webACLARN,
					"LogDestinationConfigs": []string{tt.dest},
				},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code, "destination %q should be rejected", tt.dest)
		})
	}
}

func TestBatch2_LoggingConfig_DeleteAndGetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, webACLARN := createWebACLHelper(t, h, "log-delete-acl", "REGIONAL")

	// Put a config.
	rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": map[string]any{
			"ResourceArn":           webACLARN,
			"LogDestinationConfigs": []string{"arn:aws:s3:::my-log-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete it.
	delRec := doWafv2Request(t, h, "DeleteLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Subsequent Get should return not-found.
	getRec := doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	assert.Equal(t, http.StatusBadRequest, getRec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestBatch2_LoggingConfig_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "DeleteLoggingConfiguration", map[string]any{
		"ResourceArn": "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/no-such/xyz",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestBatch2_LoggingConfig_FullFieldRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, webACLARN := createWebACLHelper(t, h, "log-full-acl", "REGIONAL")

	loggingConfig := map[string]any{
		"ResourceArn":           webACLARN,
		"LogDestinationConfigs": []string{"arn:aws:firehose:us-east-1:000000000000:deliverystream/waf-stream"},
		"RedactedFields": []map[string]any{
			{
				"SingleHeader": map[string]any{"Name": "authorization"},
			},
		},
		"LoggingFilter": map[string]any{
			"DefaultBehavior": "KEEP",
			"Filters": []map[string]any{
				{
					"Behavior":    "DROP",
					"Requirement": "MEETS_ANY",
					"Conditions": []map[string]any{
						{
							"ActionCondition": map[string]any{"Action": "BLOCK"},
						},
					},
				},
			},
		},
	}

	putRec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": loggingConfig,
	})
	require.Equal(t, http.StatusOK, putRec.Code, "full field put: %s", putRec.Body.String())

	getRec := doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	cfg := resp["LoggingConfiguration"].(map[string]any)

	// RedactedFields should round-trip.
	redacted, ok := cfg["RedactedFields"].([]any)
	require.True(t, ok, "RedactedFields should be present")
	assert.Len(t, redacted, 1)

	// LoggingFilter should round-trip.
	filter, ok := cfg["LoggingFilter"].(map[string]any)
	require.True(t, ok, "LoggingFilter should be present")
	assert.Equal(t, "KEEP", filter["DefaultBehavior"])
}

// ---- Permission policy lifecycle --------------------------------------------

func TestBatch2_PermissionPolicy_PutGetDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, rgARN := createRuleGroupHelper(t, h, "policy-rg")

	policy := `{"Version":"2012-10-17","Statement":[{` +
		`"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::111122223333:root"},` +
		`"Action":"wafv2:CreateWebACL","Resource":"` + rgARN + `"}]}`

	// Put permission policy.
	putRec := doWafv2Request(t, h, "PutPermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
		"Policy":      policy,
	})
	require.Equal(t, http.StatusOK, putRec.Code, "PutPermissionPolicy: %s", putRec.Body.String())

	// Get permission policy.
	getRec := doWafv2Request(t, h, "GetPermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code, "GetPermissionPolicy: %s", getRec.Body.String())

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	storedPolicy, ok := getResp["Policy"].(string)
	require.True(t, ok, "Policy field should be present")
	assert.Equal(t, policy, storedPolicy)

	// Delete permission policy.
	delRec := doWafv2Request(t, h, "DeletePermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Subsequent Get should return not-found.
	getRec2 := doWafv2Request(t, h, "GetPermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
	})
	assert.Equal(t, http.StatusBadRequest, getRec2.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestBatch2_PermissionPolicy_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "DeletePermissionPolicy", map[string]any{
		"ResourceArn": "arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/no-such/xyz",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestBatch2_PermissionPolicy_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetPermissionPolicy", map[string]any{
		"ResourceArn": "arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/no-such/xyz",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch2_PermissionPolicy_UpdateReplace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, rgARN := createRuleGroupHelper(t, h, "policy-replace-rg")

	policy1 := `{"Version":"2012-10-17","Statement":[]}`
	policy2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"wafv2:CreateWebACL","Resource":"*"}]}`

	// Put first policy.
	rec := doWafv2Request(t, h, "PutPermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
		"Policy":      policy1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Put second policy — should replace.
	rec = doWafv2Request(t, h, "PutPermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
		"Policy":      policy2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	getRec := doWafv2Request(t, h, "GetPermissionPolicy", map[string]any{
		"ResourceArn": rgARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	assert.Equal(t, policy2, resp["Policy"])
}

// ---- Rate-based rules with scope-down ----------------------------------------

func TestBatch2_RateBasedStatement_WithScopeDown(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-rate-scope-down",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-rate-scope-down",
		},
		"Rules": []map[string]any{
			{
				"Name":     "rate-rule",
				"Priority": 1,
				"Statement": map[string]any{
					"RateBasedStatement": map[string]any{
						"Limit":               500,
						"AggregateKeyType":    "IP",
						"EvaluationWindowSec": 300,
						"ScopeDownStatement": map[string]any{
							"GeoMatchStatement": map[string]any{
								"CountryCodes": []string{"US", "CA"},
							},
						},
					},
				},
				"Action": map[string]any{"Block": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"MetricName": "rate-rule",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "RateBasedStatement with ScopeDown: %s", rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	// Retrieve and verify the rule is stored with ScopeDownStatement.
	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	rules := getResp["WebACL"].(map[string]any)["Rules"].([]any)
	require.Len(t, rules, 1)

	stmt := rules[0].(map[string]any)["Statement"].(map[string]any)
	rbs, ok := stmt["RateBasedStatement"].(map[string]any)
	require.True(t, ok, "RateBasedStatement should be present")
	_, hasScopeDown := rbs["ScopeDownStatement"]
	assert.True(t, hasScopeDown, "ScopeDownStatement should round-trip")
}

func TestBatch2_RateBasedStatement_AllValidEvaluationWindows(t *testing.T) {
	t.Parallel()

	validWindows := []int{60, 120, 300, 600, 1800, 3600, 7200, 21600}

	for _, window := range validWindows {
		t.Run(string(rune('0'+window/100)), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
				"Name":          "acl-ew",
				"Scope":         "REGIONAL",
				"DefaultAction": map[string]any{"Allow": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"MetricName": "acl-ew",
				},
				"Rules": []map[string]any{
					{
						"Name":     "r",
						"Priority": 1,
						"Statement": map[string]any{
							"RateBasedStatement": map[string]any{
								"Limit":               100,
								"AggregateKeyType":    "IP",
								"EvaluationWindowSec": window,
							},
						},
						"Action":           map[string]any{"Block": map[string]any{}},
						"VisibilityConfig": map[string]any{"MetricName": "r"},
					},
				},
			})
			assert.Equal(t, http.StatusOK, rec.Code,
				"EvaluationWindowSec=%d should be accepted: %s", window, rec.Body.String())
		})
	}
}

func TestBatch2_RateBasedStatement_InvalidEvaluationWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		window int
	}{
		{name: "zero", window: 0},
		{name: "one", window: 1},
		{name: "59", window: 59},
		{name: "61", window: 61},
		{name: "3601", window: 3601},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
				"Name":          "acl-ew-bad",
				"Scope":         "REGIONAL",
				"DefaultAction": map[string]any{"Allow": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"MetricName": "acl-ew-bad",
				},
				"Rules": []map[string]any{
					{
						"Name":     "r",
						"Priority": 1,
						"Statement": map[string]any{
							"RateBasedStatement": map[string]any{
								"Limit":               100,
								"AggregateKeyType":    "IP",
								"EvaluationWindowSec": tt.window,
							},
						},
						"Action":           map[string]any{"Block": map[string]any{}},
						"VisibilityConfig": map[string]any{"MetricName": "r"},
					},
				},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code,
				"EvaluationWindowSec=%d should be rejected", tt.window)
		})
	}
}

func TestBatch2_RateBasedStatement_LimitBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		limit      int
		wantStatus int
	}{
		{name: "min_valid", limit: 100, wantStatus: http.StatusOK},
		{name: "below_min", limit: 99, wantStatus: http.StatusBadRequest},
		{name: "max_valid", limit: 2_000_000_000, wantStatus: http.StatusOK},
		{name: "above_max", limit: 2_000_000_001, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
				"Name":          "acl-limit",
				"Scope":         "REGIONAL",
				"DefaultAction": map[string]any{"Allow": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"MetricName": "acl-limit",
				},
				"Rules": []map[string]any{
					{
						"Name":     "r",
						"Priority": 1,
						"Statement": map[string]any{
							"RateBasedStatement": map[string]any{
								"Limit":            tt.limit,
								"AggregateKeyType": "IP",
							},
						},
						"Action":           map[string]any{"Block": map[string]any{}},
						"VisibilityConfig": map[string]any{"MetricName": "r"},
					},
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code,
				"limit=%d expected=%d: %s", tt.limit, tt.wantStatus, rec.Body.String())
		})
	}
}

// ---- Captcha and Challenge actions ------------------------------------------

func TestBatch2_CaptchaAction_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-captcha",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-captcha",
		},
		"Rules": []map[string]any{
			{
				"Name":     "captcha-rule",
				"Priority": 1,
				"Statement": map[string]any{
					"GeoMatchStatement": map[string]any{
						"CountryCodes": []string{"RU"},
					},
				},
				"Action": map[string]any{
					"Captcha": map[string]any{
						"CustomRequestHandling": map[string]any{
							"InsertHeaders": []map[string]any{
								{"Name": "x-captcha", "Value": "required"},
							},
						},
					},
				},
				"VisibilityConfig": map[string]any{"MetricName": "captcha-rule"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "Captcha action: %s", rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	// Verify round-trip.
	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	rules := getResp["WebACL"].(map[string]any)["Rules"].([]any)
	require.Len(t, rules, 1)

	action := rules[0].(map[string]any)["Action"].(map[string]any)
	_, hasCaptcha := action["Captcha"]
	assert.True(t, hasCaptcha, "Captcha action should round-trip")
}

func TestBatch2_ChallengeAction_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-challenge",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-challenge",
		},
		"Rules": []map[string]any{
			{
				"Name":     "challenge-rule",
				"Priority": 1,
				"Statement": map[string]any{
					"GeoMatchStatement": map[string]any{
						"CountryCodes": []string{"CN"},
					},
				},
				"Action": map[string]any{
					"Challenge": map[string]any{},
				},
				"VisibilityConfig": map[string]any{"MetricName": "challenge-rule"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "Challenge action: %s", rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	rules := getResp["WebACL"].(map[string]any)["Rules"].([]any)
	require.Len(t, rules, 1)

	action := rules[0].(map[string]any)["Action"].(map[string]any)
	_, hasChallenge := action["Challenge"]
	assert.True(t, hasChallenge, "Challenge action should round-trip")
}

func TestBatch2_ChallengeConfig_WebACLLevel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-challenge-config",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-challenge-config",
		},
		"ChallengeConfig": map[string]any{
			"ImmunityTimeProperty": map[string]any{
				"ImmunityTime": 600,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	webACL := getResp["WebACL"].(map[string]any)

	challengeCfg, ok := webACL["ChallengeConfig"].(map[string]any)
	require.True(t, ok, "ChallengeConfig should be present in response")
	immunityTime := challengeCfg["ImmunityTimeProperty"].(map[string]any)
	assert.InDelta(t, float64(600), immunityTime["ImmunityTime"], 0)
}

// ---- LabelMatchStatement ----------------------------------------------------

func TestBatch2_LabelMatchStatement_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-label-match",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-label-match",
		},
		"Rules": []map[string]any{
			{
				"Name":     "label-rule",
				"Priority": 1,
				"Statement": map[string]any{
					"LabelMatchStatement": map[string]any{
						"Scope": "LABEL",
						"Key":   "awswaf:managed:aws:core-rule-set:NoUserAgent_Header",
					},
				},
				"Action":           map[string]any{"Block": map[string]any{}},
				"VisibilityConfig": map[string]any{"MetricName": "label-rule"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "LabelMatchStatement: %s", rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	rules := getResp["WebACL"].(map[string]any)["Rules"].([]any)
	require.Len(t, rules, 1)

	stmt := rules[0].(map[string]any)["Statement"].(map[string]any)
	lms, ok := stmt["LabelMatchStatement"].(map[string]any)
	require.True(t, ok, "LabelMatchStatement should round-trip")
	assert.Equal(t, "LABEL", lms["Scope"])
	assert.Equal(t, "awswaf:managed:aws:core-rule-set:NoUserAgent_Header", lms["Key"])
}

func TestBatch2_LabelMatchStatement_NamespaceScope(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-label-ns",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-label-ns",
		},
		"Rules": []map[string]any{
			{
				"Name":     "ns-rule",
				"Priority": 1,
				"Statement": map[string]any{
					"LabelMatchStatement": map[string]any{
						"Scope": "NAMESPACE",
						"Key":   "awswaf:managed:aws:",
					},
				},
				"Action":           map[string]any{"Count": map[string]any{}},
				"VisibilityConfig": map[string]any{"MetricName": "ns-rule"},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code, "LabelMatchStatement NAMESPACE scope: %s", rec.Body.String())
}

// ---- AssociationConfig decryption parameters --------------------------------

func TestBatch2_AssociationConfig_RequestBodyDecryption(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-assoc-config",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-assoc-config",
		},
		"AssociationConfig": map[string]any{
			"RequestBody": map[string]any{
				"APPLICATION_LOAD_BALANCER": map[string]any{
					"DefaultSizeInspectionLimit": "KB_8",
					"OversizeHandling":           "CONTINUE",
				},
				"API_GATEWAY": map[string]any{
					"DefaultSizeInspectionLimit": "KB_64",
					"OversizeHandling":           "MATCH",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "AssociationConfig: %s", rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	webACL := getResp["WebACL"].(map[string]any)

	assocCfg, ok := webACL["AssociationConfig"].(map[string]any)
	require.True(t, ok, "AssociationConfig should be present in response")

	requestBody, ok := assocCfg["RequestBody"].(map[string]any)
	require.True(t, ok, "RequestBody should be present in AssociationConfig")

	alb, ok := requestBody["APPLICATION_LOAD_BALANCER"].(map[string]any)
	require.True(t, ok, "APPLICATION_LOAD_BALANCER config should be present")
	assert.Equal(t, "KB_8", alb["DefaultSizeInspectionLimit"])
	assert.Equal(t, "CONTINUE", alb["OversizeHandling"])
}

// ---- ManagedRuleGroupStatement in WebACL ------------------------------------

func TestBatch2_WebACL_ManagedRuleGroupStatement_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-mrg",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-mrg",
		},
		"Rules": []map[string]any{
			{
				"Name":     "aws-common",
				"Priority": 1,
				"Statement": map[string]any{
					"ManagedRuleGroupStatement": map[string]any{
						"VendorName": "AWS",
						"Name":       "AWSManagedRulesCommonRuleSet",
						"ExcludedRules": []map[string]any{
							{"Name": "SizeRestrictions_BODY"},
						},
					},
				},
				"OverrideAction": map[string]any{"None": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"MetricName": "aws-common",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "ManagedRuleGroupStatement: %s", rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	rules := getResp["WebACL"].(map[string]any)["Rules"].([]any)
	require.Len(t, rules, 1)

	stmt := rules[0].(map[string]any)["Statement"].(map[string]any)
	mrg, ok := stmt["ManagedRuleGroupStatement"].(map[string]any)
	require.True(t, ok, "ManagedRuleGroupStatement should round-trip")
	assert.Equal(t, "AWS", mrg["VendorName"])
	assert.Equal(t, "AWSManagedRulesCommonRuleSet", mrg["Name"])
}

// ---- RuleGroup: update rules, OverrideAction variants -----------------------

func TestBatch2_RuleGroup_UpdateRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doWafv2Request(t, h, "CreateRuleGroup", map[string]any{
		"Name":     "updatable-rg",
		"Scope":    "REGIONAL",
		"Capacity": 50,
		"Rules": []map[string]any{
			{
				"Name":     "r1",
				"Priority": 1,
				"Statement": map[string]any{
					"IPSetReferenceStatement": map[string]any{
						"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/x/abc",
					},
				},
				"Action":           map[string]any{"Allow": map[string]any{}},
				"VisibilityConfig": map[string]any{"MetricName": "r1"},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	rgID := createResp["Summary"].(map[string]any)["Id"].(string)
	rgLock := createResp["Summary"].(map[string]any)["LockToken"].(string)

	// Update with 2 rules.
	updateRec := doWafv2Request(t, h, "UpdateRuleGroup", map[string]any{
		"Id":        rgID,
		"LockToken": rgLock,
		"Rules": []map[string]any{
			{
				"Name":     "r1",
				"Priority": 1,
				"Statement": map[string]any{
					"IPSetReferenceStatement": map[string]any{
						"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/x/abc",
					},
				},
				"Action":           map[string]any{"Block": map[string]any{}},
				"VisibilityConfig": map[string]any{"MetricName": "r1"},
			},
			{
				"Name":     "r2",
				"Priority": 2,
				"Statement": map[string]any{
					"IPSetReferenceStatement": map[string]any{
						"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/y/def",
					},
				},
				"Action":           map[string]any{"Count": map[string]any{}},
				"VisibilityConfig": map[string]any{"MetricName": "r2"},
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code, "UpdateRuleGroup: %s", updateRec.Body.String())

	// Verify 2 rules stored.
	getRec := doWafv2Request(t, h, "GetRuleGroup", map[string]any{"Id": rgID, "Scope": "REGIONAL"})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	rules := getResp["RuleGroup"].(map[string]any)["Rules"].([]any)
	assert.Len(t, rules, 2)
}

func TestBatch2_WebACL_OverrideAction_None(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, rgARN := createRuleGroupHelper(t, h, "override-none-rg")

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-override-none",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-override-none",
		},
		"Rules": []map[string]any{
			{
				"Name":     "rg-rule",
				"Priority": 1,
				"Statement": map[string]any{
					"RuleGroupReferenceStatement": map[string]any{
						"ARN": rgARN,
					},
				},
				"OverrideAction": map[string]any{"None": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"MetricName": "rg-rule",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "OverrideAction None: %s", rec.Body.String())
}

func TestBatch2_WebACL_OverrideAction_Count(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, rgARN := createRuleGroupHelper(t, h, "override-count-rg")

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-override-count",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-override-count",
		},
		"Rules": []map[string]any{
			{
				"Name":     "rg-count-rule",
				"Priority": 1,
				"Statement": map[string]any{
					"RuleGroupReferenceStatement": map[string]any{
						"ARN": rgARN,
					},
				},
				"OverrideAction": map[string]any{"Count": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"MetricName": "rg-count-rule",
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code, "OverrideAction Count: %s", rec.Body.String())
}

// ---- IP set limit enforcement -----------------------------------------------

func TestBatch2_IPSet_MaxEntriesRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Build 10001 unique /32 CIDRs (more than the 10000 limit).
	addrs := make([]string, 10001)
	for i := range addrs {
		addrs[i] = "10." +
			string(rune('0'+i/1000000%10)) +
			"." + string(rune('0'+i/1000%10)) +
			"." + string(rune('0'+i%1000/10)) + "/32"
	}

	// Reset to proper CIDR format.
	addrs = make([]string, 10001)
	for i := range addrs {
		a := i / (256 * 256)
		b := (i / 256) % 256
		c := i % 256
		addrs[i] = "10." + itoa(a) + "." + itoa(b) + "." + itoa(c) + "/32"
	}

	rec := doWafv2Request(t, h, "CreateIPSet", map[string]any{
		"Name":             "too-many-ips",
		"Scope":            "REGIONAL",
		"IPAddressVersion": "IPV4",
		"Addresses":        addrs,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "10001 IPs should exceed limit")

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFLimitsExceededException", errResp["__type"])
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	digits := make([]byte, 0, 3)

	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}

	return string(digits)
}

func TestBatch2_IPSet_NearMaxEntries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Exactly 10000 entries should be accepted.
	addrs := make([]string, 10000)
	for i := range addrs {
		a := i / (256 * 256)
		b := (i / 256) % 256
		c := i % 256
		addrs[i] = "10." + itoa(a) + "." + itoa(b) + "." + itoa(c) + "/32"
	}

	rec := doWafv2Request(t, h, "CreateIPSet", map[string]any{
		"Name":             "max-ips",
		"Scope":            "REGIONAL",
		"IPAddressVersion": "IPV4",
		"Addresses":        addrs,
	})
	assert.Equal(t, http.StatusOK, rec.Code, "exactly 10000 IPs should be accepted: %s", rec.Body.String())
}

// ---- Regex pattern set: max entries enforcement -----------------------------

func TestBatch2_RegexPatternSet_MaxEntries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// 11 entries — exceeds the max of 10.
	entries := make([]map[string]any, 11)
	for i := range entries {
		entries[i] = map[string]any{"RegexString": "^foo" + itoa(i)}
	}

	rec := doWafv2Request(t, h, "CreateRegexPatternSet", map[string]any{
		"Name":                  "too-many-regex",
		"Scope":                 "REGIONAL",
		"RegularExpressionList": entries,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "11 regex entries should exceed limit")

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFLimitsExceededException", errResp["__type"])
}

func TestBatch2_RegexPatternSet_ExactlyMaxEntries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Exactly 10 entries — should be accepted.
	entries := make([]map[string]any, 10)
	for i := range entries {
		entries[i] = map[string]any{"RegexString": "^pattern" + itoa(i)}
	}

	rec := doWafv2Request(t, h, "CreateRegexPatternSet", map[string]any{
		"Name":                  "exact-max-regex",
		"Scope":                 "REGIONAL",
		"RegularExpressionList": entries,
	})
	assert.Equal(t, http.StatusOK, rec.Code, "exactly 10 regex entries should be accepted: %s", rec.Body.String())
}

// ---- WebACL CustomResponseBodies round-trip ---------------------------------

func TestBatch2_WebACL_CustomResponseBodies_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-custom-resp",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-custom-resp",
		},
		"CustomResponseBodies": map[string]any{
			"BlockPage": map[string]any{
				"ContentType": "TEXT_HTML",
				"Content":     "<html><body>Access Denied</body></html>",
			},
		},
		"Rules": []map[string]any{
			{
				"Name":     "block-rule",
				"Priority": 1,
				"Statement": map[string]any{
					"IPSetReferenceStatement": map[string]any{
						"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/x/abc",
					},
				},
				"Action": map[string]any{
					"Block": map[string]any{
						"CustomResponse": map[string]any{
							"ResponseCode":          403,
							"CustomResponseBodyKey": "BlockPage",
						},
					},
				},
				"VisibilityConfig": map[string]any{"MetricName": "block-rule"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "CustomResponseBodies: %s", rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	webACL := getResp["WebACL"].(map[string]any)

	crb, ok := webACL["CustomResponseBodies"].(map[string]any)
	require.True(t, ok, "CustomResponseBodies should be present in response")
	assert.Contains(t, crb, "BlockPage")
}

// ---- ListResourcesForWebACL -------------------------------------------------

func TestBatch2_ListResourcesForWebACL_MultipleResources(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, webACLARN := createWebACLHelper(t, h, "list-resources-acl", "REGIONAL")

	resources := []string{
		"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/lb-a/abc",
		"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/lb-b/def",
		"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/lb-c/ghi",
	}

	for _, r := range resources {
		rec := doWafv2Request(t, h, "AssociateWebACL", map[string]any{
			"WebACLArn":   webACLARN,
			"ResourceArn": r,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doWafv2Request(t, h, "ListResourcesForWebACL", map[string]any{
		"WebACLArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	arns, _ := resp["ResourceArns"].([]any)
	assert.Len(t, arns, 3, "should list 3 associated resources")

	// Verify all expected ARNs are present.
	arnSet := make(map[string]bool)
	for _, a := range arns {
		arnSet[a.(string)] = true
	}

	for _, expected := range resources {
		assert.True(t, arnSet[expected], "resource %q should be in list", expected)
	}
}

// ---- GetRateBasedStatementManagedKeys ---------------------------------------

func TestBatch2_GetRateBasedStatementManagedKeys_Valid(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id, _ := createWebACLHelper(t, h, "rate-keys-acl", "REGIONAL")

	rec := doWafv2Request(t, h, "GetRateBasedStatementManagedKeys", map[string]any{
		"Scope":      "REGIONAL",
		"WebACLName": "rate-keys-acl",
		"WebACLId":   id,
		"RuleName":   "my-rate-rule",
	})
	require.Equal(t, http.StatusOK, rec.Code, "GetRateBasedStatementManagedKeys: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	ipv4, ok := resp["ManagedKeysIPV4"].(map[string]any)
	require.True(t, ok, "ManagedKeysIPV4 should be present")
	assert.Equal(t, "IPV4", ipv4["IPAddressVersion"])
	_, hasIPv4Addrs := ipv4["Addresses"]
	assert.True(t, hasIPv4Addrs)

	ipv6, ok := resp["ManagedKeysIPV6"].(map[string]any)
	require.True(t, ok, "ManagedKeysIPV6 should be present")
	assert.Equal(t, "IPV6", ipv6["IPAddressVersion"])
}

func TestBatch2_GetRateBasedStatementManagedKeys_MissingScope(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetRateBasedStatementManagedKeys", map[string]any{
		"WebACLId": "some-id",
		"RuleName": "some-rule",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- Snapshot / Restore preserves managed rule sets -------------------------

func TestBatch2_Snapshot_IncludesManagedRuleSets(t *testing.T) {
	t.Parallel()

	b1 := newTestHandler(t).Backend

	// Populate managed rule set in b1.
	_, err := b1.PutManagedRuleSetVersions(
		"snap-ms-001", "snap-ruleset", "REGIONAL", "", "Version_2.0",
		map[string]any{
			"Version_2.0": map[string]any{
				"AssociatedRuleGroupArn": "arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/rg/abc",
			},
		},
	)
	require.NoError(t, err)

	// Snapshot and restore.
	snap := b1.Snapshot()
	require.NotNil(t, snap)

	b2 := newTestHandler(t).Backend
	require.NoError(t, b2.Restore(snap))

	// Verify managed rule set was restored.
	ms, err := b2.GetManagedRuleSet("snap-ms-001")
	require.NoError(t, err)
	assert.Equal(t, "snap-ruleset", ms.Name)
	assert.Equal(t, "Version_2.0", ms.RecommendedVersion)
	assert.Contains(t, ms.PublishedVersions, "Version_2.0")
}

// ---- WebACL pagination: RuleGroups and IPSets pagination --------------------

func TestBatch2_ListRuleGroups_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		rec := doWafv2Request(t, h, "CreateRuleGroup", map[string]any{
			"Name":     "rg-" + itoa(i),
			"Scope":    "REGIONAL",
			"Capacity": 10,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	page1Rec := doWafv2Request(t, h, "ListRuleGroups", map[string]any{
		"Scope": "REGIONAL",
		"Limit": 3,
	})
	require.Equal(t, http.StatusOK, page1Rec.Code)

	var page1Resp map[string]any
	require.NoError(t, json.Unmarshal(page1Rec.Body.Bytes(), &page1Resp))

	groups1, _ := page1Resp["RuleGroups"].([]any)
	assert.Len(t, groups1, 3)

	nextMarker, ok := page1Resp["NextMarker"].(string)
	require.True(t, ok, "NextMarker should be present after first page")

	page2Rec := doWafv2Request(t, h, "ListRuleGroups", map[string]any{
		"Scope":      "REGIONAL",
		"Limit":      3,
		"NextMarker": nextMarker,
	})
	require.Equal(t, http.StatusOK, page2Rec.Code)

	var page2Resp map[string]any
	require.NoError(t, json.Unmarshal(page2Rec.Body.Bytes(), &page2Resp))

	groups2, _ := page2Resp["RuleGroups"].([]any)
	assert.Len(t, groups2, 2)

	_, hasNextMarker := page2Resp["NextMarker"]
	assert.False(t, hasNextMarker, "second page should have no NextMarker")
}

// ---- WebACL with multiple rules: all action types ---------------------------

func TestBatch2_WebACL_AllActionTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, rgARN := createRuleGroupHelper(t, h, "multi-action-rg")

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-all-actions",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"MetricName": "acl-all-actions",
		},
		"Rules": []map[string]any{
			{
				"Name":     "allow-rule",
				"Priority": 1,
				"Statement": map[string]any{
					"IPSetReferenceStatement": map[string]any{
						"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/allow/abc",
					},
				},
				"Action":           map[string]any{"Allow": map[string]any{}},
				"VisibilityConfig": map[string]any{"MetricName": "allow-rule"},
			},
			{
				"Name":     "block-rule",
				"Priority": 2,
				"Statement": map[string]any{
					"IPSetReferenceStatement": map[string]any{
						"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/block/def",
					},
				},
				"Action":           map[string]any{"Block": map[string]any{}},
				"VisibilityConfig": map[string]any{"MetricName": "block-rule"},
			},
			{
				"Name":     "count-rule",
				"Priority": 3,
				"Statement": map[string]any{
					"GeoMatchStatement": map[string]any{
						"CountryCodes": []string{"FR"},
					},
				},
				"Action":           map[string]any{"Count": map[string]any{}},
				"VisibilityConfig": map[string]any{"MetricName": "count-rule"},
			},
			{
				"Name":     "captcha-rule",
				"Priority": 4,
				"Statement": map[string]any{
					"GeoMatchStatement": map[string]any{
						"CountryCodes": []string{"DE"},
					},
				},
				"Action":           map[string]any{"Captcha": map[string]any{}},
				"VisibilityConfig": map[string]any{"MetricName": "captcha-rule"},
			},
			{
				"Name":     "challenge-rule",
				"Priority": 5,
				"Statement": map[string]any{
					"GeoMatchStatement": map[string]any{
						"CountryCodes": []string{"JP"},
					},
				},
				"Action":           map[string]any{"Challenge": map[string]any{}},
				"VisibilityConfig": map[string]any{"MetricName": "challenge-rule"},
			},
			{
				"Name":     "rg-override-rule",
				"Priority": 6,
				"Statement": map[string]any{
					"RuleGroupReferenceStatement": map[string]any{"ARN": rgARN},
				},
				"OverrideAction":   map[string]any{"None": map[string]any{}},
				"VisibilityConfig": map[string]any{"MetricName": "rg-override-rule"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "all action types: %s", rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["Summary"].(map[string]any)["Id"].(string)

	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	rules := getResp["WebACL"].(map[string]any)["Rules"].([]any)
	assert.Len(t, rules, 6, "all 6 rules should round-trip")
}

// ---- WebACL update clears old rules -----------------------------------------

func TestBatch2_WebACL_Update_ClearsOldRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id, lock := createWebACLWithRules(t, h, "acl-rule-clear", "REGIONAL")

	// Update with empty rules — should clear all rules.
	updateRec := doWafv2Request(t, h, "UpdateWebACL", map[string]any{
		"Id":            id,
		"LockToken":     lock,
		"DefaultAction": map[string]any{"Block": map[string]any{}},
		"Rules":         []map[string]any{},
	})
	require.Equal(t, http.StatusOK, updateRec.Code, "update with empty rules: %s", updateRec.Body.String())

	getRec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	rules := resp["WebACL"].(map[string]any)["Rules"].([]any)
	assert.Empty(t, rules, "rules should be empty after clearing")
}
