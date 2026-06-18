package secretsmanager_test

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sm "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// Issue #2 — GetSecretValue VersionId/VersionStage mismatch
// ---------------------------------------------------------------------------

// TestGetSecretValue_MismatchReturnsResourceNotFound verifies that supplying both VersionId
// and a VersionStage that the version does not carry returns ResourceNotFoundException
// (HTTP 400 with the right error type), not InvalidParameterException.
func TestGetSecretValue_MismatchReturnsResourceNotFound(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "mismatch-error-type",
		SecretString:       "value",
		ClientRequestToken: "ver-001",
	})
	require.NoError(t, err)

	// ver-001 carries AWSCURRENT — requesting AWSPREVIOUS must return ErrVersionNotFound.
	_, err = b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{
		SecretID:     "mismatch-error-type",
		VersionID:    "ver-001",
		VersionStage: sm.StagingLabelPrevious,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, sm.ErrVersionNotFound,
		"mismatch must return ErrVersionNotFound (ResourceNotFoundException), not ErrInvalidParameter")
}

// TestGetSecretValue_MismatchHTTPErrorType verifies the HTTP response body carries
// ResourceNotFoundException rather than InvalidParameterException.
func TestGetSecretValue_MismatchHTTPErrorType(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"mismatch-http","SecretString":"v","ClientRequestToken":"v1"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	body, _ := json.Marshal(map[string]any{
		"SecretId":     "mismatch-http",
		"VersionId":    "v1",
		"VersionStage": sm.StagingLabelPrevious,
	})
	rec = doR1Request(t, h, "secretsmanager.GetSecretValue", string(body))
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sm.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp.Type,
		"HTTP error body must carry ResourceNotFoundException")
}

// TestGetSecretValue_BothSuppliedAndMatch verifies that when both VersionId and VersionStage
// match the same version the call succeeds.
func TestGetSecretValue_BothSuppliedAndMatch(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "match-both",
		SecretString:       "value",
		ClientRequestToken: "ver-aaa",
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{
		SecretID:     "match-both",
		VersionID:    "ver-aaa",
		VersionStage: sm.StagingLabelCurrent,
	})
	require.NoError(t, err)
	assert.Equal(t, "ver-aaa", out.VersionID)
}

// ---------------------------------------------------------------------------
// Issue #3 — CreateSecret AddReplicaRegions
// ---------------------------------------------------------------------------

// TestCreateSecret_AddReplicaRegionsCreatesReplication verifies AddReplicaRegions in
// CreateSecretInput creates replication entries immediately.
func TestCreateSecret_AddReplicaRegionsCreatesReplication(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "create-with-replicas",
		SecretString: "value",
		AddReplicaRegions: []sm.ReplicaRegion{
			{Region: "eu-west-1"},
			{Region: "ap-southeast-1", KmsKeyID: "alias/my-key"},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.ReplicationStatus, 2, "CreateSecret must return ReplicationStatus for each replica")

	regions := make(map[string]sm.ReplicationStatusType)
	for _, r := range out.ReplicationStatus {
		regions[r.Region] = r
	}

	assert.Contains(t, regions, "eu-west-1")
	assert.Contains(t, regions, "ap-southeast-1")
	assert.Equal(t, "alias/my-key", regions["ap-southeast-1"].KmsKeyID)

	// DescribeSecret should also show the replication status.
	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "create-with-replicas"})
	require.NoError(t, err)
	require.Len(t, desc.ReplicationStatus, 2)
}

// TestCreateSecret_AddReplicaRegionsWithValueSyncsInSync verifies that when a secret is
// created with an initial value the replication status transitions to InSync.
func TestCreateSecret_AddReplicaRegionsWithValueSyncsInSync(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "replica-insync",
		SecretString: "secret-value",
		AddReplicaRegions: []sm.ReplicaRegion{
			{Region: "us-west-2"},
		},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "replica-insync"})
	require.NoError(t, err)
	require.Len(t, desc.ReplicationStatus, 1)
	assert.Equal(t, "InSync", desc.ReplicationStatus[0].Status)
}

// TestCreateSecret_AddReplicaRegionsNoValue replication status stays InProgress / Failed
// when the secret has no initial value.
func TestCreateSecret_AddReplicaRegionsNoValue(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name: "replica-no-value",
		AddReplicaRegions: []sm.ReplicaRegion{
			{Region: "ca-central-1"},
		},
	})
	require.NoError(t, err)
	// When there is no current version replication must not be InSync.
	require.Len(t, out.ReplicationStatus, 1)
	assert.NotEqual(t, "InSync", out.ReplicationStatus[0].Status)
}

// TestCreateSecret_NoReplicaRegionsReturnsEmptyStatus verifies no extra state is created
// when AddReplicaRegions is omitted.
func TestCreateSecret_NoReplicaRegionsReturnsEmptyStatus(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	out, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "no-replicas",
		SecretString: "v",
	})
	require.NoError(t, err)
	assert.Empty(t, out.ReplicationStatus)
	assert.Equal(t, 0, sm.ReplicationConfigCount(b))
}

// TestCreateSecret_AddReplicaRegionsHTTP verifies the HTTP layer parses and returns
// AddReplicaRegions correctly.
func TestCreateSecret_AddReplicaRegionsHTTP(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	body, _ := json.Marshal(map[string]any{
		"Name":         "http-replicas",
		"SecretString": "v",
		"AddReplicaRegions": []map[string]string{
			{"Region": "eu-central-1"},
		},
	})
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var out sm.CreateSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ReplicationStatus, 1)
	assert.Equal(t, "eu-central-1", out.ReplicationStatus[0].Region)
}

// ---------------------------------------------------------------------------
// Issue #6 — ListSecrets owned-by-me filter
// ---------------------------------------------------------------------------

// TestListSecrets_OwnedByMeFilterPassesAll verifies that "owned-by-me" always returns all
// secrets in the single-account mock.
func TestListSecrets_OwnedByMeFilterPassesAll(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()

	for _, name := range []string{"sec-a", "sec-b", "sec-c"} {
		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{
		Filters: []sm.SecretFilter{{Key: "owned-by-me", Values: []string{"true"}}},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 3, "owned-by-me must pass all secrets in single-account mock")
}

// TestListSecrets_OwnedByMeWithOtherFilters verifies owned-by-me can be combined with other filters.
func TestListSecrets_OwnedByMeWithOtherFilters(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "alpha-secret",
		SecretString: "v",
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "beta-secret",
		SecretString: "v",
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{
		Filters: []sm.SecretFilter{
			{Key: "owned-by-me", Values: []string{"true"}},
			{Key: "name", Values: []string{"alpha"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.SecretList, 1)
	assert.Equal(t, "alpha-secret", out.SecretList[0].Name)
}

// ---------------------------------------------------------------------------
// Issue #5 — cron expression scheduling
// ---------------------------------------------------------------------------

// TestCronNextTime_DailyMidnight verifies that a daily-at-midnight cron gives the next midnight.
func TestCronNextTime_DailyMidnight(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 * * ? *)", from)
	require.True(t, ok)
	assert.Equal(t, time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC), next)
}

// TestCronNextTime_DailyNoon verifies next noon from a morning base.
func TestCronNextTime_DailyNoon(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 3, 15, 9, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 12 * * ? *)", from)
	require.True(t, ok)
	assert.Equal(t, time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC), next)
}

// TestCronNextTime_DailyNoonAfterNoon verifies that the next noon from 2 pm is the next day.
func TestCronNextTime_DailyNoonAfterNoon(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 3, 15, 14, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 12 * * ? *)", from)
	require.True(t, ok)
	assert.Equal(t, time.Date(2024, 3, 16, 12, 0, 0, 0, time.UTC), next)
}

// TestCronNextTime_WeeklySunday verifies a weekly-on-Sunday cron.
func TestCronNextTime_WeeklySunday(t *testing.T) {
	t.Parallel()

	// 2024-03-15 is a Friday.
	from := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 ? * SUN *)", from)
	require.True(t, ok)
	assert.Equal(t, time.Weekday(0), next.Weekday(), "must land on Sunday")
	assert.Equal(t, 0, next.Hour())
	assert.Equal(t, 0, next.Minute())
}

// TestCronNextTime_MonthlyFirstDay verifies monthly on day 1.
func TestCronNextTime_MonthlyFirstDay(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 1 * ? *)", from)
	require.True(t, ok)
	assert.Equal(t, 1, next.Day())
	assert.Equal(t, time.April, next.Month())
}

// TestCronNextTime_SpecificMonth verifies scheduling in a specific month.
func TestCronNextTime_SpecificMonth(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 1 JUN ? *)", from)
	require.True(t, ok)
	assert.Equal(t, time.June, next.Month())
	assert.Equal(t, 1, next.Day())
}

// TestCronNextTime_EveryMinute verifies */1 minute step fires each minute.
func TestCronNextTime_EveryMinute(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(* * * * ? *)", from)
	require.True(t, ok)
	// Next minute boundary.
	assert.Equal(t, time.Date(2024, 3, 15, 10, 31, 0, 0, time.UTC), next)
}

// TestCronNextTime_StepMinutes verifies */15 fires on 0, 15, 30, 45 minutes.
func TestCronNextTime_StepMinutes(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 3, 15, 10, 7, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(*/15 * * * ? *)", from)
	require.True(t, ok)
	assert.Equal(t, 15, next.Minute())
}

// TestCronNextTime_RangeHours verifies a range like 8-17 for hours.
func TestCronNextTime_RangeHours(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 3, 15, 17, 30, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 8-17 * * ? *)", from)
	require.True(t, ok)
	assert.Equal(t, 8, next.Hour())
	assert.Equal(t, time.Date(2024, 3, 16, 8, 0, 0, 0, time.UTC), next)
}

// TestCronNextTime_CommaList verifies comma-separated values (e.g. MON,WED,FRI).
func TestCronNextTime_CommaList(t *testing.T) {
	t.Parallel()

	// 2024-03-15 = Friday (AWS DOW=6). Next MON/WED/FRI = Monday (2024-03-18).
	from := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 ? * MON,WED,FRI *)", from)
	require.True(t, ok)
	assert.Equal(t, time.Monday, next.Weekday())
}

// TestCronNextTime_InvalidExpressionReturnsFalse verifies that invalid expressions return false.
func TestCronNextTime_InvalidExpressionReturnsFalse(t *testing.T) {
	t.Parallel()

	cases := []string{
		"rate(1 day)",
		"not-a-cron",
		"cron()",
		"cron(too few fields)",
		"cron(0 0 * * ?)",    // only 5 fields — missing year
		"cron(99 0 * * ? *)", // minute out of range
	}

	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, expr := range cases {
		_, ok := sm.NextCronTime(expr, from)
		assert.False(t, ok, "expected false for %q", expr)
	}
}

// TestCronNextTime_YearBoundary verifies the scheduler advances to the correct year.
func TestCronNextTime_YearBoundary(t *testing.T) {
	t.Parallel()

	// Dec 31 evening → should land on Jan 1 next year.
	from := time.Date(2024, 12, 31, 23, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 1 1 ? *)", from)
	require.True(t, ok)
	assert.Equal(t, 2025, next.Year())
	assert.Equal(t, time.January, next.Month())
	assert.Equal(t, 1, next.Day())
}

// TestCronNextTime_SpecificYear verifies that a specific year in the expression limits results.
func TestCronNextTime_SpecificYear(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 1 1 ? 2025)", from)
	require.True(t, ok)
	assert.Equal(t, 2025, next.Year())
}

// TestRotateSecret_CronScheduleTriggersRotation verifies that setting a ScheduleExpression
// with a cron expression enables automatic background rotation.
func TestRotateSecret_CronScheduleTriggersRotation(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "cron-sched-secret",
		SecretString: "initial",
	})
	require.NoError(t, err)

	before, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{SecretID: "cron-sched-secret"})
	require.NoError(t, err)

	// Use a cron that fires every minute to trigger fast in tests.
	_, err = b.RotateSecret(context.Background(), &sm.RotateSecretInput{
		SecretID: "cron-sched-secret",
		RotationRules: &sm.RotationRulesType{
			ScheduleExpression: "cron(* * * * ? *)",
		},
	})
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	rotated := false

	for time.Now().Before(deadline) {
		current, currentErr := b.GetSecretValue(
			context.Background(),
			&sm.GetSecretValueInput{SecretID: "cron-sched-secret"},
		)
		require.NoError(t, currentErr)

		if current.VersionID != before.VersionID {
			rotated = true

			break
		}

		time.Sleep(200 * time.Millisecond)
	}

	assert.True(t, rotated, "cron-scheduled rotation must fire within 5 seconds")
}

// TestDescribeSecret_CronNextRotationDate verifies that NextRotationDate is computed correctly
// from a cron ScheduleExpression.
func TestDescribeSecret_CronNextRotationDate(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "cron-next-date",
		SecretString: "v",
	})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &sm.RotateSecretInput{
		SecretID: "cron-next-date",
		RotationRules: &sm.RotationRulesType{
			ScheduleExpression: "cron(0 0 * * ? *)",
		},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "cron-next-date"})
	require.NoError(t, err)
	require.NotNil(t, desc.NextRotationDate, "NextRotationDate must be set for cron schedule")

	nextTime := time.Unix(int64(*desc.NextRotationDate), 0).UTC()
	assert.Equal(t, 0, nextTime.Hour(), "daily midnight cron must give hour=0")
	assert.Equal(t, 0, nextTime.Minute())
}

// ---------------------------------------------------------------------------
// Issue #9 — Replication KMS key round-trip
// ---------------------------------------------------------------------------

// TestReplication_KmsKeyStoredAndReturned verifies that the KmsKeyId specified when
// calling ReplicateSecretToRegions is preserved and returned in the status.
func TestReplication_KmsKeyStoredAndReturned(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: "kms-rep", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.ReplicateSecretToRegions(context.Background(), &sm.ReplicateSecretToRegionsInput{
		SecretID: "kms-rep",
		AddReplicaRegions: []sm.ReplicaRegion{
			{Region: "eu-west-1", KmsKeyID: "arn:aws:kms:eu-west-1:123456789012:key/abc-123"},
		},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "kms-rep"})
	require.NoError(t, err)
	require.Len(t, desc.ReplicationStatus, 1)
	assert.Equal(t, "arn:aws:kms:eu-west-1:123456789012:key/abc-123",
		desc.ReplicationStatus[0].KmsKeyID, "KMS key must survive round-trip")
}

// TestReplication_CreateWithKmsKeyPreserved verifies that KmsKeyId in AddReplicaRegions
// at create time is returned in the output and persisted.
func TestReplication_CreateWithKmsKeyPreserved(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	createOut, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "create-kms-rep",
		SecretString: "v",
		AddReplicaRegions: []sm.ReplicaRegion{
			{Region: "us-west-2", KmsKeyID: "alias/replica-key"},
		},
	})
	require.NoError(t, err)
	require.Len(t, createOut.ReplicationStatus, 1)
	assert.Equal(t, "alias/replica-key", createOut.ReplicationStatus[0].KmsKeyID)

	// Verify persistence in DescribeSecret.
	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "create-kms-rep"})
	require.NoError(t, err)
	require.Len(t, desc.ReplicationStatus, 1)
	assert.Equal(t, "alias/replica-key", desc.ReplicationStatus[0].KmsKeyID)
}

// ---------------------------------------------------------------------------
// PutSecretValue — empty VersionStages always applies AWSCURRENT (issue #4)
// ---------------------------------------------------------------------------

// TestPutSecretValue_EmptyVersionStagesAppliesAWSCURRENT verifies that omitting VersionStages
// in PutSecretValue always attaches AWSCURRENT to the new version and promotes the old
// AWSCURRENT to AWSPREVIOUS.
func TestPutSecretValue_EmptyVersionStagesAppliesAWSCURRENT(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "empty-stages",
		SecretString: "v1",
	})
	require.NoError(t, err)

	// Capture v1's version ID before the second put.
	desc1, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "empty-stages"})
	require.NoError(t, err)
	var v1ID string
	for id, labels := range desc1.VersionIDsToStages {
		if contains(labels, sm.StagingLabelCurrent) {
			v1ID = id
		}
	}
	require.NotEmpty(t, v1ID)

	// Put second value with no explicit VersionStages (nil slice).
	put2, err := b.PutSecretValue(context.Background(), &sm.PutSecretValueInput{
		SecretID:     "empty-stages",
		SecretString: "v2",
	})
	require.NoError(t, err)
	assert.Contains(t, put2.VersionStages, sm.StagingLabelCurrent,
		"new version must carry AWSCURRENT when VersionStages is empty")

	// v1 must now carry AWSPREVIOUS.
	desc2, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "empty-stages"})
	require.NoError(t, err)
	assert.Contains(t, desc2.VersionIDsToStages[v1ID], sm.StagingLabelPrevious,
		"old AWSCURRENT version must be promoted to AWSPREVIOUS")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// contains wraps slices.Contains for readable test assertions.
func contains(s []string, target string) bool {
	return slices.Contains(s, target)
}

// TestCronParsing_MonthAliases verifies that month name aliases resolve correctly.
func TestCronParsing_MonthAliases(t *testing.T) {
	t.Parallel()

	// cron(0 0 1 MAR ? *) — first of March every year.
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 1 MAR ? *)", from)
	require.True(t, ok)
	assert.Equal(t, time.March, next.Month())
	assert.Equal(t, 1, next.Day())
}

// TestCronParsing_DOWAliases verifies that day-of-week name aliases resolve correctly.
func TestCronParsing_DOWAliases(t *testing.T) {
	t.Parallel()

	// cron(0 0 ? * MON *) — every Monday at midnight.
	// 2024-03-15 is Friday; next Monday is 2024-03-18.
	from := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 ? * MON *)", from)
	require.True(t, ok)
	assert.Equal(t, time.Monday, next.Weekday())
}

// TestCronParsing_StepField verifies step expressions like */6 for hours.
func TestCronParsing_StepField(t *testing.T) {
	t.Parallel()

	// cron(0 */6 * * ? *) — every 6 hours (0, 6, 12, 18).
	from := time.Date(2024, 3, 15, 7, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 */6 * * ? *)", from)
	require.True(t, ok)
	assert.Equal(t, 12, next.Hour())
}

// TestCronParsing_RangeField verifies range expressions like 1-5 for day-of-week (Mon-Fri).
func TestCronParsing_RangeField(t *testing.T) {
	t.Parallel()

	// cron(0 9 ? * 2-6 *) — Mon-Fri at 09:00.
	// 2024-03-15 is Friday. Friday DOW=6 (AWS). 09:00 hasn't passed (from = 08:00).
	from := time.Date(2024, 3, 15, 8, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 9 ? * 2-6 *)", from)
	require.True(t, ok)
	assert.Equal(t, time.Friday, next.Weekday())
	assert.Equal(t, 9, next.Hour())
}

// TestListSecrets_PrimaryRegionFilter verifies that the "primary-region" filter always passes.
func TestListSecrets_PrimaryRegionFilter(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	for _, name := range []string{"pr-a", "pr-b"} {
		_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &sm.ListSecretsInput{
		Filters: []sm.SecretFilter{{Key: "primary-region", Values: []string{"us-east-1"}}},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 2)
}

// TestCreateSecret_AddReplicaRegionsMultipleHTTP verifies that multiple replica regions
// specified at create time are all present in DescribeSecret output.
func TestCreateSecret_AddReplicaRegionsMultipleHTTP(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	body, _ := json.Marshal(map[string]any{
		"Name":         "multi-rep",
		"SecretString": "v",
		"AddReplicaRegions": []map[string]string{
			{"Region": "us-west-1"},
			{"Region": "eu-north-1"},
			{"Region": "ap-east-1"},
		},
	})
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doR1Request(t, h, "secretsmanager.DescribeSecret", `{"SecretId":"multi-rep"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc sm.DescribeSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	assert.Len(t, desc.ReplicationStatus, 3)
}

// TestGetSecretValue_VersionIdOnlySucceeds verifies that supplying only VersionId (no stage)
// works correctly.
func TestGetSecretValue_VersionIdOnlySucceeds(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "ver-only",
		SecretString:       "secret",
		ClientRequestToken: "tok-123",
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{
		SecretID:  "ver-only",
		VersionID: "tok-123",
	})
	require.NoError(t, err)
	assert.Equal(t, "tok-123", out.VersionID)
	assert.Equal(t, "secret", out.SecretString)
}

// TestGetSecretValue_VersionStageOnlySucceeds verifies that supplying only VersionStage works.
func TestGetSecretValue_VersionStageOnlySucceeds(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "stage-only",
		SecretString: "value",
	})
	require.NoError(t, err)

	out, err := b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{
		SecretID:     "stage-only",
		VersionStage: sm.StagingLabelCurrent,
	})
	require.NoError(t, err)
	assert.Contains(t, out.VersionStages, sm.StagingLabelCurrent)
}

// TestRotateSecret_ScheduleExpressionPersisted verifies that a cron ScheduleExpression
// is persisted in RotationRules and visible in DescribeSecret.
func TestRotateSecret_ScheduleExpressionPersisted(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:         "cron-persist",
		SecretString: "v",
	})
	require.NoError(t, err)

	const expr = "cron(0 12 * * ? *)"
	rotateImmediately := false
	_, err = b.RotateSecret(context.Background(), &sm.RotateSecretInput{
		SecretID: "cron-persist",
		RotationRules: &sm.RotationRulesType{
			ScheduleExpression: expr,
		},
		RotateImmediately: &rotateImmediately,
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "cron-persist"})
	require.NoError(t, err)
	require.NotNil(t, desc.RotationRules)
	assert.Equal(t, expr, desc.RotationRules.ScheduleExpression)
}

// TestCronNextTime_ExactMinuteBoundary verifies that when `from` is exactly on a cron
// fire time, the next returned is strictly after (not at the same second).
func TestCronNextTime_ExactMinuteBoundary(t *testing.T) {
	t.Parallel()

	// `from` is exactly at midnight — next should be the next midnight, not this one.
	from := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 * * ? *)", from)
	require.True(t, ok)
	assert.True(t, next.After(from), "nextCronTime must return a time strictly after `from`")
	assert.Equal(t, time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC), next)
}

// TestCronNextTime_LeapYearFebuary verifies correct handling of Feb 29 in a leap year.
func TestCronNextTime_LeapYearFebuary(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 2, 28, 0, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 29 2 ? *)", from)
	require.True(t, ok)
	assert.Equal(t, time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC), next)
}

// TestRotationDue_CronExpression verifies that rotationDue returns false before the next
// cron time and true at or after it.
func TestRotationDue_CronExpression(t *testing.T) {
	t.Parallel()

	// Base: March 15 midnight. Cron: daily at midnight.
	base := sm.UnixTimeFloat(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC))
	rules := &sm.RotationRulesType{ScheduleExpression: "cron(0 0 * * ? *)"}

	// Before next midnight: not due.
	assert.False(t, sm.RotationDue(rules, time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC), &base))

	// At next midnight: due.
	assert.True(t, sm.RotationDue(rules, time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC), &base))

	// After next midnight: due.
	assert.True(t, sm.RotationDue(rules, time.Date(2024, 3, 17, 0, 0, 0, 0, time.UTC), &base))
}

// TestRotationDue_IntervalExpression verifies rotationDue for rate() expressions.
func TestRotationDue_IntervalExpression(t *testing.T) {
	t.Parallel()

	base := sm.UnixTimeFloat(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC))
	rules := &sm.RotationRulesType{ScheduleExpression: "rate(1 day)"}

	before := time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC)
	after := time.Date(2024, 3, 16, 1, 0, 0, 0, time.UTC)

	assert.False(t, sm.RotationDue(rules, before, &base))
	assert.True(t, sm.RotationDue(rules, after, &base))
}

// TestListSecretsOwnedByMeHTTP verifies the owned-by-me filter via HTTP.
func TestListSecretsOwnedByMeHTTP(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	h := sm.NewHandler(b)

	for _, name := range []string{"om-1", "om-2"} {
		rec := doR1Request(t, h, "secretsmanager.CreateSecret",
			`{"Name":"`+name+`","SecretString":"v"}`)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	filterBody := `{"Filters":[{"Key":"owned-by-me","Values":["true"]}]}`
	rec := doR1Request(t, h, "secretsmanager.ListSecrets", filterBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var out sm.ListSecretsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.SecretList, 2)
}

// TestCreateSecret_AddReplicaRegionsThenMoreReplicas verifies that AddReplicaRegions at
// create time and subsequent ReplicateSecretToRegions calls both contribute correctly.
func TestCreateSecret_AddReplicaRegionsThenMoreReplicas(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:              "grow-replicas",
		SecretString:      "v",
		AddReplicaRegions: []sm.ReplicaRegion{{Region: "us-west-2"}},
	})
	require.NoError(t, err)

	_, err = b.ReplicateSecretToRegions(context.Background(), &sm.ReplicateSecretToRegionsInput{
		SecretID:          "grow-replicas",
		AddReplicaRegions: []sm.ReplicaRegion{{Region: "eu-west-1"}},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "grow-replicas"})
	require.NoError(t, err)
	assert.Len(t, desc.ReplicationStatus, 2)

	regions := make(map[string]bool)
	for _, r := range desc.ReplicationStatus {
		regions[r.Region] = true
	}
	assert.True(t, regions["us-west-2"])
	assert.True(t, regions["eu-west-1"])
}

// TestGetSecretValue_MismatchAfterRotation verifies the mismatch check works after rotation
// (when AWSPREVIOUS exists on a different version).
func TestGetSecretValue_MismatchAfterRotation(t *testing.T) {
	t.Parallel()

	b := sm.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &sm.CreateSecretInput{
		Name:               "rot-mismatch",
		SecretString:       "v1",
		ClientRequestToken: "v1-id",
	})
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &sm.RotateSecretInput{SecretID: "rot-mismatch"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &sm.DescribeSecretInput{SecretID: "rot-mismatch"})
	require.NoError(t, err)

	// Find the AWSCURRENT version and try to get it with AWSPREVIOUS — must fail.
	var currentID string
	for id, labels := range desc.VersionIDsToStages {
		for _, l := range labels {
			if l == sm.StagingLabelCurrent {
				currentID = id
			}
		}
	}
	require.NotEmpty(t, currentID)

	_, err = b.GetSecretValue(context.Background(), &sm.GetSecretValueInput{
		SecretID:     "rot-mismatch",
		VersionID:    currentID,
		VersionStage: sm.StagingLabelPrevious,
	})
	require.ErrorIs(t, err, sm.ErrVersionNotFound)
}

// TestCronNextTime_NotFiringInPast verifies that nextCronTime always returns a future time.
func TestCronNextTime_NotFiringInPast(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 * * ? *)", from)
	require.True(t, ok)
	assert.True(t, next.After(from))
}

// TestCronNextTime_ExpressionWithSpaces verifies leading/trailing space tolerance.
func TestCronNextTime_ExpressionWithSpaces(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("  cron(0 0 * * ? *)  ", from)
	require.True(t, ok)
	assert.Equal(t, time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC), next)
}

// TestCronNextTime_BothDOWAndDOM verifies AWS OR semantics when both dom and dow are non-wildcard.
func TestCronNextTime_BothDOWAndDOM(t *testing.T) {
	t.Parallel()

	// 2024-03-15 is Friday (DOW=6 in AWS). cron(0 0 20 * FRI *) fires on day-20 OR any Friday.
	// From March 15 midnight, next Friday is March 22; next day-20 is March 20.
	from := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	next, ok := sm.NextCronTime("cron(0 0 20 * FRI *)", from)
	require.True(t, ok)
	// Should be March 20 (day-20 before next Friday March 22).
	assert.Equal(t, time.Date(2024, 3, 20, 0, 0, 0, 0, time.UTC), next)
}

// Sentinel: verify strings import is used.
var _ = strings.TrimSpace
