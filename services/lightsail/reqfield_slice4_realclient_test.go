package lightsail_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lightsail"
)

// TestReqFieldDiffSlice4_Lightsail drives every dropped-parameter fix from
// the reqfielddiff slice-4 pass through the real aws-sdk-go-v2 client
// (newTestClient, shared with typed_slice33_realclient_test.go) and asserts
// the observable effect of each.
func TestReqFieldDiffSlice4_Lightsail(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testCreateRelationalDatabaseWindows, "create_relational_database_windows"},
		{testDeleteKeyPairExpectedFingerprint, "delete_keypair_expected_fingerprint"},
		{testGetRelationalDatabaseEventsDuration, "get_relational_database_events_duration"},
		{testPutAlarmDefaults, "put_alarm_notification_and_treat_missing_data"},
		{testUpdateDistributionUseDefaultCertificate, "update_distribution_use_default_certificate"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testCreateRelationalDatabaseWindows drives
// CreateRelationalDatabase.PreferredBackupWindow/PreferredMaintenanceWindow
// (both restjson1... actually awsjson1.1 body fields, decoded but never
// forwarded to the backend before this fix -- CreateRelationalDatabase
// always hardcoded its own defaults regardless of caller input).
func testCreateRelationalDatabaseWindows(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateRelationalDatabase(ctx, &lightsailsdk.CreateRelationalDatabaseInput{
		RelationalDatabaseName:        aws.String("slice4-db"),
		MasterDatabaseName:            aws.String("appdb"),
		MasterUsername:                aws.String("dbadmin"),
		RelationalDatabaseBlueprintId: aws.String("mysql_8_0"),
		RelationalDatabaseBundleId:    aws.String("micro_2_0"),
		PreferredBackupWindow:         aws.String("03:00-03:30"),
		PreferredMaintenanceWindow:    aws.String("wed:04:00-wed:04:30"),
	})
	require.NoError(t, err)

	getOut, err := client.GetRelationalDatabase(ctx, &lightsailsdk.GetRelationalDatabaseInput{
		RelationalDatabaseName: aws.String("slice4-db"),
	})
	require.NoError(t, err)
	assert.Equal(t, "03:00-03:30", aws.ToString(getOut.RelationalDatabase.PreferredBackupWindow))
	assert.Equal(t, "wed:04:00-wed:04:30", aws.ToString(getOut.RelationalDatabase.PreferredMaintenanceWindow))
}

// testDeleteKeyPairExpectedFingerprint drives DeleteKeyPair.ExpectedFingerprint
// (an awsjson1.1 body field, decoded but never forwarded to the backend
// before this fix): a mismatched fingerprint must reject the delete, a
// matching one must succeed.
func testDeleteKeyPairExpectedFingerprint(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	createOut, err := client.CreateKeyPair(ctx, &lightsailsdk.CreateKeyPairInput{KeyPairName: aws.String("slice4-kp")})
	require.NoError(t, err)
	realFingerprint := aws.ToString(createOut.KeyPair.Fingerprint)
	require.NotEmpty(t, realFingerprint)

	_, err = client.DeleteKeyPair(ctx, &lightsailsdk.DeleteKeyPairInput{
		KeyPairName:         aws.String("slice4-kp"),
		ExpectedFingerprint: aws.String("wrong:fingerprint"),
	})
	require.Error(t, err)

	_, err = client.DeleteKeyPair(ctx, &lightsailsdk.DeleteKeyPairInput{
		KeyPairName:         aws.String("slice4-kp"),
		ExpectedFingerprint: aws.String(realFingerprint),
	})
	require.NoError(t, err)

	_, err = client.GetKeyPair(ctx, &lightsailsdk.GetKeyPairInput{KeyPairName: aws.String("slice4-kp")})
	require.Error(t, err)
}

// testGetRelationalDatabaseEventsDuration drives
// GetRelationalDatabaseEvents.DurationInMinutes (decoded but never
// forwarded to the backend before this fix, so the returned event list was
// never actually windowed by time). Backdates one recorded event via a
// real Snapshot/Restore round trip -- the same production persistence path
// this backend already uses -- since there is no fake clock to advance
// real wall-clock minutes within a unit test.
func testGetRelationalDatabaseEventsDuration(t *testing.T) {
	t.Helper()

	backend := lightsail.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)
	h := lightsail.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	_, err := client.CreateRelationalDatabase(ctx, &lightsailsdk.CreateRelationalDatabaseInput{
		RelationalDatabaseName:        aws.String("slice4-events-db"),
		MasterDatabaseName:            aws.String("appdb"),
		MasterUsername:                aws.String("dbadmin"),
		RelationalDatabaseBlueprintId: aws.String("mysql_8_0"),
		RelationalDatabaseBundleId:    aws.String("micro_2_0"),
	})
	require.NoError(t, err)

	_, err = client.RebootRelationalDatabase(ctx, &lightsailsdk.RebootRelationalDatabaseInput{
		RelationalDatabaseName: aws.String("slice4-events-db"),
	})
	require.NoError(t, err)

	backdateOldestEvent(t, backend, "slice4-events-db", time.Now().Add(-2*time.Hour))

	_, err = client.RebootRelationalDatabase(ctx, &lightsailsdk.RebootRelationalDatabaseInput{
		RelationalDatabaseName: aws.String("slice4-events-db"),
	})
	require.NoError(t, err)

	narrowOut, err := client.GetRelationalDatabaseEvents(ctx, &lightsailsdk.GetRelationalDatabaseEventsInput{
		RelationalDatabaseName: aws.String("slice4-events-db"),
		DurationInMinutes:      aws.Int32(5),
	})
	require.NoError(t, err)
	assert.Len(t, narrowOut.RelationalDatabaseEvents, 1, "the 2-hour-old event must fall outside a 5-minute window")

	wideOut, err := client.GetRelationalDatabaseEvents(ctx, &lightsailsdk.GetRelationalDatabaseEventsInput{
		RelationalDatabaseName: aws.String("slice4-events-db"),
		DurationInMinutes:      aws.Int32(180),
	})
	require.NoError(t, err)
	assert.Len(t, wideOut.RelationalDatabaseEvents, 2, "a 180-minute window must cover both events")
}

// backdateOldestEvent rewrites dbName's single recorded event's CreatedAt
// to when, via backend.Snapshot/Restore -- the same production persistence
// round trip this backend already exercises for real save/load, used here
// only to fabricate the passage of time no fake clock provides.
func backdateOldestEvent(t *testing.T, backend *lightsail.InMemoryBackend, dbName string, when time.Time) {
	t.Helper()

	type snapshot = map[string]json.RawMessage

	raw := backend.Snapshot(t.Context())
	require.NotEmpty(t, raw)

	var snap snapshot
	require.NoError(t, json.Unmarshal(raw, &snap))

	var tables snapshot
	require.NoError(t, json.Unmarshal(snap["tables"], &tables))

	// Decoded as untyped maps, not lightsail.RelationalDatabase, so this
	// edit doesn't need (and musttag doesn't demand) a json tag on an
	// internal persistence struct that intentionally has none.
	var dbs []map[string]any
	require.NoError(t, json.Unmarshal(tables["databases"], &dbs))
	require.Len(t, dbs, 1)
	require.Equal(t, dbName, dbs[0]["Name"])

	events, ok := dbs[0]["Events"].([]any)
	require.True(t, ok)
	require.Len(t, events, 1)

	event, ok := events[0].(map[string]any)
	require.True(t, ok)
	event["CreatedAt"] = when.Format(time.RFC3339Nano)

	newDBs, err := json.Marshal(dbs)
	require.NoError(t, err)
	tables["databases"] = newDBs

	newTables, err := json.Marshal(tables)
	require.NoError(t, err)
	snap["tables"] = newTables

	newSnap, err := json.Marshal(snap)
	require.NoError(t, err)

	require.NoError(t, backend.Restore(t.Context(), newSnap))
}

// testPutAlarmDefaults drives PutAlarm's NotificationEnabled/TreatMissingData
// documented defaults ("enabled by default if you don't specify", "the
// default behavior of missing is used"): omitting them previously stored
// Go zero values (false / "") instead of the real defaults (true /
// "missing"), and PutAlarm.NotificationEnabled=false must still be
// honored on an explicit request.
func testPutAlarmDefaults(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateInstances(ctx, &lightsailsdk.CreateInstancesInput{
		InstanceNames: []string{"slice4-alarm-target"}, AvailabilityZone: aws.String("us-east-1a"),
		BlueprintId: aws.String("amazon_linux_2023"), BundleId: aws.String("nano_3_0"),
	})
	require.NoError(t, err)

	_, err = client.PutAlarm(ctx, &lightsailsdk.PutAlarmInput{
		AlarmName:             aws.String("slice4-alarm-defaults"),
		ComparisonOperator:    lightsailtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:     aws.Int32(1),
		MetricName:            lightsailtypes.MetricNameCPUUtilization,
		MonitoredResourceName: aws.String("slice4-alarm-target"),
		Threshold:             aws.Float64(80),
	})
	require.NoError(t, err)

	getOut, err := client.GetAlarms(
		ctx, &lightsailsdk.GetAlarmsInput{AlarmName: aws.String("slice4-alarm-defaults")},
	)
	require.NoError(t, err)
	require.Len(t, getOut.Alarms, 1)
	assert.True(t, aws.ToBool(getOut.Alarms[0].NotificationEnabled), "default NotificationEnabled must be true")
	assert.Equal(t, lightsailtypes.TreatMissingDataMissing, getOut.Alarms[0].TreatMissingData)

	_, err = client.PutAlarm(ctx, &lightsailsdk.PutAlarmInput{
		AlarmName:             aws.String("slice4-alarm-explicit"),
		ComparisonOperator:    lightsailtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:     aws.Int32(1),
		MetricName:            lightsailtypes.MetricNameCPUUtilization,
		MonitoredResourceName: aws.String("slice4-alarm-target"),
		Threshold:             aws.Float64(80),
		NotificationEnabled:   aws.Bool(false),
		TreatMissingData:      lightsailtypes.TreatMissingDataBreaching,
	})
	require.NoError(t, err)

	explicitOut, err := client.GetAlarms(
		ctx, &lightsailsdk.GetAlarmsInput{AlarmName: aws.String("slice4-alarm-explicit")},
	)
	require.NoError(t, err)
	require.Len(t, explicitOut.Alarms, 1)
	assert.False(t, aws.ToBool(explicitOut.Alarms[0].NotificationEnabled))
	assert.Equal(t, lightsailtypes.TreatMissingDataBreaching, explicitOut.Alarms[0].TreatMissingData)
}

// testUpdateDistributionUseDefaultCertificate drives
// UpdateDistribution.UseDefaultCertificate (decoded but never forwarded to
// the backend before this fix): setting it true must clear a previously
// attached custom certificate name.
func testUpdateDistributionUseDefaultCertificate(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateBucket(ctx, &lightsailsdk.CreateBucketInput{
		BucketName: aws.String("slice4-dist-origin"), BundleId: aws.String("small_1_0"),
	})
	require.NoError(t, err)

	_, err = client.CreateDistribution(ctx, &lightsailsdk.CreateDistributionInput{
		DistributionName: aws.String("slice4-dist"), BundleId: aws.String("small_1_0"),
		Origin:               &lightsailtypes.InputOrigin{Name: aws.String("slice4-dist-origin")},
		DefaultCacheBehavior: &lightsailtypes.CacheBehavior{Behavior: lightsailtypes.BehaviorEnum("cache")},
		CertificateName:      aws.String("slice4-cert"),
	})
	require.NoError(t, err)

	beforeOut, err := client.GetDistributions(ctx, &lightsailsdk.GetDistributionsInput{
		DistributionName: aws.String("slice4-dist"),
	})
	require.NoError(t, err)
	require.Len(t, beforeOut.Distributions, 1)
	assert.Equal(t, "slice4-cert", aws.ToString(beforeOut.Distributions[0].CertificateName))

	_, err = client.UpdateDistribution(ctx, &lightsailsdk.UpdateDistributionInput{
		DistributionName:      aws.String("slice4-dist"),
		UseDefaultCertificate: aws.Bool(true),
	})
	require.NoError(t, err)

	afterOut, err := client.GetDistributions(ctx, &lightsailsdk.GetDistributionsInput{
		DistributionName: aws.String("slice4-dist"),
	})
	require.NoError(t, err)
	require.Len(t, afterOut.Distributions, 1)
	assert.Empty(t, aws.ToString(afterOut.Distributions[0].CertificateName))
}
