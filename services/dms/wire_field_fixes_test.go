package dms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
)

// TestCreateDataMigration_SettingsNestUnderDataMigrationSettings_RealClient
// proves DataMigration.DataMigrationSettings round-trips. The real
// deserializer (databasemigrationservice@v1.66.4 deserializers.go:16304,
// case "DataMigrationSettings") nests NumberOfJobs and a
// CloudwatchLogsEnabled field inside that sub-object -- gopherstack wrote
// both flat on DataMigration under EnableCloudwatchLogs, a name the real
// DataMigration case list (same file, same func) has no case for at all, so
// every real client's DataMigration.DataMigrationSettings decoded nil.
func TestCreateDataMigration_SettingsNestUnderDataMigrationSettings_RealClient(t *testing.T) {
	t.Parallel()

	b := dms.NewInMemoryBackend("123456789012", "us-east-1")
	h := dms.NewHandler(b)
	client := newTestDMSClient(t, h)

	out, err := client.CreateDataMigration(t.Context(), &dmssdk.CreateDataMigrationInput{
		DataMigrationName:          aws.String("wire-dm"),
		MigrationProjectIdentifier: aws.String("proj-1"),
		ServiceAccessRoleArn:       aws.String("arn:aws:iam::123456789012:role/dms-role"),
		DataMigrationType:          types.MigrationTypeValueFullLoad,
		NumberOfJobs:               aws.Int32(3),
		EnableCloudwatchLogs:       aws.Bool(true),
	})
	require.NoError(t, err, "real SDK client must decode CreateDataMigration without error")

	require.NotNil(t, out.DataMigration)
	require.NotNil(
		t,
		out.DataMigration.DataMigrationSettings,
		"DataMigration.DataMigrationSettings must decode non-nil",
	)
	assert.Equal(t, int32(3), *out.DataMigration.DataMigrationSettings.NumberOfJobs)
	assert.True(t, *out.DataMigration.DataMigrationSettings.CloudwatchLogsEnabled)
}
