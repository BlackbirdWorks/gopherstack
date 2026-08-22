package dms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
)

// TestDescribeEvents_DateDecodesAsEpoch drives DescribeEvents through the
// real DMS client. Event.Date deserializes from a json.Number via
// ParseEpochSeconds (deserializers.go,
// awsAwsjson11_deserializeDocumentEvent, case "Date") -- gopherstack
// previously emitted an RFC3339 string there, which failed DescribeEvents'
// decode outright for any client with a recorded event.
func TestDescribeEvents_DateDecodesAsEpoch(t *testing.T) {
	t.Parallel()

	b := dms.NewInMemoryBackend("123456789012", "us-east-1")
	h := dms.NewHandler(b)
	client := newTestDMSClient(t, h)

	_, err := b.CreateEndpoint(
		t.Context(), "my-endpoint", "source", "mysql", "host.example.com", "db", "user", "pw", 3306, nil,
	)
	require.NoError(t, err)

	out, err := client.DescribeEvents(t.Context(), &dmssdk.DescribeEventsInput{})
	require.NoError(t, err, "real SDK client must decode DescribeEvents without error")
	require.NotEmpty(t, out.Events)
	assert.NotNil(t, out.Events[0].Date)
}

// TestDescribeFleetAdvisorDatabases_EngineDecodesNested drives
// DescribeFleetAdvisorDatabases through the real DMS client.
// DatabaseResponse has no top-level EngineName or CollectorReferencedId --
// the deserializer only recurses into those fields via SoftwareDetails.Engine
// (deserializers.go, awsAwsjson11_deserializeDocumentDatabaseInstanceSoftwareDetailsResponse,
// case "Engine") and Collectors[].CollectorReferencedId
// (awsAwsjson11_deserializeDocumentCollectorShortInfoResponse, case
// "CollectorReferencedId"). gopherstack previously wrote both flat on the
// database object; an exact-case real client dropped them silently
// (gopherstack-zquj).
func TestDescribeFleetAdvisorDatabases_EngineDecodesNested(t *testing.T) {
	t.Parallel()

	b := dms.NewInMemoryBackend("123456789012", "us-east-1")
	h := dms.NewHandler(b)
	client := newTestDMSClient(t, h)

	_, err := client.CreateFleetAdvisorCollector(t.Context(), &dmssdk.CreateFleetAdvisorCollectorInput{
		CollectorName:        aws.String("col1"),
		ServiceAccessRoleArn: aws.String("arn:aws:iam::123456789012:role/dms-role"),
		S3BucketName:         aws.String("my-bucket"),
	})
	require.NoError(t, err)

	out, err := client.DescribeFleetAdvisorDatabases(t.Context(), &dmssdk.DescribeFleetAdvisorDatabasesInput{})
	require.NoError(t, err, "real SDK client must decode DescribeFleetAdvisorDatabases without error")
	require.NotEmpty(t, out.Databases)

	db0 := out.Databases[0]
	require.NotNil(t, db0.SoftwareDetails, "SoftwareDetails must decode non-nil")
	assert.NotEmpty(t, aws.ToString(db0.SoftwareDetails.Engine))
	require.NotEmpty(t, db0.Collectors, "Collectors must decode non-empty")
	assert.NotEmpty(t, aws.ToString(db0.Collectors[0].CollectorReferencedId))
}
