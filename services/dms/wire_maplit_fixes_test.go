package dms_test

import (
	"testing"

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
