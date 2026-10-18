package cur_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cursdk "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice"
	curtypes "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cur"
)

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucket(t)

	_, err := f.client.PutReportDefinition(t.Context(), &cursdk.PutReportDefinitionInput{
		ReportDefinition: def("r1"),
		Tags:             []curtypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.NoError(t, err)
	f.backend.DeliverDue(t.Context())

	snap := f.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := cur.NewInMemoryBackend("999999999999", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	p, err := restored.DescribeReportDefinitions("", nil)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "r1", p.Data[0].ReportName)
	require.NotNil(t, p.Data[0].ReportStatus)
	assert.Equal(t, "SUCCESS", p.Data[0].ReportStatus.LastStatus)
}
