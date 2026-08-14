package iam_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestGetServiceLastAccessedDetailsWithEntities_JobCompletionDate_RealClient
// covers gopherstack-r80d (required-output-member sweep). JobCompletionDate
// is required on GetServiceLastAccessedDetailsWithEntitiesOutput
// (iam@v1.58.1 api_op_GetServiceLastAccessedDetailsWithEntities.go:103-111),
// but the wire struct backing the handler's response
// (models_access_advisor.go's getSLADWithEntitiesResult) had no field for it
// at all -- not merely unset, structurally absent from the XML -- so a real
// client's *time.Time always decoded nil regardless of what the backend
// did. Driven through the real aws-sdk-go-v2 client since the bug is a
// missing struct field, invisible to any test that inspects the handler's
// map/struct literal directly instead of the actual wire bytes.
func TestGetServiceLastAccessedDetailsWithEntities_JobCompletionDate_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	out, err := client.GetServiceLastAccessedDetailsWithEntities(
		t.Context(),
		&iamsdk.GetServiceLastAccessedDetailsWithEntitiesInput{
			JobId:            aws.String("job-1234"),
			ServiceNamespace: aws.String("s3"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.JobCompletionDate)
	assert.False(t, out.JobCompletionDate.IsZero())
	require.NotNil(t, out.JobCreationDate)
	assert.False(t, out.JobCreationDate.IsZero())
}
