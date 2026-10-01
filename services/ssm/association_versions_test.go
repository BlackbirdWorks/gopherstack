package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestAssociationVersions_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("assoc-ver-doc"),
		Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
	})
	require.NoError(t, err)

	created, err := client.CreateAssociation(ctx, &ssmsdk.CreateAssociationInput{
		Name:            aws.String("assoc-ver-doc"),
		AssociationName: aws.String("first"),
		InstanceId:      aws.String("i-assocver"),
	})
	require.NoError(t, err)

	id := created.AssociationDescription.AssociationId

	for _, name := range []string{"second", "third"} {
		_, err = client.UpdateAssociation(ctx, &ssmsdk.UpdateAssociationInput{
			AssociationId:   id,
			AssociationName: aws.String(name),
		})
		require.NoError(t, err)
	}

	listed, err := client.ListAssociationVersions(ctx, &ssmsdk.ListAssociationVersionsInput{AssociationId: id})
	require.NoError(t, err)
	require.Len(t, listed.AssociationVersions, 3)

	for i, want := range []string{"first", "second", "third"} {
		v := listed.AssociationVersions[i]
		assert.Equal(t, []string{"1", "2", "3"}[i], aws.ToString(v.AssociationVersion))
		assert.Equal(t, want, aws.ToString(v.AssociationName))
	}

	page, err := client.ListAssociationVersions(ctx, &ssmsdk.ListAssociationVersionsInput{
		AssociationId: id, MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, page.AssociationVersions, 2)
	assert.NotNil(t, page.NextToken)

	tests := []struct {
		name     string
		version  *string
		wantName string
		wantVer  string
		wantErr  string
	}{
		{"omitted is latest", nil, "third", "3", ""},
		{"latest alias", aws.String("$LATEST"), "third", "3", ""},
		{"first", aws.String("1"), "first", "1", ""},
		{"second", aws.String("2"), "second", "2", ""},
		{"unknown", aws.String("9"), "", "", "InvalidAssociationVersion"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, descErr := client.DescribeAssociation(ctx, &ssmsdk.DescribeAssociationInput{
				AssociationId: id, AssociationVersion: tt.version,
			})

			if tt.wantErr != "" {
				var invalid *ssmtypes.InvalidAssociationVersion

				require.Error(t, descErr)
				require.ErrorAs(t, descErr, &invalid)

				return
			}

			require.NoError(t, descErr)
			assert.Equal(t, tt.wantName, aws.ToString(got.AssociationDescription.AssociationName))
			assert.Equal(t, tt.wantVer, aws.ToString(got.AssociationDescription.AssociationVersion))
		})
	}
}
