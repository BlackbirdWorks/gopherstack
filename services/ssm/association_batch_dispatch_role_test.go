package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAssociationBatch_DispatchAssumeRole_RealClient(t *testing.T) {
	t.Parallel()

	const batchRole = "arn:aws:iam::123456789012:role/batch-dispatch"

	cases := []struct {
		batchRole *string
		name      string
		wantRole  string
	}{
		{name: "none"},
		{name: "batch_role_applies", batchRole: aws.String(batchRole), wantRole: batchRole},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
				Name: aws.String("batch-role-doc"),
				Content: aws.String(
					`{"schemaVersion":"2.2","mainSteps":[{"action":"aws:runShellScript","name":"s"}]}`,
				),
				DocumentType: ssmtypes.DocumentTypeCommand,
			})
			require.NoError(t, err)

			out, err := client.CreateAssociationBatch(ctx, &ssmsdk.CreateAssociationBatchInput{
				AssociationDispatchAssumeRole: tc.batchRole,
				Entries: []ssmtypes.CreateAssociationBatchRequestEntry{{
					Name:       aws.String("batch-role-doc"),
					InstanceId: aws.String("i-0123456789abcdef0"),
				}},
			})
			require.NoError(t, err)
			require.Len(t, out.Successful, 1)
			assert.Equal(t, tc.wantRole, aws.ToString(out.Successful[0].AssociationDispatchAssumeRole))
		})
	}
}
