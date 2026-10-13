package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKnowledgeBase_PrimaryOwnerUsername(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ownerArn  string
		wantOwner string
	}{
		{name: "user", ownerArn: "arn:aws:quicksight:us-east-1:000000000000:user/default/alice", wantOwner: "alice"},
		{
			name:      "federated user",
			ownerArn:  "arn:aws:quicksight:us-east-1:000000000000:user/default/role/sess",
			wantOwner: "role/sess",
		},
		{name: "group", ownerArn: "arn:aws:quicksight:us-east-1:000000000000:group/default/eng"},
		{name: "none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newQuickSightTestClient(t)
			ctx := t.Context()

			in := &quicksightsdk.CreateKnowledgeBaseInput{
				AwsAccountId:    aws.String(qsTestAccountID),
				KnowledgeBaseId: aws.String("kb-1"),
				Name:            aws.String("kb"),
				DataSourceArn:   aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/ds-1"),
				KnowledgeBaseConfiguration: &types.KnowledgeBaseConfiguration{
					TemplateConfiguration: &types.KbTemplateConfiguration{},
				},
			}
			if tt.ownerArn != "" {
				in.PrimaryOwnerArn = aws.String(tt.ownerArn)
			}

			_, err := client.CreateKnowledgeBase(ctx, in)
			require.NoError(t, err)

			desc, err := client.DescribeKnowledgeBase(ctx, &quicksightsdk.DescribeKnowledgeBaseInput{
				AwsAccountId: aws.String(qsTestAccountID), KnowledgeBaseId: aws.String("kb-1"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantOwner, aws.ToString(desc.KnowledgeBase.PrimaryOwnerUsername))

			list, err := client.ListKnowledgeBases(ctx, &quicksightsdk.ListKnowledgeBasesInput{
				AwsAccountId: aws.String(qsTestAccountID),
			})
			require.NoError(t, err)
			require.Len(t, list.KnowledgeBaseSummaries, 1)
			assert.Equal(t, tt.wantOwner, aws.ToString(list.KnowledgeBaseSummaries[0].PrimaryOwnerUsername))
		})
	}
}
