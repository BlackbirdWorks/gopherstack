package bedrockagent_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

// slice23ResourcePolicyDoc builds a minimal valid resource policy document
// for a knowledge base ARN, granting principalAccountID cross-account read
// access.
func slice23ResourcePolicyDoc(principalAccountID, resourceArn string) string {
	const tmpl = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"arn:aws:iam::%s:root"},` +
		`"Action":"bedrock:GetKnowledgeBase","Resource":"%s"}]}`

	return fmt.Sprintf(tmpl, principalAccountID, resourceArn)
}

// TestTypedSlice23RealClient drives bedrockagent's remaining typed-coverage-
// blind ops (gopherstack-n3zi slice 23) through the real aws-sdk-go-v2
// client: PutResourcePolicy, GetResourcePolicy, DeleteResourcePolicy.
func TestTypedSlice23RealClient(t *testing.T) {
	t.Parallel()

	t.Run("resource policy lifecycle", func(t *testing.T) {
		t.Parallel()

		backend := bedrockagent.NewTestBackend(rtTestRegion, rtTestAccountID)
		h := bedrockagent.NewTestHandler(backend)
		h.AccountID = rtTestAccountID
		h.DefaultRegion = rtTestRegion
		client := newRoundTripClient(t, h)
		ctx := t.Context()

		kb, err := backend.CreateKnowledgeBase(ctx, bedrockagent.KnowledgeBaseConfig{
			Name:    "s23-resource-policy-kb",
			RoleARN: "arn:aws:iam::123456789012:role/kb-role",
		})
		require.NoError(t, err)

		policyDoc := slice23ResourcePolicyDoc("999999999999", kb.KnowledgeBaseARN)

		putOut, err := client.PutResourcePolicy(ctx, &bedrockagentsdk.PutResourcePolicyInput{
			ResourceArn: aws.String(kb.KnowledgeBaseARN),
			Policy:      aws.String(policyDoc),
		})
		require.NoError(t, err)
		assert.Equal(t, kb.KnowledgeBaseARN, aws.ToString(putOut.ResourceArn))
		require.NotEmpty(t, aws.ToString(putOut.RevisionId))
		firstRevision := aws.ToString(putOut.RevisionId)

		getOut, err := client.GetResourcePolicy(ctx, &bedrockagentsdk.GetResourcePolicyInput{
			ResourceArn: aws.String(kb.KnowledgeBaseARN),
		})
		require.NoError(t, err)
		assert.Equal(t, policyDoc, aws.ToString(getOut.Policy))
		assert.Equal(t, kb.KnowledgeBaseARN, aws.ToString(getOut.ResourceArn))
		assert.Equal(t, firstRevision, aws.ToString(getOut.RevisionId))

		// A stale expectedRevisionId must conflict, not silently overwrite.
		_, err = client.PutResourcePolicy(ctx, &bedrockagentsdk.PutResourcePolicyInput{
			ResourceArn:        aws.String(kb.KnowledgeBaseARN),
			Policy:             aws.String(policyDoc),
			ExpectedRevisionId: aws.String("stale-revision"),
		})
		require.Error(t, err, "put with a stale expectedRevisionId must conflict")

		updatedPolicyDoc := slice23ResourcePolicyDoc("888888888888", kb.KnowledgeBaseARN)

		putOut2, err := client.PutResourcePolicy(ctx, &bedrockagentsdk.PutResourcePolicyInput{
			ResourceArn:        aws.String(kb.KnowledgeBaseARN),
			Policy:             aws.String(updatedPolicyDoc),
			ExpectedRevisionId: aws.String(firstRevision),
		})
		require.NoError(t, err)
		secondRevision := aws.ToString(putOut2.RevisionId)
		assert.NotEqual(t, firstRevision, secondRevision)

		// A stale expectedRevisionId on delete must conflict, not delete.
		_, err = client.DeleteResourcePolicy(ctx, &bedrockagentsdk.DeleteResourcePolicyInput{
			ResourceArn:        aws.String(kb.KnowledgeBaseARN),
			ExpectedRevisionId: aws.String(firstRevision),
		})
		require.Error(t, err, "delete with a stale expectedRevisionId must conflict")

		delOut, err := client.DeleteResourcePolicy(ctx, &bedrockagentsdk.DeleteResourcePolicyInput{
			ResourceArn:        aws.String(kb.KnowledgeBaseARN),
			ExpectedRevisionId: aws.String(secondRevision),
		})
		require.NoError(t, err)
		assert.Equal(t, kb.KnowledgeBaseARN, aws.ToString(delOut.ResourceArn))
		assert.Equal(t, secondRevision, aws.ToString(delOut.RevisionId))

		_, err = client.GetResourcePolicy(ctx, &bedrockagentsdk.GetResourcePolicyInput{
			ResourceArn: aws.String(kb.KnowledgeBaseARN),
		})
		require.Error(t, err, "the policy no longer exists after delete")
	})

	t.Run("non-knowledge-base resourceArn is rejected", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		_, err := client.PutResourcePolicy(ctx, &bedrockagentsdk.PutResourcePolicyInput{
			ResourceArn: aws.String("arn:aws:bedrock:us-east-1:123456789012:agent/not-a-kb"),
			Policy:      aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
		require.Error(t, err, "resourceArn is documented as knowledge-base-only")
	})
}
