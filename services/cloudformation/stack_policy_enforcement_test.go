package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/require"
)

// stackPolicyBaseTemplate declares three resources exercised by the stack
// policy enforcement tests below: MyBucket (BucketName is a
// replacement-forcing property per changeset_diff.go's requiresRecreation,
// so changing it classifies as Update:Replace), MyQueue (VisibilityTimeout is
// not replacement-forcing, so changing it classifies as Update:Modify), and
// OtherQueue (present so it can be dropped from the template entirely, which
// classifies as Update:Delete).
const stackPolicyBaseTemplate = `{"Resources":{
	"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig-bucket"}},
	"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":30}},
	"OtherQueue":{"Type":"AWS::SQS::Queue","Properties":{}}
}}`

func allowAllThenDeny(action, logicalID string) string {
	return `{"Statement":[` +
		`{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"},` +
		`{"Effect":"Deny","Action":"` + action + `","Principal":"*","Resource":"LogicalResourceId/` + logicalID + `"}` +
		`]}`
}

func createPolicyStack(t *testing.T, client *cfnsdk.Client, stackName string) {
	t.Helper()

	_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(stackPolicyBaseTemplate),
	})
	require.NoError(t, err)
}

func TestUpdateStack_StackPolicyEnforcement(t *testing.T) {
	t.Parallel()

	t.Run("deny delete blocks resource removal", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClientWithBackend(t)
		createPolicyStack(t, client, "cqy3-delete")

		_, err := client.SetStackPolicy(t.Context(), &cfnsdk.SetStackPolicyInput{
			StackName:       aws.String("cqy3-delete"),
			StackPolicyBody: aws.String(allowAllThenDeny("Update:Delete", "OtherQueue")),
		})
		require.NoError(t, err)

		withoutOtherQueue := `{"Resources":{
			"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig-bucket"}},
			"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":30}}
		}}`

		_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
			StackName:    aws.String("cqy3-delete"),
			TemplateBody: aws.String(withoutOtherQueue),
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "Update:Delete")
		require.ErrorContains(t, err, "OtherQueue")

		out, err := client.DescribeStackResource(t.Context(), &cfnsdk.DescribeStackResourceInput{
			StackName:         aws.String("cqy3-delete"),
			LogicalResourceId: aws.String("OtherQueue"),
		})
		require.NoError(t, err, "OtherQueue must still exist: the denied delete must not have applied")
		require.Equal(t, "OtherQueue", aws.ToString(out.StackResourceDetail.LogicalResourceId))
	})

	t.Run("deny replace blocks a replacement-forcing property change", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClientWithBackend(t)
		createPolicyStack(t, client, "cqy3-replace")

		_, err := client.SetStackPolicy(t.Context(), &cfnsdk.SetStackPolicyInput{
			StackName:       aws.String("cqy3-replace"),
			StackPolicyBody: aws.String(allowAllThenDeny("Update:Replace", "MyBucket")),
		})
		require.NoError(t, err)

		renamedBucket := `{"Resources":{
			"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"new-bucket"}},
			"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":30}},
			"OtherQueue":{"Type":"AWS::SQS::Queue","Properties":{}}
		}}`

		_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
			StackName:    aws.String("cqy3-replace"),
			TemplateBody: aws.String(renamedBucket),
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "Update:Replace")
		require.ErrorContains(t, err, "MyBucket")

		tmpl, err := client.GetTemplate(t.Context(), &cfnsdk.GetTemplateInput{
			StackName: aws.String("cqy3-replace"),
		})
		require.NoError(t, err)
		require.Contains(
			t, aws.ToString(tmpl.TemplateBody), "orig-bucket",
			"template must be unchanged: the denied replace must not have applied",
		)
	})

	t.Run("deny modify blocks an in place property change", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClientWithBackend(t)
		createPolicyStack(t, client, "cqy3-modify")

		_, err := client.SetStackPolicy(t.Context(), &cfnsdk.SetStackPolicyInput{
			StackName:       aws.String("cqy3-modify"),
			StackPolicyBody: aws.String(allowAllThenDeny("Update:Modify", "MyQueue")),
		})
		require.NoError(t, err)

		changedTimeout := `{"Resources":{
			"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig-bucket"}},
			"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":600}},
			"OtherQueue":{"Type":"AWS::SQS::Queue","Properties":{}}
		}}`

		_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
			StackName:    aws.String("cqy3-modify"),
			TemplateBody: aws.String(changedTimeout),
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "Update:Modify")
		require.ErrorContains(t, err, "MyQueue")

		tmpl, err := client.GetTemplate(t.Context(), &cfnsdk.GetTemplateInput{
			StackName: aws.String("cqy3-modify"),
		})
		require.NoError(t, err)
		require.Contains(t, aws.ToString(tmpl.TemplateBody), `"VisibilityTimeout":30`)
	})

	t.Run("a permitted update still succeeds under the same policy", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClientWithBackend(t)
		createPolicyStack(t, client, "cqy3-allowed")

		// Denies only Update:Delete on OtherQueue; Update:Modify on OtherQueue
		// remains covered by the blanket Allow statement.
		_, err := client.SetStackPolicy(t.Context(), &cfnsdk.SetStackPolicyInput{
			StackName:       aws.String("cqy3-allowed"),
			StackPolicyBody: aws.String(allowAllThenDeny("Update:Delete", "OtherQueue")),
		})
		require.NoError(t, err)

		modifiedOtherQueue := `{"Resources":{
			"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig-bucket"}},
			"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":30}},
			"OtherQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":45}}
		}}`

		_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
			StackName:    aws.String("cqy3-allowed"),
			TemplateBody: aws.String(modifiedOtherQueue),
		})
		require.NoError(t, err, "a stack policy denying only Update:Delete must not block Update:Modify")
	})

	t.Run("default deny once a policy is set blocks an unmentioned resource", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClientWithBackend(t)
		createPolicyStack(t, client, "cqy3-defaultdeny")

		// Only a Deny statement for MyBucket -- no blanket Allow anywhere.
		// Per AWS docs, once any policy is set, every resource is protected
		// by default: an update to MyQueue (never mentioned) must still be
		// denied because nothing explicitly allows it.
		onlyDenyStatement := `{"Statement":[` +
			`{"Effect":"Deny","Action":"Update:*","Principal":"*","Resource":"LogicalResourceId/MyBucket"}` +
			`]}`
		_, err := client.SetStackPolicy(t.Context(), &cfnsdk.SetStackPolicyInput{
			StackName:       aws.String("cqy3-defaultdeny"),
			StackPolicyBody: aws.String(onlyDenyStatement),
		})
		require.NoError(t, err)

		changedTimeout := `{"Resources":{
			"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig-bucket"}},
			"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":600}},
			"OtherQueue":{"Type":"AWS::SQS::Queue","Properties":{}}
		}}`

		_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
			StackName:    aws.String("cqy3-defaultdeny"),
			TemplateBody: aws.String(changedTimeout),
		})
		require.Error(t, err, "default must be deny once a policy exists, even for a resource no statement names")
	})

	t.Run("no policy set at all allows every update", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClientWithBackend(t)
		createPolicyStack(t, client, "cqy3-nopolicy")

		withoutOtherQueue := `{"Resources":{
			"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig-bucket"}},
			"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":30}}
		}}`

		_, err := client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
			StackName:    aws.String("cqy3-nopolicy"),
			TemplateBody: aws.String(withoutOtherQueue),
		})
		require.NoError(t, err, "with no stack policy ever set, all update actions remain allowed")
	})

	t.Run("during update override permits a normally denied delete without persisting", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClientWithBackend(t)
		createPolicyStack(t, client, "cqy3-override")

		_, err := client.SetStackPolicy(t.Context(), &cfnsdk.SetStackPolicyInput{
			StackName:       aws.String("cqy3-override"),
			StackPolicyBody: aws.String(allowAllThenDeny("Update:Delete", "OtherQueue")),
		})
		require.NoError(t, err)

		withoutOtherQueue := `{"Resources":{
			"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig-bucket"}},
			"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":30}}
		}}`
		allowAll := `{"Statement":[{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"}]}`

		_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
			StackName:                   aws.String("cqy3-override"),
			TemplateBody:                aws.String(withoutOtherQueue),
			StackPolicyDuringUpdateBody: aws.String(allowAll),
		})
		require.NoError(t, err, "an override policy allowing the action must let this one update through")

		got, err := client.GetStackPolicy(t.Context(), &cfnsdk.GetStackPolicyInput{
			StackName: aws.String("cqy3-override"),
		})
		require.NoError(t, err)
		require.Contains(
			t, aws.ToString(got.StackPolicyBody), "Update:Delete",
			"the stored policy must be unchanged -- the override applies to this call only",
		)

		// A second stack update, without the override, must still be denied
		// for a still-protected resource (MyBucket wasn't touched above, so
		// it remains under the original policy's blanket Allow, but proving
		// the persisted policy is unchanged means a fresh delete attempt on
		// a still-present protected resource is denied again).
		recreated := `{"Resources":{
			"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig-bucket"}},
			"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":30}},
			"OtherQueue":{"Type":"AWS::SQS::Queue","Properties":{}}
		}}`
		_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
			StackName:                   aws.String("cqy3-override"),
			TemplateBody:                aws.String(recreated),
			StackPolicyDuringUpdateBody: aws.String(allowAll),
		})
		require.NoError(t, err, "re-adding OtherQueue under the override policy")

		_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
			StackName:    aws.String("cqy3-override"),
			TemplateBody: aws.String(withoutOtherQueue),
		})
		require.Error(t, err, "without the override, the persisted Deny on OtherQueue must apply again")
	})

	t.Run("set stack policy rejects malformed json", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClientWithBackend(t)
		createPolicyStack(t, client, "cqy3-malformed")

		_, err := client.SetStackPolicy(t.Context(), &cfnsdk.SetStackPolicyInput{
			StackName:       aws.String("cqy3-malformed"),
			StackPolicyBody: aws.String(`{"Statement":[{"Effect":`),
		})
		require.Error(t, err)
	})
}
