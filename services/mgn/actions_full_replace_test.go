package mgn_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutTemplateAction_FullReplaceSemantics documents why the 11 zeroguard
// rows on PutTemplateActionInput/PutSourceServerActionInput (cmd/zeroguard)
// are false positives, not bugs: ActionID/ActionName/DocumentIdentifier/
// Order/LaunchConfigurationTemplateID(or SourceServerID) are all "This
// member is required" on the real SDK request (api_op_PutTemplateAction.go/
// api_op_PutSourceServerAction.go), so omission of those is impossible via a
// real client. The remaining optional fields (Active, Description,
// DocumentVersion, MustSucceedForCutover, OperatingSystem, TimeoutSeconds)
// are correctly full-replaced on every call, matching Put*'s "creates or
// replaces" semantics (like PutKeyPolicy/PutBucketPolicy elsewhere in AWS) --
// a second Put with the same ActionID but without a previously-set optional
// field must reset it, not preserve it. This test locks that behavior in so
// nobody "fixes" it into an Update-style partial-preserve merge later.
func TestPutTemplateAction_FullReplaceSemantics(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	tmpl, err := client.CreateLaunchConfigurationTemplate(ctx, &mgnsdk.CreateLaunchConfigurationTemplateInput{})
	require.NoError(t, err)

	first, err := client.PutTemplateAction(ctx, &mgnsdk.PutTemplateActionInput{
		ActionID:                      aws.String("fr-action"),
		ActionName:                    aws.String("first-name"),
		DocumentIdentifier:            aws.String("AWS-RunShellScript"),
		Order:                         aws.Int32(1),
		LaunchConfigurationTemplateID: tmpl.LaunchConfigurationTemplateID,
		Description:                   aws.String("first description"),
		Active:                        aws.Bool(true),
		MustSucceedForCutover:         aws.Bool(true),
		TimeoutSeconds:                aws.Int32(60),
	})
	require.NoError(t, err)
	assert.Equal(t, "first description", aws.ToString(first.Description))
	assert.True(t, aws.ToBool(first.Active))

	// A second Put for the same ActionID that omits the optional fields must
	// replace the whole document, resetting them -- not preserve them.
	second, err := client.PutTemplateAction(ctx, &mgnsdk.PutTemplateActionInput{
		ActionID:                      aws.String("fr-action"),
		ActionName:                    aws.String("second-name"),
		DocumentIdentifier:            aws.String("AWS-RunShellScript"),
		Order:                         aws.Int32(2),
		LaunchConfigurationTemplateID: tmpl.LaunchConfigurationTemplateID,
	})
	require.NoError(t, err)
	assert.Equal(t, "second-name", aws.ToString(second.ActionName))
	assert.Empty(t, aws.ToString(second.Description), "Put must replace, not preserve, omitted optional fields")
	assert.False(t, aws.ToBool(second.Active))
	assert.False(t, aws.ToBool(second.MustSucceedForCutover))
	assert.Zero(t, aws.ToInt32(second.TimeoutSeconds))
}
