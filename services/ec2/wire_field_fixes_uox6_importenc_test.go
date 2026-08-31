package ec2_test

// uox6-importenc: ImportImage and ImportSnapshot both declare Encrypted and
// KmsKeyId ("Specifies whether the destination AMI ... should be
// encrypted. The default KMS key for EBS is used unless you specify a
// non-default ... KMS key using KmsKeyId." -- api_op_ImportImage.go,
// ec2@v1.319.1; the analogous pair on ImportSnapshot's SnapshotTaskDetail),
// echoed straight back on the immediate response AND on the matching
// Describe*ImportTasks list item (ImportImageTask/SnapshotTaskDetail both
// carry the same two fields in types.go) -- but the handler read neither,
// so every import silently came back unencrypted with no KMS key
// regardless of what the caller asked for.
//
// RoleName is also declared on both operations with a real, unambiguous
// documented default ("...when not using the default role, 'vmimport'.")
// but is deliberately NOT wired here: it appears nowhere in
// ImportImageOutput, ImportSnapshotOutput or SnapshotTaskDetail in the
// pinned SDK, and this backend does not simulate IAM role assumption for
// import tasks at all, so storing it would be inert -- no test, and no
// real client, could ever observe a difference. Recorded rather than
// fabricated.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportImage_EncryptedKmsKeyId(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	out, err := client.ImportImage(t.Context(), &ec2sdk.ImportImageInput{
		Description: aws.String("uox6 encrypted import"),
		Encrypted:   aws.Bool(true),
		KmsKeyId:    aws.String("alias/uox6-test-key"),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(out.Encrypted))
	assert.Equal(t, "alias/uox6-test-key", aws.ToString(out.KmsKeyId))

	tasks, err := client.DescribeImportImageTasks(t.Context(), &ec2sdk.DescribeImportImageTasksInput{
		ImportTaskIds: []string{aws.ToString(out.ImportTaskId)},
	})
	require.NoError(t, err)
	require.Len(t, tasks.ImportImageTasks, 1)
	assert.True(t, aws.ToBool(tasks.ImportImageTasks[0].Encrypted))
	assert.Equal(t, "alias/uox6-test-key", aws.ToString(tasks.ImportImageTasks[0].KmsKeyId))
}

func TestImportImage_EncryptedOmitted_DefaultsFalse(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	out, err := client.ImportImage(t.Context(), &ec2sdk.ImportImageInput{
		Description: aws.String("uox6 plain import"),
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(out.Encrypted))
	assert.Empty(t, aws.ToString(out.KmsKeyId))
}

func TestImportSnapshot_EncryptedKmsKeyId(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	out, err := client.ImportSnapshot(t.Context(), &ec2sdk.ImportSnapshotInput{
		Description: aws.String("uox6 encrypted snapshot import"),
		Encrypted:   aws.Bool(true),
		KmsKeyId:    aws.String("alias/uox6-test-key"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.SnapshotTaskDetail)
	assert.True(t, aws.ToBool(out.SnapshotTaskDetail.Encrypted))
	assert.Equal(t, "alias/uox6-test-key", aws.ToString(out.SnapshotTaskDetail.KmsKeyId))

	tasks, err := client.DescribeImportSnapshotTasks(t.Context(), &ec2sdk.DescribeImportSnapshotTasksInput{
		ImportTaskIds: []string{aws.ToString(out.ImportTaskId)},
	})
	require.NoError(t, err)
	require.Len(t, tasks.ImportSnapshotTasks, 1)
	require.NotNil(t, tasks.ImportSnapshotTasks[0].SnapshotTaskDetail)
	assert.True(t, aws.ToBool(tasks.ImportSnapshotTasks[0].SnapshotTaskDetail.Encrypted))
	assert.Equal(t, "alias/uox6-test-key", aws.ToString(tasks.ImportSnapshotTasks[0].SnapshotTaskDetail.KmsKeyId))
}

func TestImportImage_EncryptedNoKmsKeyId_UsesDefaultEBSKey(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	out, err := client.ImportImage(t.Context(), &ec2sdk.ImportImageInput{
		Description: aws.String("uox6 encrypted default key import"),
		Encrypted:   aws.Bool(true),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(out.Encrypted))
	assert.Equal(t, "alias/aws/ebs", aws.ToString(out.KmsKeyId))
}

func TestImportSnapshot_EncryptedOmitted_DefaultsFalse(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	out, err := client.ImportSnapshot(t.Context(), &ec2sdk.ImportSnapshotInput{
		Description: aws.String("uox6 plain snapshot import"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.SnapshotTaskDetail)
	assert.False(t, aws.ToBool(out.SnapshotTaskDetail.Encrypted))
	assert.Empty(t, aws.ToString(out.SnapshotTaskDetail.KmsKeyId))
}
