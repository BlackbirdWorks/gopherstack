package backup_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// Test_CreateBackupSelection_UnknownPlanIsInvalidParameterValue proves that
// CreateBackupSelection's unknown-BackupPlanId path is wire-shape-wrong.
// The real operation's own deserializeOpError switch (deserializers.go)
// recognizes AlreadyExistsException, InvalidParameterValueException,
// LimitExceededException, MissingParameterValueException and
// ServiceUnavailableException -- it has no ResourceNotFoundException case
// at all. gopherstack's backend (selections.go CreateBackupSelection) wraps
// the shared ErrNotFound sentinel for an unresolved plan ID/name, which
// handleError renders as ResourceNotFoundException -- a code this
// operation's real deserializer switch never matches, so it falls to the
// switch's default case and produces a *smithy.GenericAPIError instead of
// any typed exception.
func Test_CreateBackupSelection_UnknownPlanIsInvalidParameterValue(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	_, err := client.CreateBackupSelection(
		t.Context(),
		&backupsdk.CreateBackupSelectionInput{
			BackupPlanId: aws.String("no-such-plan"),
			BackupSelection: &types.BackupSelection{
				SelectionName: aws.String("sel"),
				IamRoleArn:    aws.String("arn:aws:iam::000000000000:role/BackupRole"),
			},
		},
	)
	require.Error(t, err)

	var ipv *types.InvalidParameterValueException
	require.ErrorAs(t, err, &ipv,
		"expected a typed InvalidParameterValueException, got: %v", err)
}
