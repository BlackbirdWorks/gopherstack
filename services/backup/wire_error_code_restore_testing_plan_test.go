package backup_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// Test_DeleteRestoreTestingPlan_UnknownPlanIsInvalidRequest proves that
// DeleteRestoreTestingPlan's not-found path is wire-shape-wrong. The real
// operation's own deserializeOpError switch (deserializers.go) recognizes
// only InvalidRequestException and ServiceUnavailableException -- it has no
// ResourceNotFoundException case at all, unlike almost every sibling
// Delete/Describe op in this service. gopherstack's backend
// (restore_testing.go DeleteRestoreTestingPlan) wraps the shared ErrNotFound
// sentinel on an unknown plan name, which handleError renders as
// ResourceNotFoundException -- a code this operation's real deserializer
// switch never matches, so it falls to the switch's default case and
// produces a *smithy.GenericAPIError instead of any typed exception. A real
// client's errors.As(&types.ResourceNotFoundException{}) branch can never
// fire for this operation; InvalidRequestException is the only client-fault
// type this op declares.
func Test_DeleteRestoreTestingPlan_UnknownPlanIsInvalidRequest(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	_, err := client.DeleteRestoreTestingPlan(
		t.Context(),
		&backupsdk.DeleteRestoreTestingPlanInput{
			RestoreTestingPlanName: aws.String("no-such-plan"),
		},
	)
	require.Error(t, err)

	var ire *types.InvalidRequestException
	require.ErrorAs(t, err, &ire,
		"expected a typed InvalidRequestException, got: %v", err)
}
