package backup_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestListCopyJobSummaries_AccountId proves ListCopyJobSummaries never
// emitted AccountId (real CopyJobSummary member, backup@v1.64.0 types.go),
// unlike its three sibling summary ops (ListBackupJobSummaries/
// ListRestoreJobSummaries/ListScanJobSummaries) which all already included
// it -- gopherstack-21my.
func TestListCopyJobSummaries_AccountId(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	_, err := backend.CreateBackupVault("cjs-src-vault", "", "", nil)
	require.NoError(t, err)
	dstVault, err := backend.CreateBackupVault("cjs-dst-vault", "", "", nil)
	require.NoError(t, err)

	_, err = backend.StartCopyJob("arn:rp", "cjs-src-vault", dstVault.BackupVaultArn, "arn:role")
	require.NoError(t, err)

	out, err := client.ListCopyJobSummaries(t.Context(), &backupsdk.ListCopyJobSummariesInput{})
	require.NoError(t, err)
	require.Len(t, out.CopyJobSummaries, 1)

	assert.Equal(t, "000000000000", aws.ToString(out.CopyJobSummaries[0].AccountId),
		"AccountId must be populated, not dropped")
}
