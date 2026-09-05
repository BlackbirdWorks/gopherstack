package transfer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	transfersdk "github.com/aws/aws-sdk-go-v2/service/transfer"
	"github.com/aws/aws-sdk-go-v2/service/transfer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestListFileTransferResults_OneRowPerFile_RealClient covers gopherstack-tp8x:
// handleListFileTransferResults used to emit one row per TRANSFER, with a
// "FilePaths" array of every file in it. The real per-item member
// (transfer@v1.75.4 types.ConnectorFileTransferResult) is a singular
// "FilePath" string -- each real result row covers exactly one file. This is
// a cardinality bug, not a rename: a single-file transfer would pass either
// shape, so this test starts a transfer with THREE files and asserts three
// separate rows come back, each with its own FilePath, decoded through the
// real SDK client (which would silently drop an unknown "FilePaths" key with
// no error, so decoding into the typed FilePath field is the proof).
func TestListFileTransferResults_OneRowPerFile_RealClient(t *testing.T) {
	t.Parallel()

	backend := transfer.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestTransferClient(t, transfer.NewHandler(backend))
	ctx := t.Context()

	conn, err := client.CreateConnector(ctx, &transfersdk.CreateConnectorInput{
		Url:        aws.String("sftp://example.com"),
		AccessRole: aws.String("arn:aws:iam::123456789012:role/transfer"),
	})
	require.NoError(t, err)

	files := []string{"/a/one.txt", "/a/two.txt", "/a/three.txt"}

	started, err := client.StartFileTransfer(ctx, &transfersdk.StartFileTransferInput{
		ConnectorId:   conn.ConnectorId,
		SendFilePaths: files,
	})
	require.NoError(t, err)

	out, err := client.ListFileTransferResults(ctx, &transfersdk.ListFileTransferResultsInput{
		ConnectorId: conn.ConnectorId,
		TransferId:  started.TransferId,
	})
	require.NoError(t, err)

	require.Len(t, out.FileTransferResults, len(files),
		"one row per file: a 3-file transfer must produce 3 result rows, not 1 row listing 3 paths")

	gotPaths := make([]string, len(out.FileTransferResults))
	for i, r := range out.FileTransferResults {
		gotPaths[i] = aws.ToString(r.FilePath)
		assert.Equal(t, types.TransferTableStatusQueued, r.StatusCode)
	}

	assert.ElementsMatch(t, files, gotPaths)
}

// TestListFileTransferResults_SingleFile_RealClient guards against the
// single-file case masking the cardinality bug (it passes under either
// shape) by pinning the exact FilePath value through the real client.
func TestListFileTransferResults_SingleFile_RealClient(t *testing.T) {
	t.Parallel()

	backend := transfer.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestTransferClient(t, transfer.NewHandler(backend))
	ctx := t.Context()

	conn, err := client.CreateConnector(ctx, &transfersdk.CreateConnectorInput{
		Url:        aws.String("sftp://example.com"),
		AccessRole: aws.String("arn:aws:iam::123456789012:role/transfer"),
	})
	require.NoError(t, err)

	started, err := client.StartFileTransfer(ctx, &transfersdk.StartFileTransferInput{
		ConnectorId:   conn.ConnectorId,
		SendFilePaths: []string{"/only/file.txt"},
	})
	require.NoError(t, err)

	out, err := client.ListFileTransferResults(ctx, &transfersdk.ListFileTransferResultsInput{
		ConnectorId: conn.ConnectorId,
		TransferId:  started.TransferId,
	})
	require.NoError(t, err)
	require.Len(t, out.FileTransferResults, 1)
	assert.Equal(t, "/only/file.txt", aws.ToString(out.FileTransferResults[0].FilePath))
}

// TestListFileTransferResults_SDKRoundTrip_Pagination drives the real SDK client across two
// pages of ListFileTransferResults and asserts the pages are disjoint and the marker
// round-trips. Before the fix, handleListFileTransferResults ignored MaxResults/NextToken
// (both real ListFileTransferResultsInput members) and always returned every file transferred
// in one unbounded page.
func TestListFileTransferResults_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := transfer.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestTransferClient(t, transfer.NewHandler(backend))
	ctx := t.Context()

	conn, err := client.CreateConnector(ctx, &transfersdk.CreateConnectorInput{
		Url:        aws.String("sftp://example.com"),
		AccessRole: aws.String("arn:aws:iam::123456789012:role/transfer"),
	})
	require.NoError(t, err)

	files := []string{"/a/one.txt", "/a/two.txt", "/a/three.txt", "/a/four.txt"}

	started, err := client.StartFileTransfer(ctx, &transfersdk.StartFileTransferInput{
		ConnectorId:   conn.ConnectorId,
		SendFilePaths: files,
	})
	require.NoError(t, err)

	page1, err := client.ListFileTransferResults(ctx, &transfersdk.ListFileTransferResultsInput{
		ConnectorId: conn.ConnectorId,
		TransferId:  started.TransferId,
		MaxResults:  aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.FileTransferResults, 2)
	require.NotNil(t, page1.NextToken)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListFileTransferResults(ctx, &transfersdk.ListFileTransferResultsInput{
		ConnectorId: conn.ConnectorId,
		TransferId:  started.TransferId,
		MaxResults:  aws.Int32(2),
		NextToken:   page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.FileTransferResults, 2)

	seen := make(map[string]bool, 4)
	for _, r := range page1.FileTransferResults {
		seen[aws.ToString(r.FilePath)] = true
	}

	for _, r := range page2.FileTransferResults {
		assert.False(t, seen[aws.ToString(r.FilePath)], "page 2 repeated file %s from page 1", aws.ToString(r.FilePath))
		seen[aws.ToString(r.FilePath)] = true
	}

	assert.Len(t, seen, 4)
}
