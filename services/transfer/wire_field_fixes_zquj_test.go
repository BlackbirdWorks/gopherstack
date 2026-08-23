package transfer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	transfersdk "github.com/aws/aws-sdk-go-v2/service/transfer"
	"github.com/stretchr/testify/require"
)

// TestListHostKeys_FingerprintKey_RealSDKClient proves gopherstack-zquj:
// handleListHostKeys wrote each item's fingerprint under "HostKeyFingerprint"
// -- DescribedHostKey's (DescribeHostKey) member name -- but the real
// ListedHostKey deserializer switches on "Fingerprint"
// (deserializers.go's awsRestjson1_deserializeDocumentListedHostKey,
// transfer SDK), so every real client's ListHostKeys().Fingerprint decoded
// empty regardless of what ImportHostKey computed.
func TestListHostKeys_FingerprintKey_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestTransferClient(t, h)
	ctx := t.Context()

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	_, err = client.ImportHostKey(ctx, &transfersdk.ImportHostKeyInput{
		ServerId:    srv.ServerId,
		HostKeyBody: aws.String(testHostKeyEd25519),
	})
	require.NoError(t, err)

	out, err := client.ListHostKeys(ctx, &transfersdk.ListHostKeysInput{ServerId: srv.ServerId})
	require.NoError(t, err)
	require.Len(t, out.HostKeys, 1)
	require.NotEmpty(t, aws.ToString(out.HostKeys[0].Fingerprint))
}
