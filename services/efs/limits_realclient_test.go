package efs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	efssdktypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestSDKRoundTrip_FileSystemLimitExceeded drives CreateFileSystem through
// the real aws-sdk-go-v2 client against httptest, past a lowered
// WithResourceLimits cap, and proves the resulting FileSystemLimitExceeded
// round-trips through the real REST-JSON deserializer
// (efssdktypes.FileSystemLimitExceeded, HTTP 403) rather than only through
// the hand-rolled JSON assertions in limits_test.go. EFS is REST-JSON: the
// deserializer keys off the X-Amzn-ErrorType header (see
// awsRestjson1_deserializeOpErrorCreateFileSystem reading
// response.Header.Get("X-Amzn-ErrorType") before falling back to the body's
// __type/code), which is exactly the header handleError sets (handler.go).
// Covers gopherstack-ne9h.
func TestSDKRoundTrip_FileSystemLimitExceeded(t *testing.T) {
	t.Parallel()

	client, h := newWireTestClient(t)
	h.Backend.WithResourceLimits(efs.ResourceLimits{FileSystemsPerAccount: 1})

	_, err := client.CreateFileSystem(t.Context(), &efssdk.CreateFileSystemInput{
		CreationToken: aws.String("sdk-fsle-1"),
	})
	require.NoError(t, err)

	_, err = client.CreateFileSystem(t.Context(), &efssdk.CreateFileSystemInput{
		CreationToken: aws.String("sdk-fsle-2"),
	})
	require.Error(t, err)

	var fsle *efssdktypes.FileSystemLimitExceeded
	require.ErrorAs(t, err, &fsle, "CreateFileSystem over the limit must decode as FileSystemLimitExceeded")
}

// TestSDKRoundTrip_AccessPointLimitExceeded is
// TestSDKRoundTrip_FileSystemLimitExceeded's CreateAccessPoint counterpart,
// proving efssdktypes.AccessPointLimitExceeded round-trips through the real
// SDK deserializer.
func TestSDKRoundTrip_AccessPointLimitExceeded(t *testing.T) {
	t.Parallel()

	client, h := newWireTestClient(t)
	h.Backend.WithResourceLimits(efs.ResourceLimits{AccessPointsPerFileSys: 1})

	fsOut, err := client.CreateFileSystem(t.Context(), &efssdk.CreateFileSystemInput{
		CreationToken: aws.String("sdk-aple-fs"),
	})
	require.NoError(t, err)

	_, err = client.CreateAccessPoint(t.Context(), &efssdk.CreateAccessPointInput{
		FileSystemId: fsOut.FileSystemId,
	})
	require.NoError(t, err)

	_, err = client.CreateAccessPoint(t.Context(), &efssdk.CreateAccessPointInput{
		FileSystemId: fsOut.FileSystemId,
	})
	require.Error(t, err)

	var aple *efssdktypes.AccessPointLimitExceeded
	require.ErrorAs(t, err, &aple, "CreateAccessPoint over the limit must decode as AccessPointLimitExceeded")
}
