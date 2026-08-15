package fsx_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real FSx
// operation, extracted from fsx@v1.68.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AWSSimbaAPIService_v20180301.<Op>")
// and always POSTs to "/" -- FSx is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. "AWSSimbaAPIService_v20180301."
// is FSx's internal codename target prefix -- not guessable from the
// service name "FSx" or "fsx", confirmed only by reading the pinned SDK's
// own serializers.go.
//
// ExtractOperation (TrimPrefix on "AWSSimbaAPIService_v20180301.") and
// Handler() (via pkgs/service.HandleTarget splitting on "." and taking
// parts[1], then dispatch()'s h.ops flat map lookup) both resolve to the
// identical action string, so the class of bug this table catches is a
// dispatch-table key that doesn't exactly match the real op name (typo,
// wrong case), not a route-template or splitting mismatch.
//
// This table covers all 48 real FSx ops (fsx@v1.68.4) -- confirmed by
// diffing both GetSupportedOperations() and buildOps()'s h.ops map keys
// against this exact list: zero mismatches in either direction, no dead or
// excluded keys. NOTE ON INDEPENDENCE: unlike cloudtrail/acm/acmpca, FSx's
// two lists are not independently-spelled string literals -- both
// GetSupportedOperations() and buildOps() reference the same op<Name>
// string constants (e.g. opCreateFileSystem = "CreateFileSystem"), so a
// typo in a constant's *string value* would appear identically in both and
// not be caught by diffing them against each other. What the two diffs DO
// independently catch is an *omission* -- a constant referenced in one
// list/map but not the other (there were none). The real independent check
// against the pinned SDK is this test itself: it drives the SDK's own
// target strings through ExtractOperation/Handler() rather than trusting
// gopherstack's internal constant values.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AWSSimbaAPIService_v20180301.` and
// pulling the suffix after the dot.
func sdkRouteCases() []string {
	return []string{
		"AssociateFileSystemAliases",
		"CancelDataRepositoryTask",
		"CopyBackup",
		"CopySnapshotAndUpdateVolume",
		"CreateAndAttachS3AccessPoint",
		"CreateBackup",
		"CreateDataRepositoryAssociation",
		"CreateDataRepositoryTask",
		"CreateFileCache",
		"CreateFileSystem",
		"CreateFileSystemFromBackup",
		"CreateSnapshot",
		"CreateStorageVirtualMachine",
		"CreateVolume",
		"CreateVolumeFromBackup",
		"DeleteBackup",
		"DeleteDataRepositoryAssociation",
		"DeleteFileCache",
		"DeleteFileSystem",
		"DeleteSnapshot",
		"DeleteStorageVirtualMachine",
		"DeleteVolume",
		"DescribeBackups",
		"DescribeDataRepositoryAssociations",
		"DescribeDataRepositoryTasks",
		"DescribeFileCaches",
		"DescribeFileSystemAliases",
		"DescribeFileSystems",
		"DescribeS3AccessPointAttachments",
		"DescribeSharedVpcConfiguration",
		"DescribeSnapshots",
		"DescribeStorageVirtualMachines",
		"DescribeVolumes",
		"DetachAndDeleteS3AccessPoint",
		"DisassociateFileSystemAliases",
		"ListTagsForResource",
		"ReleaseFileSystemNfsV3Locks",
		"RestoreVolumeFromSnapshot",
		"StartMisconfiguredStateRecovery",
		"TagResource",
		"UntagResource",
		"UpdateDataRepositoryAssociation",
		"UpdateFileCache",
		"UpdateFileSystem",
		"UpdateSharedVpcConfiguration",
		"UpdateSnapshot",
		"UpdateStorageVirtualMachine",
		"UpdateVolume",
	}
}

// TestExtractOperation_SDKRouteTable drives every real FSx operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the dispatch-miss branch (dispatch()'s h.ops
// lookup miss, returning errUnknownOperation, mapped by handleError to wire
// code "UnsupportedOperation"). Grepped handler.go: "UnsupportedOperation"
// is written in exactly that one handleError case
// (errors.Is(err, errUnknownOperation)) -- every other case in handleError
// covers a disjoint sentinel family (BackupNotFound, SnapshotNotFound,
// ServiceLimitExceeded, BadRequest, MissingFileSystemConfiguration,
// IncompatibleParameterError, InvalidNetworkSettings, InternalFailure) --
// so asserting on the wire type is safe here.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			h := fsx.NewHandler(fsx.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "AWSSimbaAPIService_v20180301."+op)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnsupportedOperation",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}
