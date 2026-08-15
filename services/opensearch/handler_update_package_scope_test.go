package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// Test_SDKRoundTrip_UpdatePackageScope proves that PackageUserList -- the
// real op's only required member beyond PackageID/Operation
// (api_op_UpdatePackageScope.go:29-48, opensearch@v1.75.4) -- is actually
// read and echoed on the response. Before the fix, the handler decoded the
// list under a "PackageScopeOperationConfig" JSON key that the real SDK
// never sends, so PackageUserList silently deserialized to nil on every real
// request; the response also fabricated a PackageScopeOperationStatus field
// that is not part of the real wire shape at all, and never returned
// PackageUserList back to the caller.
func Test_SDKRoundTrip_UpdatePackageScope(t *testing.T) {
	t.Parallel()

	backend := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	h := opensearch.NewHandler(backend)
	client := newTestOpenSearchClient(t, h)

	created, err := client.CreatePackage(t.Context(), &opensearchsdk.CreatePackageInput{
		PackageName: aws.String("scope-pkg"),
		PackageType: types.PackageTypeTxtDictionary,
		PackageSource: &types.PackageSource{
			S3BucketName: aws.String("bucket"),
			S3Key:        aws.String("key"),
		},
	})
	require.NoError(t, err)
	pkgID := created.PackageDetails.PackageID

	added, err := client.UpdatePackageScope(t.Context(), &opensearchsdk.UpdatePackageScopeInput{
		PackageID:       pkgID,
		Operation:       types.PackageScopeOperationEnumAdd,
		PackageUserList: []string{"alice", "bob"},
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"alice", "bob"}, added.PackageUserList)
	assert.Equal(t, types.PackageScopeOperationEnumAdd, added.Operation)
	assert.Equal(t, aws.ToString(pkgID), aws.ToString(added.PackageID))

	removed, err := client.UpdatePackageScope(t.Context(), &opensearchsdk.UpdatePackageScopeInput{
		PackageID:       pkgID,
		Operation:       types.PackageScopeOperationEnumRemove,
		PackageUserList: []string{"alice"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"bob"}, removed.PackageUserList)

	overridden, err := client.UpdatePackageScope(t.Context(), &opensearchsdk.UpdatePackageScopeInput{
		PackageID:       pkgID,
		Operation:       types.PackageScopeOperationEnumOverride,
		PackageUserList: []string{"carol"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"carol"}, overridden.PackageUserList)
}
