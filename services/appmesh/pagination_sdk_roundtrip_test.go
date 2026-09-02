package appmesh_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appmeshsdk "github.com/aws/aws-sdk-go-v2/service/appmesh"
	"github.com/aws/aws-sdk-go-v2/service/appmesh/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListMeshes_SDKRoundTrip_StaleCursorResumesPastDeletedItem drives
// ListMeshes through the real aws-sdk-go-v2/service/appmesh client to prove
// paginateStrings (services/appmesh/store.go, shared by 7 List operations):
// a nextToken naming the last mesh seen on a page, when that mesh is
// deleted before the next page is fetched, must resume at the next
// surviving name -- never restart the walk, never skip a survivor.
func TestListMeshes_SDKRoundTrip_StaleCursorResumesPastDeletedItem(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	// paginateStrings' token names the LAST item already returned (not the
	// first item of the next page), so a resume actually goes stale only
	// when an item BETWEEN the cursor and the next page is deleted -- here,
	// mesh-c, sorting between the page1 cursor (mesh-b) and the survivor
	// (mesh-d).
	names := []string{"mesh-a", "mesh-b", "mesh-c", "mesh-d"}
	for _, n := range names {
		_, err := client.CreateMesh(t.Context(), &appmeshsdk.CreateMeshInput{MeshName: aws.String(n)})
		require.NoError(t, err)
	}

	page1, err := client.ListMeshes(t.Context(), &appmeshsdk.ListMeshesInput{Limit: aws.Int32(2)})
	require.NoError(t, err)
	require.Equal(t, []string{"mesh-a", "mesh-b"}, meshNames(page1.Meshes))
	require.NotNil(t, page1.NextToken)

	staleToken := aws.ToString(page1.NextToken)
	require.Equal(t, "mesh-b", staleToken)

	_, err = client.DeleteMesh(t.Context(), &appmeshsdk.DeleteMeshInput{MeshName: aws.String("mesh-c")})
	require.NoError(t, err)

	page2, err := client.ListMeshes(t.Context(), &appmeshsdk.ListMeshesInput{
		Limit:     aws.Int32(10),
		NextToken: aws.String(staleToken),
	})
	require.NoError(t, err)

	page2Names := meshNames(page2.Meshes)

	const resetMsg = "a stale cursor must not re-return page1's meshes -- pagination reset to page one"
	assert.NotContains(t, page2Names, "mesh-a", resetMsg)
	assert.NotContains(t, page2Names, "mesh-b", resetMsg)
	assert.NotContains(t, page2Names, "mesh-c", "the deleted mesh itself must not reappear")
	assert.Equal(t, []string{"mesh-d"}, page2Names, "exactly the surviving mesh after the deleted one")
}

func meshNames(meshes []types.MeshRef) []string {
	out := make([]string, 0, len(meshes))
	for _, m := range meshes {
		out = append(out, aws.ToString(m.MeshName))
	}

	return out
}
