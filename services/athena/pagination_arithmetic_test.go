package athena_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

// TestListWorkGroups_RealClient_BoundaryWalk confirms, through the real
// aws-sdk-go-v2 client (not just raw JSON), that paginationStart's
// threshold-search cursor (already the appmesh-style safe-by-construction
// pattern: sort.Search for the first key >= boundary, defaulting a full miss
// to n rather than 0) walks a full ListWorkGroups collection without
// dropping or duplicating entries.
func TestListWorkGroups_RealClient_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	h := athena.NewHandler(b)
	client := newTestAthenaClient(t, h)

	const n = 7
	for i := range n {
		_, err := client.CreateWorkGroup(t.Context(), &athenasdk.CreateWorkGroupInput{
			Name: aws.String(fmt.Sprintf("wg-%03d", i)),
		})
		require.NoError(t, err)
	}

	var got []string

	var token *string
	for range n + 2 { // +primary default workgroup, +1 slack
		out, err := client.ListWorkGroups(t.Context(), &athenasdk.ListWorkGroupsInput{
			MaxResults: aws.Int32(3),
			NextToken:  token,
		})
		require.NoError(t, err)

		for _, wg := range out.WorkGroups {
			got = append(got, aws.ToString(wg.Name))
		}

		token = out.NextToken
		if aws.ToString(token) == "" {
			break
		}
	}

	for i := range n {
		assert.Contains(t, got, fmt.Sprintf("wg-%03d", i))
	}

	assert.Contains(t, got, "primary", "the default workgroup must also appear")
	assert.Len(t, got, n+1, "no duplicates across the walk")
}
