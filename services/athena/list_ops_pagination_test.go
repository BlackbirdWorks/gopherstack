package athena_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/stretchr/testify/require"
)

// TestRealClient_ListOpsHonourMaxResults pages ops whose body MaxResults/
// NextToken members were previously ignored (serializers.go, athena@v1.60.4).
func TestRealClient_ListOpsHonourMaxResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed  func(t *testing.T, c *athenasdk.Client)
		fetch func(t *testing.T, c *athenasdk.Client, token *string) (int, *string)
		name  string
		total int
	}{
		{
			name: "capacity_reservations", total: 3,
			seed: func(t *testing.T, c *athenasdk.Client) {
				t.Helper()

				for _, n := range []string{"cr-a", "cr-b", "cr-c"} {
					_, err := c.CreateCapacityReservation(t.Context(), &athenasdk.CreateCapacityReservationInput{
						Name: aws.String(n), TargetDpus: aws.Int32(24),
					})
					require.NoError(t, err)
				}
			},
			fetch: func(t *testing.T, c *athenasdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListCapacityReservations(t.Context(), &athenasdk.ListCapacityReservationsInput{
					MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.CapacityReservations), out.NextToken
			},
		},
		{
			name: "notebook_metadata", total: 3,
			seed: func(t *testing.T, c *athenasdk.Client) {
				t.Helper()

				for _, n := range []string{"nb-a", "nb-b", "nb-c"} {
					_, err := c.CreateNotebook(t.Context(), &athenasdk.CreateNotebookInput{
						WorkGroup: aws.String("primary"), Name: aws.String(n),
					})
					require.NoError(t, err)
				}
			},
			fetch: func(t *testing.T, c *athenasdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListNotebookMetadata(t.Context(), &athenasdk.ListNotebookMetadataInput{
					WorkGroup: aws.String("primary"), MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.NotebookMetadataList), out.NextToken
			},
		},
		{
			name: "engine_versions", total: 3,
			seed: func(*testing.T, *athenasdk.Client) {},
			fetch: func(t *testing.T, c *athenasdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListEngineVersions(t.Context(), &athenasdk.ListEngineVersionsInput{
					MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.EngineVersions), out.NextToken
			},
		},
		{
			name: "application_dpu_sizes", total: 2,
			seed: func(*testing.T, *athenasdk.Client) {},
			fetch: func(t *testing.T, c *athenasdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListApplicationDPUSizes(t.Context(), &athenasdk.ListApplicationDPUSizesInput{
					MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.ApplicationDPUSizes), out.NextToken
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			tt.seed(t, client)

			var token *string

			seen := 0

			for pages := 0; ; pages++ {
				require.Less(t, pages, 10)

				n, next := tt.fetch(t, client, token)
				require.LessOrEqual(t, n, 1)

				seen += n

				if next == nil {
					break
				}

				token = next
			}

			require.Equal(t, tt.total, seen)
		})
	}
}

func TestRealClient_ListOpsRejectForgedToken(t *testing.T) {
	t.Parallel()

	_, err := newRealClient(t).ListEngineVersions(t.Context(), &athenasdk.ListEngineVersionsInput{
		NextToken: aws.String("forged"),
	})
	require.Error(t, err)
}
