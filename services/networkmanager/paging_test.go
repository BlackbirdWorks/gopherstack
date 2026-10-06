package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/stretchr/testify/require"
)

// pageFn fetches one page and returns its item count and NextToken.
type pageFn func(token *string) (int, *string)

// TestRoundTrip_GetOpsHonourMaxResults pages every Get op whose request
// binds maxResults/nextToken as query members (serializers.go).
func TestRoundTrip_GetOpsHonourMaxResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build func(t *testing.T, client *networkmanagersdk.Client) pageFn
		name  string
		total int
	}{
		{
			name: "change_set", total: 2,
			build: func(t *testing.T, client *networkmanagersdk.Client) pageFn {
				t.Helper()

				cn, version := policyWithSegments(t, client)

				return func(token *string) (int, *string) {
					out, err := client.GetCoreNetworkChangeSet(
						t.Context(),
						&networkmanagersdk.GetCoreNetworkChangeSetInput{
							CoreNetworkId: cn, PolicyVersionId: version, MaxResults: aws.Int32(1), NextToken: token,
						},
					)
					require.NoError(t, err)

					return len(out.CoreNetworkChanges), out.NextToken
				}
			},
		},
		{
			name: "change_events", total: 2,
			build: func(t *testing.T, client *networkmanagersdk.Client) pageFn {
				t.Helper()

				cn, version := policyWithSegments(t, client)

				return func(token *string) (int, *string) {
					out, err := client.GetCoreNetworkChangeEvents(
						t.Context(),
						&networkmanagersdk.GetCoreNetworkChangeEventsInput{
							CoreNetworkId: cn, PolicyVersionId: version, MaxResults: aws.Int32(1), NextToken: token,
						},
					)
					require.NoError(t, err)

					return len(out.CoreNetworkChangeEvents), out.NextToken
				}
			},
		},
		{
			name: "resource_counts", total: 2,
			build: func(t *testing.T, client *networkmanagersdk.Client) pageFn {
				t.Helper()

				gn, err := client.CreateGlobalNetwork(t.Context(), &networkmanagersdk.CreateGlobalNetworkInput{})
				require.NoError(t, err)

				id := gn.GlobalNetwork.GlobalNetworkId
				site, err := client.CreateSite(t.Context(), &networkmanagersdk.CreateSiteInput{GlobalNetworkId: id})
				require.NoError(t, err)

				_, err = client.CreateDevice(t.Context(), &networkmanagersdk.CreateDeviceInput{
					GlobalNetworkId: id, SiteId: site.Site.SiteId,
				})
				require.NoError(t, err)

				return func(token *string) (int, *string) {
					out, cErr := client.GetNetworkResourceCounts(
						t.Context(),
						&networkmanagersdk.GetNetworkResourceCountsInput{
							GlobalNetworkId: id, MaxResults: aws.Int32(1), NextToken: token,
						},
					)
					require.NoError(t, cErr)

					return len(out.NetworkResourceCounts), out.NextToken
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			fetch := tt.build(t, client)

			var token *string

			seen, pages := 0, 0

			for {
				n, next := fetch(token)
				require.LessOrEqual(t, n, 1)

				seen += n
				pages++

				if next == nil {
					break
				}

				token = next

				require.Less(t, pages, 10)
			}

			require.Equal(t, tt.total, seen)
			require.Equal(t, tt.total, pages)
		})
	}
}

func policyWithSegments(t *testing.T, client *networkmanagersdk.Client) (*string, *int32) {
	t.Helper()

	cn := createTestCoreNetwork(t, client).CoreNetwork.CoreNetworkId
	doc := `{"version":"2021.12","segments":[{"name":"a"},{"name":"b"}]}`

	put, err := client.PutCoreNetworkPolicy(t.Context(), &networkmanagersdk.PutCoreNetworkPolicyInput{
		CoreNetworkId: cn, PolicyDocument: aws.String(doc),
	})
	require.NoError(t, err)

	return cn, put.CoreNetworkPolicy.PolicyVersionId
}
