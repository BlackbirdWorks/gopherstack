package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/outposts"
)

type pageEnv struct {
	client    *outpostssdk.Client
	outpostID *string
}

func newPageEnv(t *testing.T) (pageEnv, *outposts.Handler, string) {
	t.Helper()

	h, client := newTestHandlerAndClient(t)
	arn, _ := setupOutpostWithCapacity(t, client, "m5.xlarge", 5)
	out, err := client.ListOutposts(t.Context(), &outpostssdk.ListOutpostsInput{})
	require.NoError(t, err)
	require.Len(t, out.Outposts, 1)

	return pageEnv{client: client, outpostID: out.Outposts[0].OutpostId}, h, arn
}

// TestRealClient_ListOpsHonourMaxResults pages ops whose MaxResults/NextToken
// query members were ignored (serializers.go:1361, outposts@v1.66.1).
func TestRealClient_ListOpsHonourMaxResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fetch func(t *testing.T, e pageEnv, token *string) (int, *string)
		name  string
		total int
	}{
		{
			name: "supported_instance_types", total: 3,
			fetch: func(t *testing.T, e pageEnv, token *string) (int, *string) {
				t.Helper()

				out, err := e.client.GetOutpostSupportedInstanceTypes(t.Context(),
					&outpostssdk.GetOutpostSupportedInstanceTypesInput{
						OutpostIdentifier: e.outpostID, MaxResults: aws.Int32(1), NextToken: token,
					})
				require.NoError(t, err)

				return len(out.InstanceTypes), out.NextToken
			},
		},
		{
			name: "asset_instances", total: 3,
			fetch: func(t *testing.T, e pageEnv, token *string) (int, *string) {
				t.Helper()

				out, err := e.client.ListAssetInstances(t.Context(), &outpostssdk.ListAssetInstancesInput{
					OutpostIdentifier: e.outpostID, MaxResults: aws.Int32(2), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.AssetInstances), out.NextToken
			},
		},
		{
			name: "assets_single_page", total: 1,
			fetch: func(t *testing.T, e pageEnv, token *string) (int, *string) {
				t.Helper()

				out, err := e.client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{
					OutpostIdentifier: e.outpostID, MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.Assets), out.NextToken
			},
		},
		{
			name: "outpost_instance_types", total: 1,
			fetch: func(t *testing.T, e pageEnv, token *string) (int, *string) {
				t.Helper()

				out, err := e.client.GetOutpostInstanceTypes(t.Context(), &outpostssdk.GetOutpostInstanceTypesInput{
					OutpostId: e.outpostID, MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.InstanceTypes), out.NextToken
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, h, arn := newPageEnv(t)
			require.NoError(
				t,
				h.Backend.ConsumeCapacity(arn, "m5.xlarge", rtTestAccountID, []string{"i-a", "i-b", "i-c"}),
			)

			var token *string

			got, pages := 0, 0

			for {
				n, next := tt.fetch(t, e, token)
				got += n
				pages++

				if next == nil {
					break
				}

				token = next

				require.LessOrEqual(t, pages, tt.total+1)
			}

			require.Equal(t, tt.total, got)
		})
	}
}

func requireValidationCode(t *testing.T, err error) {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ValidationException", apiErr.ErrorCode())
}

// TestRealClient_ListOpsRejectBadPaging covers the documented MaxResults range
// (1-1000) and a malformed NextToken on paged ops.
func TestRealClient_ListOpsRejectBadPaging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, e pageEnv, m *int32, token *string) error
		name string
	}{
		{name: "list_assets", call: func(t *testing.T, e pageEnv, m *int32, tok *string) error {
			t.Helper()

			_, err := e.client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{
				OutpostIdentifier: e.outpostID, MaxResults: m, NextToken: tok,
			})

			return err
		}},
		{name: "billing", call: func(t *testing.T, e pageEnv, m *int32, tok *string) error {
			t.Helper()

			_, err := e.client.GetOutpostBillingInformation(t.Context(), &outpostssdk.GetOutpostBillingInformationInput{
				OutpostIdentifier: e.outpostID, MaxResults: m, NextToken: tok,
			})

			return err
		}},
		{name: "list_sites"},
		{name: "blocking_instances", call: func(t *testing.T, e pageEnv, m *int32, tok *string) error {
			t.Helper()

			_, err := e.client.ListBlockingInstancesForCapacityTask(t.Context(),
				&outpostssdk.ListBlockingInstancesForCapacityTaskInput{
					OutpostIdentifier: e.outpostID, CapacityTaskId: aws.String("x"), MaxResults: m, NextToken: tok,
				})

			return err
		}},
	}

	for _, tt := range tests {
		for _, bad := range []struct {
			max   *int32
			token *string
			name  string
		}{
			{name: "zero", max: aws.Int32(0)},
			{name: "too_big", max: aws.Int32(1001)},
			{name: "bad_token", token: aws.String("%%%")},
		} {
			t.Run(tt.name+"/"+bad.name, func(t *testing.T) {
				t.Parallel()

				e, _, _ := newPageEnv(t)
				if tt.name == "list_sites" {
					_, err := e.client.ListSites(t.Context(), &outpostssdk.ListSitesInput{
						MaxResults: bad.max, NextToken: bad.token,
					})
					requireValidationCode(t, err)

					return
				}

				requireValidationCode(t, tt.call(t, e, bad.max, bad.token))
			})
		}
	}
}
