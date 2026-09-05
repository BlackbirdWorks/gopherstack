package route53_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// TestListHostedZonesByVPC_Pagination is a regression test for gopherstack:
// ListHostedZonesByVPC truncated to MaxItems but the response carried neither
// IsTruncated nor a cursor field, so everything past the first page was
// unreachable and a client had no way to detect truncation at all -- the same
// severity band as an unpopulated cursor. api_op_ListHostedZonesByVPC.go
// confirms the real continuation field is NextToken on both the input and
// output (unlike sibling ListHostedZones*, which use NextMarker), and the
// wire element is also "NextToken" (deserializers.go, awsRestxml_deserialize
// OpDocumentListHostedZonesByVPCOutput's NextToken case).
func TestListHostedZonesByVPC_Pagination(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	const vpcID = "vpc-pagination-1"
	const total = 5
	const pageSize = 2

	wantNames := make(map[string]bool, total)
	for i := range total {
		name := fmt.Sprintf("pg-vpc-%d.example.com.", i)
		_, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
			Name:            aws.String(name),
			CallerReference: aws.String(fmt.Sprintf("pg-vpc-ref-%d", i)),
			HostedZoneConfig: &types.HostedZoneConfig{
				PrivateZone: true,
			},
			VPC: &types.VPC{
				VPCId:     aws.String(vpcID),
				VPCRegion: types.VPCRegionUsEast1,
			},
		})
		require.NoError(t, err)
		wantNames[name] = true
	}

	seen := make(map[string]bool, total)

	page1, err := client.ListHostedZonesByVPC(t.Context(), &route53sdk.ListHostedZonesByVPCInput{
		VPCId:     aws.String(vpcID),
		VPCRegion: types.VPCRegionUsEast1,
		MaxItems:  aws.Int32(pageSize),
	})
	require.NoError(t, err)
	require.Len(t, page1.HostedZoneSummaries, pageSize, "first page must be full")
	require.NotNil(t, page1.NextToken, "truncated response must carry a NextToken cursor")
	assert.NotEmpty(t, aws.ToString(page1.NextToken))

	for _, z := range page1.HostedZoneSummaries {
		seen[aws.ToString(z.Name)] = true
	}

	token := page1.NextToken
	for token != nil && aws.ToString(token) != "" {
		next, nextErr := client.ListHostedZonesByVPC(t.Context(), &route53sdk.ListHostedZonesByVPCInput{
			VPCId:     aws.String(vpcID),
			VPCRegion: types.VPCRegionUsEast1,
			MaxItems:  aws.Int32(pageSize),
			NextToken: token,
		})
		require.NoError(t, nextErr)

		for _, z := range next.HostedZoneSummaries {
			name := aws.ToString(z.Name)
			require.False(t, seen[name], "zone %q must not be returned twice across pages", name)
			seen[name] = true
		}

		token = next.NextToken
	}

	assert.Len(t, seen, total, "every zone must be reachable exactly once across all pages")
	for name := range wantNames {
		assert.True(t, seen[name], "zone %q must appear in some page", name)
	}
}
