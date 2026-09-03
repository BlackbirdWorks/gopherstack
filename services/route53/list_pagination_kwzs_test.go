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

// This file is a regression test suite for gopherstack-kwzs: six route53
// list operations never truncated or applied their marker at all --
// ListReusableDelegationSets, ListGeoLocations, ListCidrCollections,
// ListCidrBlocks, ListCidrLocations, and the ListTrafficPolic{y,yInstance}*
// family (ListTrafficPolicies, ListTrafficPolicyVersions,
// ListTrafficPolicyInstances(ByHostedZone|ByPolicy)) -- plus
// ListVPCAssociationAuthorizations, which ignored its marker (lower impact,
// AWS bounds it by quota, but the same shape). Each test seeds more records
// than one page, walks every page through the real aws-sdk-go-v2 client,
// and asserts the union across pages equals the seed set with nothing
// dropped or repeated.

func TestListReusableDelegationSets_Pagination(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)
	ctx := t.Context()

	const total = 5
	const pageSize = 2

	wantIDs := make(map[string]bool, total)
	for i := range total {
		out, err := client.CreateReusableDelegationSet(ctx, &route53sdk.CreateReusableDelegationSetInput{
			CallerReference: aws.String(fmt.Sprintf("rds-ref-%d", i)),
		})
		require.NoError(t, err)
		wantIDs[aws.ToString(out.DelegationSet.Id)] = true
	}

	seen := make(map[string]bool, total)
	marker := ""
	pageNum := 0
	for {
		out, err := client.ListReusableDelegationSets(ctx, &route53sdk.ListReusableDelegationSetsInput{
			Marker:   aws.String(marker),
			MaxItems: aws.Int32(pageSize),
		})
		require.NoError(t, err)

		if pageNum == 0 {
			require.Len(t, out.DelegationSets, pageSize, "first page must be truncated to MaxItems")
			require.True(t, out.IsTruncated, "first page must be marked truncated")
		}
		pageNum++

		for _, ds := range out.DelegationSets {
			id := aws.ToString(ds.Id)
			require.False(t, seen[id], "delegation set %q must not be returned twice across pages", id)
			seen[id] = true
		}

		if !out.IsTruncated {
			assert.Empty(t, aws.ToString(out.NextMarker))

			break
		}

		require.NotEmpty(t, aws.ToString(out.NextMarker), "truncated response must carry NextMarker")
		marker = aws.ToString(out.NextMarker)
	}

	assert.Len(t, seen, total)
	for id := range wantIDs {
		assert.True(t, seen[id], "delegation set %q must appear in some page", id)
	}
}

func TestListCidrCollections_Pagination(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)
	ctx := t.Context()

	const total = 5
	const pageSize = 2

	wantIDs := make(map[string]bool, total)
	for i := range total {
		out, err := client.CreateCidrCollection(ctx, &route53sdk.CreateCidrCollectionInput{
			Name:            aws.String(fmt.Sprintf("cidr-col-%d", i)),
			CallerReference: aws.String(fmt.Sprintf("cidr-col-ref-%d", i)),
		})
		require.NoError(t, err)
		wantIDs[aws.ToString(out.Collection.Id)] = true
	}

	seen := make(map[string]bool, total)
	nextToken := (*string)(nil)
	pageNum := 0
	for {
		out, err := client.ListCidrCollections(ctx, &route53sdk.ListCidrCollectionsInput{
			MaxResults: aws.Int32(pageSize),
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		if pageNum == 0 {
			require.Len(t, out.CidrCollections, pageSize, "first page must be truncated to MaxResults")
			require.NotNil(t, out.NextToken, "first page must carry a NextToken cursor")
		}
		pageNum++

		for _, col := range out.CidrCollections {
			id := aws.ToString(col.Id)
			require.False(t, seen[id], "CIDR collection %q must not be returned twice across pages", id)
			seen[id] = true
		}

		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}

		nextToken = out.NextToken
	}

	assert.Len(t, seen, total)
	for id := range wantIDs {
		assert.True(t, seen[id], "CIDR collection %q must appear in some page", id)
	}
}

func TestListCidrLocations_Pagination(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)
	ctx := t.Context()

	colOut, colErr := client.CreateCidrCollection(ctx, &route53sdk.CreateCidrCollectionInput{
		Name:            aws.String("cidr-loc-col"),
		CallerReference: aws.String("cidr-loc-col-ref"),
	})
	require.NoError(t, colErr)

	const total = 5
	const pageSize = 2

	wantLocations := make(map[string]bool, total)
	for i := range total {
		loc := fmt.Sprintf("loc-%d", i)
		_, changeErr := client.ChangeCidrCollection(ctx, &route53sdk.ChangeCidrCollectionInput{
			Id: colOut.Collection.Id,
			Changes: []types.CidrCollectionChange{
				{
					Action:       types.CidrCollectionChangeActionPut,
					LocationName: aws.String(loc),
					CidrList:     []string{"192.0.2.0/24"},
				},
			},
		})
		require.NoError(t, changeErr)
		wantLocations[loc] = true
	}

	seen := make(map[string]bool, total)
	nextToken := (*string)(nil)
	pageNum := 0
	for {
		out, err := client.ListCidrLocations(ctx, &route53sdk.ListCidrLocationsInput{
			CollectionId: colOut.Collection.Id,
			MaxResults:   aws.Int32(pageSize),
			NextToken:    nextToken,
		})
		require.NoError(t, err)

		if pageNum == 0 {
			require.Len(t, out.CidrLocations, pageSize, "first page must be truncated to MaxResults")
			require.NotNil(t, out.NextToken, "first page must carry a NextToken cursor")
		}
		pageNum++

		for _, l := range out.CidrLocations {
			name := aws.ToString(l.LocationName)
			require.False(t, seen[name], "location %q must not be returned twice across pages", name)
			seen[name] = true
		}

		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}

		nextToken = out.NextToken
	}

	assert.Len(t, seen, total)
	for name := range wantLocations {
		assert.True(t, seen[name], "location %q must appear in some page", name)
	}
}

func TestListCidrBlocks_Pagination(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)
	ctx := t.Context()

	colOut, colErr := client.CreateCidrCollection(ctx, &route53sdk.CreateCidrCollectionInput{
		Name:            aws.String("cidr-block-col"),
		CallerReference: aws.String("cidr-block-col-ref"),
	})
	require.NoError(t, colErr)

	const total = 6
	const pageSize = 2

	wantBlocks := make(map[string]bool, total)
	cidrs := make([]string, 0, total)
	for i := range total {
		cidr := fmt.Sprintf("192.0.%d.0/24", i)
		cidrs = append(cidrs, cidr)
		wantBlocks[cidr] = true
	}

	_, changeErr := client.ChangeCidrCollection(ctx, &route53sdk.ChangeCidrCollectionInput{
		Id: colOut.Collection.Id,
		Changes: []types.CidrCollectionChange{
			{
				Action:       types.CidrCollectionChangeActionPut,
				LocationName: aws.String("blocks-loc"),
				CidrList:     cidrs,
			},
		},
	})
	require.NoError(t, changeErr)

	seen := make(map[string]bool, total)
	nextToken := (*string)(nil)
	pageNum := 0
	for {
		out, err := client.ListCidrBlocks(ctx, &route53sdk.ListCidrBlocksInput{
			CollectionId: colOut.Collection.Id,
			LocationName: aws.String("blocks-loc"),
			MaxResults:   aws.Int32(pageSize),
			NextToken:    nextToken,
		})
		require.NoError(t, err)

		if pageNum == 0 {
			require.Len(t, out.CidrBlocks, pageSize, "first page must be truncated to MaxResults")
			require.NotNil(t, out.NextToken, "first page must carry a NextToken cursor")
		}
		pageNum++

		for _, b := range out.CidrBlocks {
			block := aws.ToString(b.CidrBlock)
			require.False(t, seen[block], "CIDR block %q must not be returned twice across pages", block)
			seen[block] = true
		}

		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}

		nextToken = out.NextToken
	}

	assert.Len(t, seen, total)
	for cidr := range wantBlocks {
		assert.True(t, seen[cidr], "CIDR block %q must appear in some page", cidr)
	}
}

func TestListVPCAssociationAuthorizations_Pagination(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)
	ctx := t.Context()

	zoneOut, zoneErr := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("vpc-auth-pagination.example.com."),
		CallerReference: aws.String("vpc-auth-pagination-ref"),
		HostedZoneConfig: &types.HostedZoneConfig{
			PrivateZone: true,
		},
		VPC: &types.VPC{
			VPCId:     aws.String("vpc-owner"),
			VPCRegion: types.VPCRegionUsEast1,
		},
	})
	require.NoError(t, zoneErr)
	zoneID := aws.ToString(zoneOut.HostedZone.Id)

	const total = 5
	const pageSize = 2

	wantVPCIDs := make(map[string]bool, total)
	for i := range total {
		vpcID := fmt.Sprintf("vpc-auth-%d", i)
		_, authErr := client.CreateVPCAssociationAuthorization(ctx, &route53sdk.CreateVPCAssociationAuthorizationInput{
			HostedZoneId: aws.String(zoneID),
			VPC: &types.VPC{
				VPCId:     aws.String(vpcID),
				VPCRegion: types.VPCRegionUsWest2,
			},
		})
		require.NoError(t, authErr)
		wantVPCIDs[vpcID] = true
	}

	seen := make(map[string]bool, total)
	nextToken := (*string)(nil)
	pageNum := 0
	for {
		out, err := client.ListVPCAssociationAuthorizations(ctx, &route53sdk.ListVPCAssociationAuthorizationsInput{
			HostedZoneId: aws.String(zoneID),
			MaxResults:   aws.Int32(pageSize),
			NextToken:    nextToken,
		})
		require.NoError(t, err)

		if pageNum == 0 {
			require.Len(t, out.VPCs, pageSize, "first page must be truncated to MaxResults")
			require.NotNil(t, out.NextToken, "first page must carry a NextToken cursor")
		}
		pageNum++

		for _, v := range out.VPCs {
			id := aws.ToString(v.VPCId)
			require.False(t, seen[id], "VPC %q must not be returned twice across pages", id)
			seen[id] = true
		}

		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}

		nextToken = out.NextToken
	}

	assert.Len(t, seen, total)
	for id := range wantVPCIDs {
		assert.True(t, seen[id], "VPC %q must appear in some page", id)
	}
}

func TestListTrafficPolicies_Pagination(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)
	ctx := t.Context()

	const total = 5
	const pageSize = 2
	const doc = `{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A",` +
		`"Endpoints":{"e1":{"Type":"value","Value":"1.2.3.4"}},"StartEndpoint":"e1"}`

	wantIDs := make(map[string]bool, total)
	for i := range total {
		out, err := client.CreateTrafficPolicy(ctx, &route53sdk.CreateTrafficPolicyInput{
			Name:     aws.String(fmt.Sprintf("tp-pagination-%d", i)),
			Document: aws.String(doc),
		})
		require.NoError(t, err)
		wantIDs[aws.ToString(out.TrafficPolicy.Id)] = true
	}

	seen := make(map[string]bool, total)
	marker := ""
	pageNum := 0
	for {
		out, err := client.ListTrafficPolicies(ctx, &route53sdk.ListTrafficPoliciesInput{
			TrafficPolicyIdMarker: aws.String(marker),
			MaxItems:              aws.Int32(pageSize),
		})
		require.NoError(t, err)

		if pageNum == 0 {
			require.Len(t, out.TrafficPolicySummaries, pageSize, "first page must be truncated to MaxItems")
			require.True(t, out.IsTruncated, "first page must be marked truncated")
		}
		pageNum++

		for _, tp := range out.TrafficPolicySummaries {
			id := aws.ToString(tp.Id)
			require.False(t, seen[id], "traffic policy %q must not be returned twice across pages", id)
			seen[id] = true
		}

		if !out.IsTruncated {
			break
		}

		require.NotEmpty(
			t,
			aws.ToString(out.TrafficPolicyIdMarker),
			"truncated response must carry TrafficPolicyIdMarker",
		)
		marker = aws.ToString(out.TrafficPolicyIdMarker)
	}

	assert.Len(t, seen, total)
	for id := range wantIDs {
		assert.True(t, seen[id], "traffic policy %q must appear in some page", id)
	}
}

func TestListTrafficPolicyVersions_Pagination(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)
	ctx := t.Context()

	const total = 5
	const pageSize = 2
	const doc = `{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A",` +
		`"Endpoints":{"e1":{"Type":"value","Value":"1.2.3.4"}},"StartEndpoint":"e1"}`

	createOut, createErr := client.CreateTrafficPolicy(ctx, &route53sdk.CreateTrafficPolicyInput{
		Name:     aws.String("tp-versions-pagination"),
		Document: aws.String(doc),
	})
	require.NoError(t, createErr)
	tpID := createOut.TrafficPolicy.Id

	wantVersions := map[int32]bool{1: true}
	for range total - 1 {
		out, err := client.CreateTrafficPolicyVersion(ctx, &route53sdk.CreateTrafficPolicyVersionInput{
			Id:       tpID,
			Document: aws.String(doc),
		})
		require.NoError(t, err)
		wantVersions[aws.ToInt32(out.TrafficPolicy.Version)] = true
	}
	require.Len(t, wantVersions, total)

	seen := make(map[int32]bool, total)
	marker := ""
	pageNum := 0
	for {
		out, err := client.ListTrafficPolicyVersions(ctx, &route53sdk.ListTrafficPolicyVersionsInput{
			Id:                         tpID,
			TrafficPolicyVersionMarker: aws.String(marker),
			MaxItems:                   aws.Int32(pageSize),
		})
		require.NoError(t, err)

		if pageNum == 0 {
			require.Len(t, out.TrafficPolicies, pageSize, "first page must be truncated to MaxItems")
			require.True(t, out.IsTruncated, "first page must be marked truncated")
		}
		pageNum++

		for _, tp := range out.TrafficPolicies {
			v := aws.ToInt32(tp.Version)
			require.False(t, seen[v], "version %d must not be returned twice across pages", v)
			seen[v] = true
		}

		if !out.IsTruncated {
			break
		}

		require.NotEmpty(
			t,
			aws.ToString(out.TrafficPolicyVersionMarker),
			"truncated response must carry TrafficPolicyVersionMarker",
		)
		marker = aws.ToString(out.TrafficPolicyVersionMarker)
	}

	assert.Len(t, seen, total)
	for v := range wantVersions {
		assert.True(t, seen[v], "version %d must appear in some page", v)
	}
}

// createTPIForPaginationTest creates a hosted zone, a traffic policy, and a
// traffic policy instance in that zone, returning the instance's ID (the
// only return value any caller needs -- it's used solely to seed a decoy
// instance that pagination filters must exclude).
func createTPIForPaginationTest(
	t *testing.T,
	client *route53sdk.Client,
	instanceName string,
) string {
	t.Helper()

	ctx := t.Context()
	const doc = `{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A",` +
		`"Endpoints":{"e1":{"Type":"value","Value":"1.2.3.4"}},"StartEndpoint":"e1"}`

	zoneOut, zoneErr := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String(instanceName + "-zone.example.com."),
		CallerReference: aws.String(instanceName + "-zone-ref"),
	})
	require.NoError(t, zoneErr)
	zoneID := aws.ToString(zoneOut.HostedZone.Id)

	tpOut, tpErr := client.CreateTrafficPolicy(ctx, &route53sdk.CreateTrafficPolicyInput{
		Name:     aws.String(instanceName + "-tp"),
		Document: aws.String(doc),
	})
	require.NoError(t, tpErr)
	tpID := aws.ToString(tpOut.TrafficPolicy.Id)
	tpVersion := aws.ToInt32(tpOut.TrafficPolicy.Version)

	instOut, instErr := client.CreateTrafficPolicyInstance(ctx, &route53sdk.CreateTrafficPolicyInstanceInput{
		HostedZoneId:         aws.String(zoneID),
		Name:                 aws.String(instanceName + ".example.com."),
		TrafficPolicyId:      aws.String(tpID),
		TrafficPolicyVersion: aws.Int32(tpVersion),
		TTL:                  aws.Int64(60),
	})
	require.NoError(t, instErr)

	return aws.ToString(instOut.TrafficPolicyInstance.Id)
}

func TestListTrafficPolicyInstances_Pagination(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	const total = 5
	const pageSize = 2

	wantIDs := make(map[string]bool, total)
	for i := range total {
		id := createTPIForPaginationTest(t, client, fmt.Sprintf("tpi-all-%d", i))
		wantIDs[id] = true
	}

	seen := make(map[string]bool, total)
	marker := ""
	ctx := t.Context()
	pageNum := 0
	for {
		out, err := client.ListTrafficPolicyInstances(ctx, &route53sdk.ListTrafficPolicyInstancesInput{
			HostedZoneIdMarker: aws.String(marker),
			MaxItems:           aws.Int32(pageSize),
		})
		require.NoError(t, err)

		if pageNum == 0 {
			require.Len(t, out.TrafficPolicyInstances, pageSize, "first page must be truncated to MaxItems")
			require.True(t, out.IsTruncated, "first page must be marked truncated")
		}
		pageNum++

		for _, inst := range out.TrafficPolicyInstances {
			id := aws.ToString(inst.Id)
			require.False(t, seen[id], "instance %q must not be returned twice across pages", id)
			seen[id] = true
		}

		if !out.IsTruncated {
			break
		}

		require.NotEmpty(t, aws.ToString(out.HostedZoneIdMarker), "truncated response must carry HostedZoneIdMarker")
		marker = aws.ToString(out.HostedZoneIdMarker)
	}

	assert.Len(t, seen, total)
	for id := range wantIDs {
		assert.True(t, seen[id], "instance %q must appear in some page", id)
	}
}

func TestListTrafficPolicyInstancesByHostedZone_Pagination(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)
	ctx := t.Context()

	// One shared zone and traffic policy; several instances within it.
	const doc = `{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A",` +
		`"Endpoints":{"e1":{"Type":"value","Value":"1.2.3.4"}},"StartEndpoint":"e1"}`

	zoneOut, setupErr := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("tpi-byzone-pagination.example.com."),
		CallerReference: aws.String("tpi-byzone-pagination-ref"),
	})
	require.NoError(t, setupErr)
	zoneID := aws.ToString(zoneOut.HostedZone.Id)

	tpOut, setupErr := client.CreateTrafficPolicy(ctx, &route53sdk.CreateTrafficPolicyInput{
		Name:     aws.String("tpi-byzone-tp"),
		Document: aws.String(doc),
	})
	require.NoError(t, setupErr)
	tpID := aws.ToString(tpOut.TrafficPolicy.Id)
	tpVersion := aws.ToInt32(tpOut.TrafficPolicy.Version)

	const total = 5
	const pageSize = 2

	wantIDs := make(map[string]bool, total)
	for i := range total {
		instOut, instErr := client.CreateTrafficPolicyInstance(ctx, &route53sdk.CreateTrafficPolicyInstanceInput{
			HostedZoneId:         aws.String(zoneID),
			Name:                 aws.String(fmt.Sprintf("tpi-byzone-%d.example.com.", i)),
			TrafficPolicyId:      aws.String(tpID),
			TrafficPolicyVersion: aws.Int32(tpVersion),
			TTL:                  aws.Int64(60),
		})
		require.NoError(t, instErr)
		wantIDs[aws.ToString(instOut.TrafficPolicyInstance.Id)] = true
	}

	// A decoy instance in a different zone must never show up.
	decoyID := createTPIForPaginationTest(t, client, "tpi-byzone-decoy")

	seen := make(map[string]bool, total)
	marker := ""
	pageNum := 0
	for {
		out, err := client.ListTrafficPolicyInstancesByHostedZone(
			ctx,
			&route53sdk.ListTrafficPolicyInstancesByHostedZoneInput{
				HostedZoneId:                    aws.String(zoneID),
				TrafficPolicyInstanceNameMarker: aws.String(marker),
				MaxItems:                        aws.Int32(pageSize),
			},
		)
		require.NoError(t, err)

		if pageNum == 0 {
			require.Len(t, out.TrafficPolicyInstances, pageSize, "first page must be truncated to MaxItems")
			require.True(t, out.IsTruncated, "first page must be marked truncated")
		}
		pageNum++

		for _, inst := range out.TrafficPolicyInstances {
			id := aws.ToString(inst.Id)
			require.False(t, seen[id], "instance %q must not be returned twice across pages", id)
			assert.NotEqual(t, decoyID, id, "instance from a different zone must not appear")
			seen[id] = true
		}

		if !out.IsTruncated {
			break
		}

		require.NotEmpty(
			t,
			aws.ToString(out.TrafficPolicyInstanceNameMarker),
			"truncated response must carry TrafficPolicyInstanceNameMarker",
		)
		marker = aws.ToString(out.TrafficPolicyInstanceNameMarker)
	}

	assert.Len(t, seen, total)
	for id := range wantIDs {
		assert.True(t, seen[id], "instance %q must appear in some page", id)
	}
}

func TestListTrafficPolicyInstancesByPolicy_Pagination(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)
	ctx := t.Context()

	const doc = `{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A",` +
		`"Endpoints":{"e1":{"Type":"value","Value":"1.2.3.4"}},"StartEndpoint":"e1"}`

	tpOut, setupErr := client.CreateTrafficPolicy(ctx, &route53sdk.CreateTrafficPolicyInput{
		Name:     aws.String("tpi-bypolicy-tp"),
		Document: aws.String(doc),
	})
	require.NoError(t, setupErr)
	tpID := aws.ToString(tpOut.TrafficPolicy.Id)
	tpVersion := aws.ToInt32(tpOut.TrafficPolicy.Version)

	const total = 5
	const pageSize = 2

	wantIDs := make(map[string]bool, total)
	for i := range total {
		zoneOut, zoneErr := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
			Name:            aws.String(fmt.Sprintf("tpi-bypolicy-%d.example.com.", i)),
			CallerReference: aws.String(fmt.Sprintf("tpi-bypolicy-ref-%d", i)),
		})
		require.NoError(t, zoneErr)

		instOut, instErr := client.CreateTrafficPolicyInstance(ctx, &route53sdk.CreateTrafficPolicyInstanceInput{
			HostedZoneId:         zoneOut.HostedZone.Id,
			Name:                 aws.String(fmt.Sprintf("tpi-bypolicy-%d.example.com.", i)),
			TrafficPolicyId:      aws.String(tpID),
			TrafficPolicyVersion: aws.Int32(tpVersion),
			TTL:                  aws.Int64(60),
		})
		require.NoError(t, instErr)
		wantIDs[aws.ToString(instOut.TrafficPolicyInstance.Id)] = true
	}

	// A decoy instance tied to a different traffic policy must never show up.
	decoyID := createTPIForPaginationTest(t, client, "tpi-bypolicy-decoy")

	seen := make(map[string]bool, total)
	marker := ""
	pageNum := 0
	for {
		out, err := client.ListTrafficPolicyInstancesByPolicy(
			ctx,
			&route53sdk.ListTrafficPolicyInstancesByPolicyInput{
				TrafficPolicyId:      aws.String(tpID),
				TrafficPolicyVersion: aws.Int32(tpVersion),
				HostedZoneIdMarker:   aws.String(marker),
				MaxItems:             aws.Int32(pageSize),
			},
		)
		require.NoError(t, err)

		if pageNum == 0 {
			require.Len(t, out.TrafficPolicyInstances, pageSize, "first page must be truncated to MaxItems")
			require.True(t, out.IsTruncated, "first page must be marked truncated")
		}
		pageNum++

		for _, inst := range out.TrafficPolicyInstances {
			id := aws.ToString(inst.Id)
			require.False(t, seen[id], "instance %q must not be returned twice across pages", id)
			assert.NotEqual(t, decoyID, id, "instance from a different policy must not appear")
			seen[id] = true
		}

		if !out.IsTruncated {
			break
		}

		require.NotEmpty(t, aws.ToString(out.HostedZoneIdMarker), "truncated response must carry HostedZoneIdMarker")
		marker = aws.ToString(out.HostedZoneIdMarker)
	}

	assert.Len(t, seen, total)
	for id := range wantIDs {
		assert.True(t, seen[id], "instance %q must appear in some page", id)
	}
}

func TestListGeoLocations_Pagination(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)
	ctx := t.Context()

	const pageSize = 5

	// geoLocationTable is a fixed compile-time table; get its true size and
	// membership from an unpaginated first call.
	full, fullErr := client.ListGeoLocations(ctx, &route53sdk.ListGeoLocationsInput{})
	require.NoError(t, fullErr)
	total := len(full.GeoLocationDetailsList)
	require.Greater(t, total, pageSize, "table must span multiple pages at this page size")

	type key struct{ continent, country, subdivision string }

	want := make(map[key]bool, total)
	for _, loc := range full.GeoLocationDetailsList {
		want[key{aws.ToString(loc.ContinentCode), aws.ToString(loc.CountryCode), aws.ToString(loc.SubdivisionCode)}] = true
	}

	seen := make(map[key]bool, total)
	var startContinent, startCountry, startSubdivision *string
	pageNum := 0
	for {
		out, err := client.ListGeoLocations(ctx, &route53sdk.ListGeoLocationsInput{
			MaxItems:             aws.Int32(pageSize),
			StartContinentCode:   startContinent,
			StartCountryCode:     startCountry,
			StartSubdivisionCode: startSubdivision,
		})
		require.NoError(t, err)

		if pageNum == 0 {
			require.Len(t, out.GeoLocationDetailsList, pageSize, "first page must be truncated to MaxItems")
			require.True(t, out.IsTruncated, "first page must be marked truncated")
		}
		pageNum++

		for _, loc := range out.GeoLocationDetailsList {
			k := key{aws.ToString(loc.ContinentCode), aws.ToString(loc.CountryCode), aws.ToString(loc.SubdivisionCode)}
			require.False(t, seen[k], "geolocation %+v must not be returned twice across pages", k)
			seen[k] = true
		}

		if !out.IsTruncated {
			break
		}

		startContinent = out.NextContinentCode
		startCountry = out.NextCountryCode
		startSubdivision = out.NextSubdivisionCode
	}

	assert.Len(t, seen, total)
	for k := range want {
		assert.True(t, seen[k], "geolocation %+v must appear in some page", k)
	}
}
