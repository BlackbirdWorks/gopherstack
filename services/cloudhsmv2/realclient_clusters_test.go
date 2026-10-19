package cloudhsmv2_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/cloudhsmv2"
	"github.com/aws/aws-sdk-go-v2/service/cloudhsmv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *fixture) createCluster(t *testing.T) *types.Cluster {
	t.Helper()

	out, err := f.client.CreateCluster(t.Context(), &sdk.CreateClusterInput{
		HsmType:   aws.String("hsm1.medium"),
		SubnetIds: []string{"subnet-a", "subnet-b"},
	})
	require.NoError(t, err)

	return out.Cluster
}

// initializedCluster returns a cluster taken through UNINITIALIZED and InitializeCluster.
func (f *fixture) initializedCluster(t *testing.T) types.Cluster {
	t.Helper()

	c := f.createCluster(t)
	f.clock.advance(time.Second)

	got := f.describeOne(t, aws.ToString(c.ClusterId))
	signed, root := signCSR(t, aws.ToString(got.Certificates.ClusterCsr))

	_, err := f.client.InitializeCluster(t.Context(), &sdk.InitializeClusterInput{
		ClusterId: c.ClusterId, SignedCert: aws.String(signed), TrustAnchor: aws.String(root),
	})
	require.NoError(t, err)
	f.clock.advance(time.Second)

	return f.describeOne(t, aws.ToString(c.ClusterId))
}

func (f *fixture) describeOne(t *testing.T, id string) types.Cluster {
	t.Helper()

	out, err := f.client.DescribeClusters(t.Context(), &sdk.DescribeClustersInput{
		Filters: map[string][]string{"clusterIds": {id}},
	})
	require.NoError(t, err)
	require.Len(t, out.Clusters, 1)

	return out.Clusters[0]
}

func TestRealClient_Clusters(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T, f *fixture)
		name string
	}{
		{testClusterLifecycle, "lifecycle"},
		{testCreateClusterValidation, "validation"},
		{testInitializeCluster, "initialize"},
		{testInitializeClusterRejects, "initialize_rejects"},
		{testHsms, "hsms"},
		{testModifyCluster, "modify"},
		{testDescribeClustersFilters, "filters"},
		{testClusterLimit, "limit"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newFixture(t))
		})
	}
}

func testClusterLifecycle(t *testing.T, f *fixture) {
	t.Helper()

	out, err := f.client.CreateCluster(t.Context(), &sdk.CreateClusterInput{
		HsmType:   aws.String("hsm1.medium"),
		SubnetIds: []string{"subnet-a", "subnet-b"},
		TagList:   []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	c := out.Cluster
	assert.Regexp(t, `^cluster-[0-9a-f]{11}$`, aws.ToString(c.ClusterId))
	assert.Equal(t, types.ClusterStateCreateInProgress, c.State)
	assert.Equal(t, types.ClusterModeFips, c.Mode)
	assert.Equal(t, types.NetworkTypeIpv4, c.NetworkType)
	assert.Equal(t, "vpc-1", aws.ToString(c.VpcId))
	assert.Equal(t, map[string]string{"us-east-1a": "subnet-a", "us-east-1b": "subnet-b"}, c.SubnetMapping)
	assert.Regexp(t, `^sg-`, aws.ToString(c.SecurityGroup))
	assert.Equal(t, "90", aws.ToString(c.BackupRetentionPolicy.Value))
	assert.Equal(t, f.clock.now().Unix(), c.CreateTimestamp.Unix())
	assert.Nil(t, c.Certificates)
	require.Len(t, c.TagList, 1)

	f.clock.advance(time.Second)

	got := f.describeOne(t, aws.ToString(c.ClusterId))
	assert.Equal(t, types.ClusterStateUninitialized, got.State)
	require.NotNil(t, got.Certificates)

	blk, _ := pem.Decode([]byte(aws.ToString(got.Certificates.ClusterCsr)))
	require.NotNil(t, blk)

	csr, err := x509.ParseCertificateRequest(blk.Bytes)
	require.NoError(t, err)
	require.NoError(t, csr.CheckSignature())

	del, err := f.client.DeleteCluster(t.Context(), &sdk.DeleteClusterInput{ClusterId: c.ClusterId})
	require.NoError(t, err)
	assert.Equal(t, types.ClusterStateDeleteInProgress, del.Cluster.State)

	f.clock.advance(time.Second)

	list, err := f.client.DescribeClusters(t.Context(), &sdk.DescribeClustersInput{})
	require.NoError(t, err)
	assert.Empty(t, list.Clusters)

	_, err = f.client.DeleteCluster(t.Context(), &sdk.DeleteClusterInput{ClusterId: c.ClusterId})
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err))
	assert.Empty(t, f.net.groups)
}

func testCreateClusterValidation(t *testing.T, f *fixture) {
	t.Helper()

	cases := []struct {
		in   *sdk.CreateClusterInput
		name string
		code string
	}{
		{
			&sdk.CreateClusterInput{HsmType: aws.String("hsm9"), SubnetIds: []string{"subnet-a"}},
			"hsm_type",
			"CloudHsmInvalidRequestException",
		},
		{
			&sdk.CreateClusterInput{HsmType: aws.String("hsm1.medium"), SubnetIds: []string{}},
			"no_subnets",
			"CloudHsmInvalidRequestException",
		},
		{
			&sdk.CreateClusterInput{HsmType: aws.String("hsm1.medium"), SubnetIds: []string{"subnet-zzz"}},
			"unknown_subnet", "CloudHsmInvalidRequestException",
		},
		{
			&sdk.CreateClusterInput{HsmType: aws.String("hsm1.medium"), SubnetIds: []string{"subnet-a", "subnet-c"}},
			"mixed_vpc", "CloudHsmInvalidRequestException",
		},
		{
			&sdk.CreateClusterInput{HsmType: aws.String("hsm1.medium"), SubnetIds: []string{"subnet-a", "subnet-d"}},
			"same_az", "CloudHsmInvalidRequestException",
		},
		{
			&sdk.CreateClusterInput{
				HsmType: aws.String("hsm1.medium"), SubnetIds: []string{"subnet-a"}, Mode: types.ClusterModeNonFips,
			},
			"v1_non_fips", "CloudHsmInvalidRequestException",
		},
		{
			&sdk.CreateClusterInput{
				HsmType:   aws.String("hsm1.medium"),
				SubnetIds: []string{"subnet-a"},
				BackupRetentionPolicy: &types.BackupRetentionPolicy{
					Type:  types.BackupRetentionTypeDays,
					Value: aws.String("3"),
				},
			},
			"retention_range",
			"CloudHsmInvalidRequestException",
		},
		{
			&sdk.CreateClusterInput{
				HsmType: aws.String("hsm1.medium"), SubnetIds: []string{"subnet-a"},
				TagList: []types.Tag{{Key: aws.String("aws:x"), Value: aws.String("v")}},
			},
			"reserved_tag", "CloudHsmTagException",
		},
		{
			&sdk.CreateClusterInput{
				HsmType: aws.String(
					"hsm1.medium",
				),
				SubnetIds:      []string{"subnet-a"},
				SourceBackupId: aws.String("backup-missing"),
			},
			"missing_backup",
			"CloudHsmResourceNotFoundException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.client.CreateCluster(t.Context(), tc.in)
			require.Error(t, err)
			assert.Equal(t, tc.code, apiCode(err))
		})
	}

	assert.Equal(t, 0, f.backend.ClusterCount())
}

func testInitializeCluster(t *testing.T, f *fixture) {
	t.Helper()

	c := f.createCluster(t)

	got := f.describeOne(t, aws.ToString(c.ClusterId))
	assert.Equal(t, types.ClusterStateCreateInProgress, got.State)

	_, err := f.client.InitializeCluster(t.Context(), &sdk.InitializeClusterInput{
		ClusterId: c.ClusterId, SignedCert: aws.String("x"), TrustAnchor: aws.String("y"),
	})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err), "cluster is not UNINITIALIZED yet")

	f.clock.advance(time.Second)

	got = f.describeOne(t, aws.ToString(c.ClusterId))
	signed, root := signCSR(t, aws.ToString(got.Certificates.ClusterCsr))

	out, err := f.client.InitializeCluster(t.Context(), &sdk.InitializeClusterInput{
		ClusterId: c.ClusterId, SignedCert: aws.String(signed), TrustAnchor: aws.String(root),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ClusterStateInitializeInProgress, out.State)

	f.clock.advance(time.Second)

	got = f.describeOne(t, aws.ToString(c.ClusterId))
	assert.Equal(t, types.ClusterStateInitialized, got.State)
	assert.Equal(t, signed, aws.ToString(got.Certificates.ClusterCertificate))
	assert.Nil(t, got.Certificates.ClusterCsr)

	_, err = f.client.InitializeCluster(t.Context(), &sdk.InitializeClusterInput{
		ClusterId: c.ClusterId, SignedCert: aws.String(signed), TrustAnchor: aws.String(root),
	})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err), "already initialized")
}

func testInitializeClusterRejects(t *testing.T, f *fixture) {
	t.Helper()

	c := f.createCluster(t)
	f.clock.advance(time.Second)

	got := f.describeOne(t, aws.ToString(c.ClusterId))
	signed, root := signCSR(t, aws.ToString(got.Certificates.ClusterCsr))

	otherKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	foreignCert, foreignRoot := issue(t, &otherKey.PublicKey)

	cases := []struct {
		name, signed, anchor string
	}{
		{"not_pem", "garbage", root},
		{"wrong_key", foreignCert, foreignRoot},
		{"wrong_anchor", signed, foreignRoot},
		{"anchor_is_leaf", signed, signed},
		{"too_long", string(make([]byte, 5001)), root},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, ierr := f.client.InitializeCluster(t.Context(), &sdk.InitializeClusterInput{
				ClusterId: c.ClusterId, SignedCert: aws.String(tc.signed), TrustAnchor: aws.String(tc.anchor),
			})
			assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(ierr))
		})
	}

	assert.Equal(t, types.ClusterStateUninitialized, f.describeOne(t, aws.ToString(c.ClusterId)).State)
}

func testHsms(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	c := f.initializedCluster(t)
	id := c.ClusterId

	created, err := f.client.CreateHsm(ctx, &sdk.CreateHsmInput{
		ClusterId: id, AvailabilityZone: aws.String("us-east-1a"), IpAddress: aws.String("10.0.5.5"),
	})
	require.NoError(t, err)

	h := created.Hsm
	assert.Regexp(t, `^hsm-[0-9a-f]{11}$`, aws.ToString(h.HsmId))
	assert.Equal(t, types.HsmStateCreateInProgress, h.State)
	assert.Equal(t, "10.0.5.5", aws.ToString(h.EniIp))
	assert.Equal(t, "subnet-a", aws.ToString(h.SubnetId))
	assert.Regexp(t, `^eni-`, aws.ToString(h.EniId))

	_, err = f.client.CreateHsm(ctx, &sdk.CreateHsmInput{ClusterId: id, AvailabilityZone: aws.String("us-east-1a")})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err), "second HSM in the AZ")

	_, err = f.client.CreateHsm(ctx, &sdk.CreateHsmInput{ClusterId: id, AvailabilityZone: aws.String("us-east-1z")})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err), "AZ outside the cluster")

	f.clock.advance(time.Second)

	got := f.describeOne(t, aws.ToString(id))
	assert.Equal(t, types.ClusterStateActive, got.State)
	require.Len(t, got.Hsms, 1)
	assert.Equal(t, types.HsmStateActive, got.Hsms[0].State)

	_, err = f.client.DeleteCluster(ctx, &sdk.DeleteClusterInput{ClusterId: id})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err), "cluster still has an HSM")

	_, err = f.client.DeleteHsm(ctx, &sdk.DeleteHsmInput{ClusterId: id})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err), "needs an HSM identifier")

	del, err := f.client.DeleteHsm(ctx, &sdk.DeleteHsmInput{ClusterId: id, EniIp: aws.String("10.0.5.5")})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(h.HsmId), aws.ToString(del.HsmId))
	assert.Equal(t, 1, f.net.eniCount())

	f.clock.advance(time.Second)

	got = f.describeOne(t, aws.ToString(id))
	assert.Equal(t, types.ClusterStateInitialized, got.State)
	assert.Empty(t, got.Hsms)

	_, err = f.client.DeleteHsm(ctx, &sdk.DeleteHsmInput{ClusterId: id, HsmId: h.HsmId})
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err))
	assert.Equal(t, 0, f.net.eniCount(), "ENI released once the HSM is gone")
}

func testModifyCluster(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	c := f.initializedCluster(t)

	_, err := f.client.ModifyCluster(ctx, &sdk.ModifyClusterInput{ClusterId: c.ClusterId})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err), "nothing to modify")

	out, err := f.client.ModifyCluster(ctx, &sdk.ModifyClusterInput{
		ClusterId: c.ClusterId,
		BackupRetentionPolicy: &types.BackupRetentionPolicy{
			Type:  types.BackupRetentionTypeDays,
			Value: aws.String("30"),
		},
		HsmType: aws.String("hsm2m.medium"),
	})
	require.NoError(t, err)
	assert.Equal(t, "30", aws.ToString(out.Cluster.BackupRetentionPolicy.Value))
	assert.Equal(t, "hsm2m.medium", aws.ToString(out.Cluster.HsmType))
	assert.Equal(t, types.ClusterStateModifyInProgress, out.Cluster.State)
	require.NotNil(t, out.Cluster.HsmTypeRollbackExpiration)

	_, err = f.client.ModifyCluster(
		ctx,
		&sdk.ModifyClusterInput{ClusterId: c.ClusterId, HsmType: aws.String("hsm1.medium")},
	)
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err), "still modifying")

	f.clock.advance(time.Second)

	back, err := f.client.ModifyCluster(
		ctx,
		&sdk.ModifyClusterInput{ClusterId: c.ClusterId, HsmType: aws.String("hsm1.medium")},
	)
	require.NoError(t, err)
	assert.Equal(t, "hsm1.medium", aws.ToString(back.Cluster.HsmType))

	f.clock.advance(time.Second)

	_, err = f.client.ModifyCluster(
		ctx,
		&sdk.ModifyClusterInput{ClusterId: aws.String("cluster-nope"), HsmType: aws.String("hsm2m.medium")},
	)
	assert.Equal(t, "CloudHsmResourceNotFoundException", apiCode(err))
}

func testDescribeClustersFilters(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	a := f.createCluster(t)
	f.clock.advance(600 * time.Millisecond)
	b := f.createCluster(t)

	cases := []struct {
		filters map[string][]string
		name    string
		want    []string
	}{
		{nil, "all", []string{aws.ToString(a.ClusterId), aws.ToString(b.ClusterId)}},
		{map[string][]string{"clusterIds": {aws.ToString(b.ClusterId)}}, "by_id", []string{aws.ToString(b.ClusterId)}},
		{map[string][]string{"states": {"UNINITIALIZED"}}, "by_state", []string{aws.ToString(a.ClusterId)}},
		{map[string][]string{"vpcIds": {"vpc-9"}}, "by_vpc", nil},
	}

	f.clock.advance(200 * time.Millisecond)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := f.client.DescribeClusters(ctx, &sdk.DescribeClustersInput{Filters: tc.filters})
			require.NoError(t, err)

			var got []string
			for _, c := range out.Clusters {
				got = append(got, aws.ToString(c.ClusterId))
			}

			assert.Equal(t, tc.want, got)
		})
	}

	_, err := f.client.DescribeClusters(ctx, &sdk.DescribeClustersInput{Filters: map[string][]string{"bogus": {"x"}}})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err))

	page1, err := f.client.DescribeClusters(ctx, &sdk.DescribeClustersInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.Clusters, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := f.client.DescribeClusters(
		ctx,
		&sdk.DescribeClustersInput{MaxResults: aws.Int32(1), NextToken: page1.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page2.Clusters, 1)
	assert.Nil(t, page2.NextToken)
	assert.NotEqual(t, aws.ToString(page1.Clusters[0].ClusterId), aws.ToString(page2.Clusters[0].ClusterId))

	_, err = f.client.DescribeClusters(ctx, &sdk.DescribeClustersInput{NextToken: aws.String("!!")})
	assert.Equal(t, "CloudHsmInvalidRequestException", apiCode(err))
}

func testClusterLimit(t *testing.T, f *fixture) {
	t.Helper()

	for range 4 {
		f.createCluster(t)
	}

	_, err := f.client.CreateCluster(t.Context(), &sdk.CreateClusterInput{
		HsmType: aws.String("hsm1.medium"), SubnetIds: []string{"subnet-a"},
	})
	assert.Equal(t, "CloudHsmResourceLimitExceededException", apiCode(err))
}
