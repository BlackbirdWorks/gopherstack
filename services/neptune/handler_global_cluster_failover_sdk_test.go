package neptune_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/aws/aws-sdk-go-v2/service/neptune/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// Test_SDKRoundTrip_FailoverGlobalCluster_UnknownTargetIsDBClusterNotFound
// proves FailoverGlobalCluster/SwitchoverGlobalCluster to a
// TargetDbClusterIdentifier that names no DB cluster this backend tracks
// rejects with the real typed DBClusterNotFoundFault (neptune@v1.48.4
// deserializers.go:5338/8147 both declare it) instead of the previous silent
// no-op.
func Test_SDKRoundTrip_FailoverGlobalCluster_UnknownTargetIsDBClusterNotFound(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)

	_, err := client.CreateGlobalCluster(t.Context(), &neptunesdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("gc-sdk-unknown"),
	})
	require.NoError(t, err)

	_, err = client.FailoverGlobalCluster(t.Context(), &neptunesdk.FailoverGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("gc-sdk-unknown"),
		TargetDbClusterIdentifier: aws.String("no-such-cluster"),
	})
	require.Error(t, err)
	var notFound *types.DBClusterNotFoundFault
	require.ErrorAs(t, err, &notFound)

	_, err = client.SwitchoverGlobalCluster(t.Context(), &neptunesdk.SwitchoverGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("gc-sdk-unknown"),
		TargetDbClusterIdentifier: aws.String("no-such-cluster"),
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &notFound)
}

// Test_SDKRoundTrip_FailoverGlobalCluster_NonMemberIsInvalidState proves a
// real DB cluster that is not a member of the named global cluster is
// rejected with InvalidDBClusterStateFault, not attached as a surprise new
// member (the pre-fix "attach as writer" fallback).
func Test_SDKRoundTrip_FailoverGlobalCluster_NonMemberIsInvalidState(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)

	_, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("gc-sdk-primary"),
		Engine:              aws.String("neptune"),
	})
	require.NoError(t, err)
	_, err = client.CreateGlobalCluster(t.Context(), &neptunesdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("gc-sdk-nonmember"),
		SourceDBClusterIdentifier: aws.String("gc-sdk-primary"),
	})
	require.NoError(t, err)
	_, err = client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("gc-sdk-unrelated"),
		Engine:              aws.String("neptune"),
	})
	require.NoError(t, err)

	_, err = client.FailoverGlobalCluster(t.Context(), &neptunesdk.FailoverGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("gc-sdk-nonmember"),
		TargetDbClusterIdentifier: aws.String("gc-sdk-unrelated"),
	})
	require.Error(t, err)
	var invalidState *types.InvalidDBClusterStateFault
	require.ErrorAs(t, err, &invalidState)
}

// Test_SDKRoundTrip_FailoverGlobalCluster_JoinedMemberPromotes proves the
// happy path end-to-end through the real SDK client: a cluster that joins a
// global cluster via CreateDBCluster's GlobalClusterIdentifier (the real
// Neptune join path, api_op_CreateDBCluster.go:129) is a genuine member, so
// failing over to it succeeds and DescribeGlobalClusters reflects the new
// writer.
func Test_SDKRoundTrip_FailoverGlobalCluster_JoinedMemberPromotes(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)

	_, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("gc-sdk-join-primary"),
		Engine:              aws.String("neptune"),
	})
	require.NoError(t, err)
	_, err = client.CreateGlobalCluster(t.Context(), &neptunesdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("gc-sdk-join"),
		SourceDBClusterIdentifier: aws.String("gc-sdk-join-primary"),
	})
	require.NoError(t, err)
	_, err = client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier:     aws.String("gc-sdk-join-secondary"),
		Engine:                  aws.String("neptune"),
		GlobalClusterIdentifier: aws.String("gc-sdk-join"),
	})
	require.NoError(t, err)

	out, err := client.FailoverGlobalCluster(t.Context(), &neptunesdk.FailoverGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("gc-sdk-join"),
		TargetDbClusterIdentifier: aws.String("gc-sdk-join-secondary"),
	})
	require.NoError(t, err)
	require.Len(t, out.GlobalCluster.GlobalClusterMembers, 2)
	writers := 0
	secondaryIsWriter := false
	for _, m := range out.GlobalCluster.GlobalClusterMembers {
		if !aws.ToBool(m.IsWriter) {
			continue
		}
		writers++
		if strings.HasSuffix(aws.ToString(m.DBClusterArn), "gc-sdk-join-secondary") {
			secondaryIsWriter = true
		}
	}
	assert.Equal(t, 1, writers, "exactly one member must be the writer after failover")
	assert.True(t, secondaryIsWriter, "the failover target must become the writer")

	// Failing over to the now-writer secondary a second time must reject as
	// already-primary rather than silently succeeding.
	_, err = client.FailoverGlobalCluster(t.Context(), &neptunesdk.FailoverGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("gc-sdk-join"),
		TargetDbClusterIdentifier: aws.String("gc-sdk-join-secondary"),
	})
	require.Error(t, err)
	var invalidState *types.InvalidDBClusterStateFault
	require.ErrorAs(t, err, &invalidState)
}
