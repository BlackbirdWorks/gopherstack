package dax_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

func newTestBackend() *dax.InMemoryBackend {
	return dax.NewInMemoryBackend("123456789012", "us-east-1")
}

func validCreateInput(name string) dax.CreateClusterInput {
	return dax.CreateClusterInput{
		ClusterName:       name,
		NodeType:          "dax.r5.large",
		IamRoleArn:        "arn:aws:iam::123456789012:role/DAXRole",
		ReplicationFactor: 1,
	}
}

// ---- Reset ----

func TestReset(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateCluster(validCreateInput("temp"))
	require.NoError(t, err)

	b.Reset()

	clusters, _, err := b.DescribeClusters(nil, 0, "")
	require.NoError(t, err)
	assert.Empty(t, clusters)

	groups, _, err := b.DescribeParameterGroups(nil, 0, "")
	require.NoError(t, err)
	assert.NotEmpty(t, groups)
}
