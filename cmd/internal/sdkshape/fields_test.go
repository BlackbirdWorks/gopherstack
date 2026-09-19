package sdkshape_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cmd/internal/sdkshape"
)

// clusterTypesFixture mirrors the real eks types/types.go shape: no struct
// tags at all (the SDK's own generated code relies on custom (de)serializers,
// not encoding/json), a required-member doc comment, and a nested named
// struct field.
const clusterTypesFixture = `package types

type Cluster struct {

	// The Amazon Resource Name (ARN) of the cluster.
	Arn *string

	// The current status of the cluster.
	Status ClusterStatus

	// The certificate-authority-data for your cluster.
	CertificateAuthority *Certificate

	noSmithyDocumentSerde
}

type Certificate struct {
	// This member is required.
	Data *string
}
`

const describeClusterOpFixture = `package eks

type DescribeClusterInput struct {
	// This member is required.
	Name *string
}

type DescribeClusterOutput struct {
	Cluster *Cluster
}
`

func TestLoadModuleStructs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "types"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "types", "types.go"), []byte(clusterTypesFixture), 0o600))
	require.NoError(
		t, os.WriteFile(filepath.Join(dir, "api_op_DescribeCluster.go"), []byte(describeClusterOpFixture), 0o600),
	)

	structs, opNames, err := sdkshape.LoadModuleStructs(dir)
	require.NoError(t, err)

	assert.Equal(t, []string{"DescribeCluster"}, opNames)

	require.Contains(t, structs, "Cluster")
	cluster := structs["Cluster"]
	assert.ElementsMatch(t, []sdkshape.Field{
		{Name: "Arn", Type: "*string"},
		{Name: "Status", Type: "ClusterStatus"},
		{Name: "CertificateAuthority", Type: "*Certificate"},
	}, cluster.Fields, "noSmithyDocumentSerde is a marker, never a real field")

	require.Contains(t, structs, "Certificate")
	assert.Equal(t, []sdkshape.Field{{Name: "Data", Type: "*string", Required: true}}, structs["Certificate"].Fields)

	require.Contains(t, structs, "DescribeClusterOutput")
	assert.Equal(
		t, []sdkshape.Field{{Name: "Cluster", Type: "*Cluster"}}, structs["DescribeClusterOutput"].Fields,
	)
}

func TestBareTypeName(t *testing.T) {
	t.Parallel()

	tests := []struct{ in, want string }{
		{"*string", "string"},
		{"*types.ClusterStatus", "ClusterStatus"},
		{"ClusterStatus", "ClusterStatus"},
		{"[]*types.Tag", "Tag"},
		{"[]string", "string"},
		{"map[string]*string", "string"},
		{"map[string]*types.Foo", "Foo"},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, sdkshape.BareTypeName(tc.in))
		})
	}
}
