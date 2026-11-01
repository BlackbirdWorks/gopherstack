package ocicontainerengine_test

import (
	"net/http"
	"testing"

	ke "github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetClusterOptions(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		name string
		all  *bool
		want []string
	}{
		{"latest patch per minor", nil, []string{"v1.34.10", "v1.35.2", "v1.36.4"}},
		{"all patches", new(true), []string{"v1.34.1", "v1.34.2", "v1.34.10", "v1.35.2", "v1.36.1", "v1.36.4"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.ce.GetClusterOptions(t.Context(), ke.GetClusterOptionsRequest{
				ClusterOptionId: new("all"), ShouldListAllPatchVersions: tt.all,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, resp.KubernetesVersions)
			require.Len(t, resp.ClusterPodNetworkOptions, 2)
		})
	}

	_, err := e.ce.GetClusterOptions(
		t.Context(),
		ke.GetClusterOptionsRequest{ClusterOptionId: new("nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestGetNodePoolOptions(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.mkCluster(t, "opts", nil)

	tests := []struct {
		name       string
		req        ke.GetNodePoolOptionsRequest
		wantShapes []string
		wantImages []string
		wantVers   []string
	}{
		{
			"all", ke.GetNodePoolOptionsRequest{NodePoolOptionId: new("all")},
			[]string{shapeFlex, shapeFixed, shapeArm}, []string{fakeImages()[0].Name, fakeImages()[1].Name},
			[]string{"v1.34.10", "v1.35.2", "v1.36.4"},
		},
		{
			"arm only", ke.GetNodePoolOptionsRequest{
				NodePoolOptionId: new("all"), NodePoolOsArch: ke.GetNodePoolOptionsNodePoolOsArchAarch64},
			[]string{shapeArm}, []string{fakeImages()[1].Name}, []string{"v1.34.10", "v1.35.2", "v1.36.4"},
		},
		{
			"ubuntu",
			ke.GetNodePoolOptionsRequest{
				NodePoolOptionId: new("all"), NodePoolOsType: ke.GetNodePoolOptionsNodePoolOsTypeUbuntu},
			[]string{
				shapeFlex,
				shapeFixed,
				shapeArm,
			},
			[]string{fakeImages()[2].Name},
			[]string{"v1.34.10", "v1.35.2", "v1.36.4"},
		},
		{
			"ol7 has none", ke.GetNodePoolOptionsRequest{
				NodePoolOptionId: new("all"), NodePoolOsType: ke.GetNodePoolOptionsNodePoolOsTypeOl7},
			[]string{shapeFlex, shapeFixed, shapeArm}, []string{}, []string{"v1.34.10", "v1.35.2", "v1.36.4"},
		},
		{
			"for cluster", ke.GetNodePoolOptionsRequest{NodePoolOptionId: c.Id},
			[]string{shapeFlex, shapeFixed, shapeArm}, []string{fakeImages()[0].Name, fakeImages()[1].Name},
			[]string{"v1.34.1", "v1.34.2"},
		},
		{
			"version filter",
			ke.GetNodePoolOptionsRequest{NodePoolOptionId: new("all"), NodePoolK8sVersion: new("v1.35.2")},
			[]string{
				shapeFlex,
				shapeFixed,
				shapeArm,
			},
			[]string{fakeImages()[0].Name, fakeImages()[1].Name},
			[]string{"v1.35.2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.ce.GetNodePoolOptions(t.Context(), tt.req)
			require.NoError(t, err)
			assert.Equal(t, tt.wantShapes, resp.Shapes)
			assert.Equal(t, tt.wantImages, resp.Images)
			assert.Equal(t, tt.wantVers, resp.KubernetesVersions)
			assert.Len(t, resp.Sources, len(tt.wantImages))
		})
	}

	resp, err := e.ce.GetNodePoolOptions(
		t.Context(),
		ke.GetNodePoolOptionsRequest{NodePoolOptionId: new("all")},
	)
	require.NoError(t, err)

	src, ok := resp.Sources[0].(ke.NodeSourceViaImageOption)
	require.True(t, ok)
	assert.Equal(t, imgOL8, *src.ImageId)

	_, err = e.ce.GetNodePoolOptions(
		t.Context(),
		ke.GetNodePoolOptionsRequest{NodePoolOptionId: new("ocid1.cluster.oc1.iad.nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestNodePoolOptionsStandalone(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{noNet: true})

	resp, err := e.ce.GetNodePoolOptions(
		t.Context(),
		ke.GetNodePoolOptionsRequest{NodePoolOptionId: new("all")},
	)
	require.NoError(t, err)
	assert.Empty(t, resp.Shapes)
	assert.NotEmpty(t, resp.KubernetesVersions)
}
