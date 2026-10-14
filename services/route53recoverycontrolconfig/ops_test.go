package route53recoverycontrolconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig"
	"github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type topology struct {
	cluster, defaultPanel, customPanel, ctlA, ctlB, ctlC string
}

func (f *fixture) topology(t *testing.T) topology {
	t.Helper()

	ctx := t.Context()
	cl, err := f.client.CreateCluster(ctx, &sdk.CreateClusterInput{ClusterName: aws.String("prod")})
	require.NoError(t, err)

	arn := aws.ToString(cl.Cluster.ClusterArn)
	panels, err := f.client.ListControlPanels(ctx, &sdk.ListControlPanelsInput{ClusterArn: aws.String(arn)})
	require.NoError(t, err)
	require.Len(t, panels.ControlPanels, 1)

	custom, err := f.client.CreateControlPanel(ctx, &sdk.CreateControlPanelInput{
		ClusterArn: aws.String(arn), ControlPanelName: aws.String("custom"),
	})
	require.NoError(t, err)

	ctl := func(name string, panel *string) string {
		out, cErr := f.client.CreateRoutingControl(ctx, &sdk.CreateRoutingControlInput{
			ClusterArn: aws.String(arn), RoutingControlName: aws.String(name), ControlPanelArn: panel,
		})
		require.NoError(t, cErr)

		return aws.ToString(out.RoutingControl.RoutingControlArn)
	}

	customArn := aws.ToString(custom.ControlPanel.ControlPanelArn)

	return topology{
		cluster: arn, defaultPanel: aws.ToString(panels.ControlPanels[0].ControlPanelArn), customPanel: customArn,
		ctlA: ctl("a", nil), ctlB: ctl("b", nil), ctlC: ctl("c", aws.String(customArn)),
	}
}

func TestClusterLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "create and describe", run: func(t *testing.T, f *fixture) {
			t.Helper()

			out, err := f.client.CreateCluster(t.Context(), &sdk.CreateClusterInput{
				ClusterName: aws.String("c"), NetworkType: types.NetworkTypeDualstack,
				Tags: map[string]string{"env": "dev"},
			})
			require.NoError(t, err)
			assert.Equal(t, types.StatusDeployed, out.Cluster.Status)
			assert.Equal(t, types.NetworkTypeDualstack, out.Cluster.NetworkType)
			assert.Equal(t, testAccount, aws.ToString(out.Cluster.Owner))
			assert.Regexp(
				t,
				`^arn:aws:route53-recovery-control::123456789012:cluster/[0-9a-f]{32}$`,
				aws.ToString(out.Cluster.ClusterArn),
			)
			require.Len(t, out.Cluster.ClusterEndpoints, 5)

			for _, ep := range out.Cluster.ClusterEndpoints {
				assert.Contains(t, aws.ToString(ep.Endpoint), f.url)
				assert.NotEmpty(t, aws.ToString(ep.Region))
			}

			tags, err := f.client.ListTagsForResource(
				t.Context(),
				&sdk.ListTagsForResourceInput{ResourceArn: out.Cluster.ClusterArn},
			)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "dev"}, tags.Tags)
		}},
		{name: "default control panel is created", run: func(t *testing.T, f *fixture) {
			t.Helper()

			topo := f.topology(t)
			out, err := f.client.DescribeControlPanel(
				t.Context(),
				&sdk.DescribeControlPanelInput{ControlPanelArn: aws.String(topo.defaultPanel)},
			)
			require.NoError(t, err)
			assert.True(t, aws.ToBool(out.ControlPanel.DefaultControlPanel))
			assert.Equal(t, "DefaultControlPanel", aws.ToString(out.ControlPanel.Name))
			assert.EqualValues(t, 2, aws.ToInt32(out.ControlPanel.RoutingControlCount))
		}},
		{name: "client token is idempotent", run: func(t *testing.T, f *fixture) {
			t.Helper()

			in := &sdk.CreateClusterInput{ClusterName: aws.String("c"), ClientToken: aws.String("tok")}
			a, err := f.client.CreateCluster(t.Context(), in)
			require.NoError(t, err)
			b, err := f.client.CreateCluster(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(a.Cluster.ClusterArn), aws.ToString(b.Cluster.ClusterArn))

			list, err := f.client.ListClusters(t.Context(), &sdk.ListClustersInput{})
			require.NoError(t, err)
			assert.Len(t, list.Clusters, 1)
		}},
		{name: "update network type", run: func(t *testing.T, f *fixture) {
			t.Helper()

			topo := f.topology(t)
			out, err := f.client.UpdateCluster(t.Context(), &sdk.UpdateClusterInput{
				ClusterArn: aws.String(topo.cluster), NetworkType: types.NetworkTypeDualstack,
			})
			require.NoError(t, err)
			assert.Equal(t, types.NetworkTypeDualstack, out.Cluster.NetworkType)
		}},
		{name: "list paginates", run: func(t *testing.T, f *fixture) {
			t.Helper()

			for _, n := range []string{"c1", "c2", "c3"} {
				_, err := f.client.CreateCluster(t.Context(), &sdk.CreateClusterInput{ClusterName: aws.String(n)})
				require.NoError(t, err)
			}

			p := sdk.NewListClustersPaginator(f.client, &sdk.ListClustersInput{MaxResults: aws.Int32(2)})
			var names []string
			pages := 0

			for p.HasMorePages() {
				pg, err := p.NextPage(t.Context())
				require.NoError(t, err)
				pages++

				for _, c := range pg.Clusters {
					names = append(names, aws.ToString(c.Name))
				}
			}

			assert.Equal(t, []string{"c1", "c2", "c3"}, names)
			assert.Equal(t, 2, pages)
		}},
		{name: "delete empty cluster", run: func(t *testing.T, f *fixture) {
			t.Helper()

			out, err := f.client.CreateCluster(t.Context(), &sdk.CreateClusterInput{ClusterName: aws.String("c")})
			require.NoError(t, err)
			_, err = f.client.DeleteCluster(t.Context(), &sdk.DeleteClusterInput{ClusterArn: out.Cluster.ClusterArn})
			require.NoError(t, err)
			_, err = f.client.DescribeCluster(
				t.Context(),
				&sdk.DescribeClusterInput{ClusterArn: out.Cluster.ClusterArn},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			panels, err := f.client.ListControlPanels(t.Context(), &sdk.ListControlPanelsInput{})
			require.NoError(t, err)
			assert.Empty(t, panels.ControlPanels, "the default panel goes with the cluster")
		}},
		{name: "delete cluster with custom panel conflicts", run: func(t *testing.T, f *fixture) {
			t.Helper()

			topo := f.topology(t)
			_, err := f.client.DeleteCluster(t.Context(), &sdk.DeleteClusterInput{ClusterArn: aws.String(topo.cluster)})
			assert.Equal(t, "ConflictException", apiCode(err))
			assert.Contains(t, apiMessage(err), "custom")
		}},
		{name: "describe unknown", run: func(t *testing.T, f *fixture) {
			t.Helper()

			_, err := f.client.DescribeCluster(
				t.Context(),
				&sdk.DescribeClusterInput{
					ClusterArn: aws.String("arn:aws:route53-recovery-control::123456789012:cluster/nope"),
				},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "invalid name and network type", run: func(t *testing.T, f *fixture) {
			t.Helper()

			_, err := f.client.CreateCluster(t.Context(), &sdk.CreateClusterInput{ClusterName: aws.String("")})
			assert.Equal(t, "ValidationException", apiCode(err))
			_, err = f.client.CreateCluster(
				t.Context(),
				&sdk.CreateClusterInput{ClusterName: aws.String("x"), NetworkType: "IPV9"},
			)
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newFixture(t))
		})
	}
}

func TestControlPanelLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, topo topology)
		name string
	}{
		{name: "rename", run: func(t *testing.T, f *fixture, topo topology) {
			t.Helper()

			out, err := f.client.UpdateControlPanel(t.Context(), &sdk.UpdateControlPanelInput{
				ControlPanelArn: aws.String(topo.customPanel), ControlPanelName: aws.String("renamed"),
			})
			require.NoError(t, err)
			assert.Equal(t, "renamed", aws.ToString(out.ControlPanel.Name))
			assert.EqualValues(t, 1, aws.ToInt32(out.ControlPanel.RoutingControlCount))
		}},
		{name: "delete default conflicts", run: func(t *testing.T, f *fixture, topo topology) {
			t.Helper()

			_, err := f.client.DeleteControlPanel(
				t.Context(),
				&sdk.DeleteControlPanelInput{ControlPanelArn: aws.String(topo.defaultPanel)},
			)
			assert.Equal(t, "ConflictException", apiCode(err))
			assert.Contains(t, apiMessage(err), "default")
		}},
		{name: "delete non-empty conflicts", run: func(t *testing.T, f *fixture, topo topology) {
			t.Helper()

			_, err := f.client.DeleteControlPanel(
				t.Context(),
				&sdk.DeleteControlPanelInput{ControlPanelArn: aws.String(topo.customPanel)},
			)
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "delete empty custom panel", run: func(t *testing.T, f *fixture, topo topology) {
			t.Helper()

			_, err := f.client.DeleteRoutingControl(
				t.Context(),
				&sdk.DeleteRoutingControlInput{RoutingControlArn: aws.String(topo.ctlC)},
			)
			require.NoError(t, err)
			_, err = f.client.DeleteControlPanel(
				t.Context(),
				&sdk.DeleteControlPanelInput{ControlPanelArn: aws.String(topo.customPanel)},
			)
			require.NoError(t, err)
			_, err = f.client.DescribeControlPanel(
				t.Context(),
				&sdk.DescribeControlPanelInput{ControlPanelArn: aws.String(topo.customPanel)},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "list filters by cluster", run: func(t *testing.T, f *fixture, topo topology) {
			t.Helper()

			other, err := f.client.CreateCluster(t.Context(), &sdk.CreateClusterInput{ClusterName: aws.String("other")})
			require.NoError(t, err)

			mine, err := f.client.ListControlPanels(
				t.Context(),
				&sdk.ListControlPanelsInput{ClusterArn: aws.String(topo.cluster)},
			)
			require.NoError(t, err)
			assert.Len(t, mine.ControlPanels, 2)

			theirs, err := f.client.ListControlPanels(
				t.Context(),
				&sdk.ListControlPanelsInput{ClusterArn: other.Cluster.ClusterArn},
			)
			require.NoError(t, err)
			assert.Len(t, theirs.ControlPanels, 1)

			all, err := f.client.ListControlPanels(t.Context(), &sdk.ListControlPanelsInput{})
			require.NoError(t, err)
			assert.Len(t, all.ControlPanels, 3)
		}},
		{name: "create in unknown cluster", run: func(t *testing.T, f *fixture, _ topology) {
			t.Helper()

			_, err := f.client.CreateControlPanel(t.Context(), &sdk.CreateControlPanelInput{
				ClusterArn: aws.String(
					"arn:aws:route53-recovery-control::123456789012:cluster/nope",
				),
				ControlPanelName: aws.String("x"),
			})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			tt.run(t, f, f.topology(t))
		})
	}
}

func TestRoutingControlLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, topo topology)
		name string
	}{
		{
			name: "defaults to the cluster default panel and starts Off",
			run: func(t *testing.T, f *fixture, topo topology) {
				t.Helper()

				out, err := f.client.DescribeRoutingControl(
					t.Context(),
					&sdk.DescribeRoutingControlInput{RoutingControlArn: aws.String(topo.ctlA)},
				)
				require.NoError(t, err)
				assert.Equal(t, topo.defaultPanel, aws.ToString(out.RoutingControl.ControlPanelArn))
				assert.Equal(t, types.StatusDeployed, out.RoutingControl.Status)
				assert.Regexp(
					t,
					`:controlpanel/[0-9a-f]{32}/routingcontrol/[0-9a-f]{12}$`,
					aws.ToString(out.RoutingControl.RoutingControlArn),
				)
			},
		},
		{name: "list by panel", run: func(t *testing.T, f *fixture, topo topology) {
			t.Helper()

			out, err := f.client.ListRoutingControls(
				t.Context(),
				&sdk.ListRoutingControlsInput{ControlPanelArn: aws.String(topo.defaultPanel)},
			)
			require.NoError(t, err)
			require.Len(t, out.RoutingControls, 2)
			assert.Equal(t, "a", aws.ToString(out.RoutingControls[0].Name))
		}},
		{name: "rename", run: func(t *testing.T, f *fixture, topo topology) {
			t.Helper()

			out, err := f.client.UpdateRoutingControl(t.Context(), &sdk.UpdateRoutingControlInput{
				RoutingControlArn: aws.String(topo.ctlA), RoutingControlName: aws.String("alpha"),
			})
			require.NoError(t, err)
			assert.Equal(t, "alpha", aws.ToString(out.RoutingControl.Name))
		}},
		{name: "delete", run: func(t *testing.T, f *fixture, topo topology) {
			t.Helper()

			_, err := f.client.DeleteRoutingControl(
				t.Context(),
				&sdk.DeleteRoutingControlInput{RoutingControlArn: aws.String(topo.ctlA)},
			)
			require.NoError(t, err)
			_, err = f.client.DescribeRoutingControl(
				t.Context(),
				&sdk.DescribeRoutingControlInput{RoutingControlArn: aws.String(topo.ctlA)},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "panel from another cluster is rejected", run: func(t *testing.T, f *fixture, topo topology) {
			t.Helper()

			other, err := f.client.CreateCluster(t.Context(), &sdk.CreateClusterInput{ClusterName: aws.String("other")})
			require.NoError(t, err)
			_, err = f.client.CreateRoutingControl(t.Context(), &sdk.CreateRoutingControlInput{
				ClusterArn:         other.Cluster.ClusterArn,
				RoutingControlName: aws.String("x"),
				ControlPanelArn:    aws.String(topo.customPanel),
			})
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
		{name: "associated health checks without route53", run: func(t *testing.T, f *fixture, topo topology) {
			t.Helper()

			out, err := f.client.ListAssociatedRoute53HealthChecks(
				t.Context(),
				&sdk.ListAssociatedRoute53HealthChecksInput{RoutingControlArn: aws.String(topo.ctlA)},
			)
			require.NoError(t, err)
			assert.Empty(t, out.HealthCheckIds)

			_, err = f.client.ListAssociatedRoute53HealthChecks(
				t.Context(),
				&sdk.ListAssociatedRoute53HealthChecksInput{
					RoutingControlArn: aws.String(topo.ctlA + "x"),
				},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			tt.run(t, f, f.topology(t))
		})
	}
}

func TestTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		arn      func(topology) string
		name     string
		wantCode string
	}{
		{func(o topology) string { return o.cluster }, "cluster", ""},
		{func(o topology) string { return o.customPanel }, "control panel", ""},
		{func(o topology) string { return o.ctlA }, "routing control", "ValidationException"},
		{
			func(topology) string { return "arn:aws:route53-recovery-control::123456789012:cluster/none" },
			"unknown",
			"ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			arn := aws.String(tt.arn(f.topology(t)))

			_, err := f.client.TagResource(
				t.Context(),
				&sdk.TagResourceInput{ResourceArn: arn, Tags: map[string]string{"a": "1", "b": "2"}},
			)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)

			_, err = f.client.UntagResource(
				t.Context(),
				&sdk.UntagResourceInput{ResourceArn: arn, TagKeys: []string{"a"}},
			)
			require.NoError(t, err)

			out, err := f.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: arn})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"b": "2"}, out.Tags)
		})
	}
}

func TestResourcePolicy(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	topo := f.topology(t)

	out, err := f.client.GetResourcePolicy(
		t.Context(),
		&sdk.GetResourcePolicyInput{ResourceArn: aws.String(topo.cluster)},
	)
	require.NoError(t, err)
	assert.Nil(t, out.Policy)

	_, err = f.client.GetResourcePolicy(
		t.Context(),
		&sdk.GetResourcePolicyInput{ResourceArn: aws.String(topo.cluster + "x")},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
