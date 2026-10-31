package route53recoverycontrolconfig_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig"
	"github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

type hostRoundTripper struct {
	rt   http.RoundTripper
	host string
}

func (h hostRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Host = h.host

	return h.rt.RoundTrip(r)
}

func (f *fixture) clientVia(t *testing.T, host string) *sdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion("us-west-2"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return sdk.NewFromConfig(cfg, func(o *sdk.Options) {
		o.BaseEndpoint = aws.String(f.url)
		o.HTTPClient = &http.Client{Transport: hostRoundTripper{host: host, rt: testclient.New(t).Transport}}
	})
}

func TestClusterEndpointsFollowCallerHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		host     string
		external string
		want     string
	}{
		{name: "docker host", host: "host.docker.internal:4566", want: "http://host.docker.internal:4566"},
		{name: "ip host", host: "10.1.2.3:8123", want: "http://10.1.2.3:8123"},
		{name: "explicit endpoint wins", host: "host.docker.internal:4566", external: "https://arc.example.com:9443/",
			want: "https://arc.example.com:9443"},
		{name: "host with path falls back", host: "evil.example/x", want: "SERVER"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetExternalEndpoint(tt.external)

			want := tt.want
			if want == "SERVER" {
				want = f.url
			}

			client := f.clientVia(t, tt.host)
			ctx := t.Context()

			created, err := client.CreateCluster(ctx, &sdk.CreateClusterInput{ClusterName: aws.String("c")})
			require.NoError(t, err)

			described, err := client.DescribeCluster(
				ctx,
				&sdk.DescribeClusterInput{ClusterArn: created.Cluster.ClusterArn},
			)
			require.NoError(t, err)
			require.Len(t, described.Cluster.ClusterEndpoints, 5)

			list, err := client.ListClusters(ctx, &sdk.ListClustersInput{})
			require.NoError(t, err)
			require.Len(t, list.Clusters, 1)

			for _, eps := range [][]string{
				endpointsOf(created.Cluster), endpointsOf(described.Cluster), endpointsOf(&list.Clusters[0]),
			} {
				require.Len(t, eps, 5)

				for _, ep := range eps {
					assert.True(t, strings.HasPrefix(ep, want+"/route53-recovery-cluster/"), "%s vs %s", ep, want)
				}
			}
		})
	}
}

func endpointsOf(c *types.Cluster) []string {
	out := make([]string, 0, len(c.ClusterEndpoints))
	for _, e := range c.ClusterEndpoints {
		out = append(out, aws.ToString(e.Endpoint))
	}

	return out
}
