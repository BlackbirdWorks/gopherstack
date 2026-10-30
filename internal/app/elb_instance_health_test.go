package app

import (
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elbsdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbtypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
)

func TestInitializeServices_ELBInstanceHealthFromEC2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		mutate      func(t *testing.T, b ec2backend.Backend, id string)
		state       string
		reasonCode  string
		description string
	}{
		{name: "running", state: "InService", reasonCode: "N/A", description: "N/A"},
		{
			name: "stopped",
			mutate: func(t *testing.T, b ec2backend.Backend, id string) {
				t.Helper()
				_, err := b.StopInstances([]string{id})
				require.NoError(t, err)
			},
			state: "OutOfService", reasonCode: "Instance", description: "Instance is in stopped state.",
		},
		{
			name: "terminated",
			mutate: func(t *testing.T, b ec2backend.Backend, id string) {
				t.Helper()
				_, err := b.TerminateInstances([]string{id})
				require.NoError(t, err)
			},
			state: "OutOfService", reasonCode: "Instance", description: "Instance is in terminated state.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cli := &CLI{AccountID: "000000000000"}
			appCtx := &service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()}
			cli.faultStore = chaos.NewFaultStore()

			services, err := initializeServicesT(t, appCtx)
			require.NoError(t, err)

			byName := serviceByName(services)
			elbH, ok := byName["ELB"].(*elbbackend.Handler)
			require.True(t, ok)
			ec2H, ok := byName["EC2"].(*ec2backend.Handler)
			require.True(t, ok)

			insts, err := ec2H.Backend.RunInstances("ami-12345678", "t3.micro", "", 1)
			require.NoError(t, err)
			id := insts[0].ID

			require.Eventually(t, func() bool {
				got := ec2H.Backend.DescribeInstances([]string{id}, "")

				return len(got) == 1 && got[0].State.Name == "running"
			}, 5*time.Second, 10*time.Millisecond)

			vpc, err := ec2H.Backend.CreateVpc("10.0.0.0/16", "default")
			require.NoError(t, err)
			subnet, err := ec2H.Backend.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
			require.NoError(t, err)

			e := echo.New()
			registry := service.NewRegistry()
			require.NoError(t, registry.Register(elbH))
			e.Use(service.NewServiceRouter(registry).RouteHandler())
			srv := httptest.NewServer(e)
			t.Cleanup(srv.Close)

			cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
				awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
			require.NoError(t, err)
			client := elbsdk.NewFromConfig(cfg, func(o *elbsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

			_, err = client.CreateLoadBalancer(t.Context(), &elbsdk.CreateLoadBalancerInput{
				LoadBalancerName: aws.String("health-elb"),
				Subnets:          []string{subnet.ID},
				Listeners: []elbtypes.Listener{{
					Protocol: aws.String("HTTP"), LoadBalancerPort: 80,
					InstanceProtocol: aws.String("HTTP"), InstancePort: aws.Int32(8080),
				}},
			})
			require.NoError(t, err)

			_, err = client.RegisterInstancesWithLoadBalancer(
				t.Context(),
				&elbsdk.RegisterInstancesWithLoadBalancerInput{
					LoadBalancerName: aws.String("health-elb"),
					Instances:        []elbtypes.Instance{{InstanceId: aws.String(id)}},
				},
			)
			require.NoError(t, err)

			if tt.mutate != nil {
				tt.mutate(t, ec2H.Backend, id)
			}

			for _, in := range []*elbsdk.DescribeInstanceHealthInput{
				{LoadBalancerName: aws.String("health-elb")},
				{LoadBalancerName: aws.String("health-elb"), Instances: []elbtypes.Instance{{InstanceId: aws.String(id)}}},
			} {
				out, descErr := client.DescribeInstanceHealth(t.Context(), in)
				require.NoError(t, descErr)
				require.Len(t, out.InstanceStates, 1)
				got := out.InstanceStates[0]
				assert.Equal(t, tt.state, aws.ToString(got.State))
				assert.Equal(t, tt.reasonCode, aws.ToString(got.ReasonCode))
				assert.Equal(t, tt.description, aws.ToString(got.Description))
			}
		})
	}
}
