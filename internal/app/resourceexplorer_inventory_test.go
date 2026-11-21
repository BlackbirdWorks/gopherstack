package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	retypes "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
)

func TestResourceExplorerInventoryUntaggedServices(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ctx := t.Context()
	re := resourceexplorer2.NewFromConfig(fx.cfg)
	byName := serviceByName(fx.services)

	ecsH, ok := byName["ECS"].(*ecsbackend.Handler)
	require.True(t, ok)

	ecsBk, ok := ecsH.Backend.(*ecsbackend.InMemoryBackend)
	require.True(t, ok)

	_, err := ecsBk.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: "re-inv"})
	require.NoError(t, err)

	rdsH, ok := byName["RDS"].(*rdsbackend.Handler)
	require.True(t, ok)

	_, err = rdsH.Backend.CreateDBInstance("re-inv-db", "postgres", "db.t3.micro", "", "admin", "", 20,
		rdsbackend.DBInstanceOptions{})
	require.NoError(t, err)

	cwH, ok := byName["CloudWatchLogs"].(*cwlogsbackend.Handler)
	require.True(t, ok)

	cwBk, ok := cwH.Backend.(*cwlogsbackend.InMemoryBackend)
	require.True(t, ok)

	_, err = cwBk.CreateLogGroup(ctx, "/re/inv", "", "")
	require.NoError(t, err)

	iamH, ok := byName["IAM"].(*iambackend.Handler)
	require.True(t, ok)

	iamBk, ok := iamH.Backend.(*iambackend.InMemoryBackend)
	require.True(t, ok)

	_, err = iamBk.CreateRole("re-inv-role", "/", `{"Version":"2012-10-17","Statement":[]}`, "")
	require.NoError(t, err)

	cfnH, ok := byName["CloudFormation"].(*cfnbackend.Handler)
	require.True(t, ok)

	_, err = cfnH.Backend.CreateStack(ctx, "re-inv-stack", `{"Resources":{"T":{"Type":"AWS::SNS::Topic"}}}`, nil,
		cfnbackend.StackOptions{})
	require.NoError(t, err)

	elbv2H, ok := byName["ELBv2"].(*elbv2backend.Handler)
	require.True(t, ok)

	_, err = elbv2H.Backend.CreateLoadBalancer(
		elbv2backend.CreateLoadBalancerInput{Name: "re-inv-nlb", Type: "network"},
	)
	require.NoError(t, err)

	elbH, ok := byName["ELB"].(*elbbackend.Handler)
	require.True(t, ok)

	_, err = elbH.Backend.CreateLoadBalancer(ctx, elbbackend.CreateLoadBalancerInput{
		LoadBalancerName: "re-inv-clb", AvailabilityZones: []string{"us-east-1a"},
		Listeners: []elbbackend.Listener{{Protocol: "HTTP", LoadBalancerPort: 80, InstancePort: 80}},
	})
	require.NoError(t, err)

	_, err = re.CreateIndex(ctx, &resourceexplorer2.CreateIndexInput{})
	require.NoError(t, err)

	view, err := re.CreateView(ctx, &resourceexplorer2.CreateViewInput{ViewName: aws.String("all")})
	require.NoError(t, err)

	_, err = re.AssociateDefaultView(ctx, &resourceexplorer2.AssociateDefaultViewInput{ViewArn: view.View.ViewArn})
	require.NoError(t, err)

	tests := []struct {
		name     string
		query    string
		wantType string
		wantID   string
	}{
		{name: "ecs cluster", query: "service:ecs", wantType: "ecs:cluster", wantID: "re-inv"},
		{name: "rds instance", query: "service:rds", wantType: "rds:db", wantID: "re-inv-db"},
		{name: "log group", query: "service:logs", wantType: "logs:log-group", wantID: "/re/inv"},
		{name: "iam role", query: "service:iam", wantType: "iam:role", wantID: "re-inv-role"},
		{
			name: "cloudformation stack", query: "service:cloudformation",
			wantType: "cloudformation:stack", wantID: "re-inv-stack",
		},
		{
			name: "network load balancer", query: "service:elasticloadbalancing",
			wantType: "elasticloadbalancing:loadbalancer/net", wantID: "re-inv-nlb",
		},
		{
			name: "classic load balancer", query: "service:elasticloadbalancing",
			wantType: "elasticloadbalancing:loadbalancer", wantID: "re-inv-clb",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, serr := re.Search(t.Context(), &resourceexplorer2.SearchInput{
				QueryString: aws.String(tc.query), MaxResults: aws.Int32(100),
			})
			require.NoError(t, serr)

			count := 0

			var found retypes.Resource

			for _, r := range out.Resources {
				if containsStr(aws.ToString(r.Arn), tc.wantID) && aws.ToString(r.ResourceType) == tc.wantType {
					count++
					found = r
				}
			}

			require.Equal(t, 1, count, "%s should list %s exactly once", tc.query, tc.wantID)
			assert.Equal(t, "000000000000", aws.ToString(found.OwningAccountId))
			assert.NotNil(t, found.LastReportedAt)
		})
	}
}
