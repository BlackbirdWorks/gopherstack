package route53recoveryreadiness_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/route53recoveryreadiness"
	"github.com/aws/aws-sdk-go-v2/service/route53recoveryreadiness/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	apigw "github.com/blackbirdworks/gopherstack/services/apigateway"
	apigwv2 "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	asg "github.com/blackbirdworks/gopherstack/services/autoscaling"
	ec2bk "github.com/blackbirdworks/gopherstack/services/ec2"
	elbbk "github.com/blackbirdworks/gopherstack/services/elb"
	kafkabk "github.com/blackbirdworks/gopherstack/services/kafka"
	rdsbk "github.com/blackbirdworks/gopherstack/services/rds"
	snsbk "github.com/blackbirdworks/gopherstack/services/sns"
)

// evalSiblings adds the siblings needed by the per-type evaluators.
type evalSiblings struct {
	*siblings
	apigw, apigwv2, asg, ec2, elb, kafka, rds, sns service.Registerable
}

func (s *evalSiblings) GetAPIGatewayHandler() service.Registerable   { return s.apigw }
func (s *evalSiblings) GetAPIGatewayV2Handler() service.Registerable { return s.apigwv2 }
func (s *evalSiblings) GetAutoscalingHandler() service.Registerable  { return s.asg }
func (s *evalSiblings) GetEC2Handler() service.Registerable          { return s.ec2 }
func (s *evalSiblings) GetELBHandler() service.Registerable          { return s.elb }
func (s *evalSiblings) GetKafkaHandler() service.Registerable        { return s.kafka }
func (s *evalSiblings) GetRDSHandler() service.Registerable          { return s.rds }
func (s *evalSiblings) GetSNSHandler() service.Registerable          { return s.sns }

type evalWorld struct {
	apigw   *apigw.InMemoryBackend
	apigwv2 *apigwv2.InMemoryBackend
	asg     *asg.InMemoryBackend
	ec2     *ec2bk.InMemoryBackend
	elb     *elbbk.InMemoryBackend
	kafka   *kafkabk.InMemoryBackend
	rds     *rdsbk.InMemoryBackend
	sns     *snsbk.InMemoryBackend
}

func newEvalFixture(t *testing.T) (*fixture, *evalWorld) {
	t.Helper()

	f := newFixture(t)
	w := &evalWorld{
		apigw:   apigw.NewInMemoryBackend(),
		apigwv2: apigwv2.NewInMemoryBackend(),
		asg:     asg.NewInMemoryBackendWithConfig(testAccount, testRegion),
		ec2:     ec2bk.NewInMemoryBackend(testAccount, testRegion),
		elb:     elbbk.NewInMemoryBackend(testAccount, testRegion),
		kafka:   kafkabk.NewInMemoryBackend(testAccount, testRegion),
		rds:     rdsbk.NewInMemoryBackend(testAccount, testRegion),
		sns:     snsbk.NewInMemoryBackendWithConfig(testAccount, testRegion),
	}
	t.Cleanup(w.kafka.Close)

	f.backend.SetAppConfig(&evalSiblings{
		siblings: &siblings{},
		apigw:    apigw.NewHandler(w.apigw),
		apigwv2:  apigwv2.NewHandler(w.apigwv2),
		asg:      asg.NewHandler(w.asg),
		ec2:      ec2bk.NewHandler(w.ec2),
		elb:      elbbk.NewHandler(w.elb),
		kafka:    kafkabk.NewHandler(w.kafka),
		rds:      rdsbk.NewHandler(w.rds),
		sns:      snsbk.NewHandler(w.sns),
	})

	return f, w
}

const evalARNPrefix = "arn:aws:"

func evalARN(svc, resource string) string {
	return evalARN3(svc, testRegion, resource)
}

func evalARN3(svc, region, resource string) string {
	return evalARNPrefix + svc + ":" + region + ":" + testAccount + ":" + resource
}

func TestReadinessEvaluatesEveryResourceSetType(t *testing.T) {
	t.Parallel()

	ready := func(ids ...string) map[string]types.Readiness {
		m := map[string]types.Readiness{}
		for _, id := range ids {
			m[id] = types.ReadinessReady
		}

		return m
	}

	tests := []struct {
		create func(t *testing.T, w *evalWorld) string
		want   map[string]types.Readiness
		name   string
		typ    string
		bogus  string
	}{
		{
			name: "api gateway stage", typ: "AWS::ApiGateway::Stage",
			bogus: "arn:aws:apigateway:us-east-1::/restapis/nope/stages/prod",
			want:  ready("ApiGatewayStageExists"),
			create: func(t *testing.T, w *evalWorld) string {
				t.Helper()

				api, err := w.apigw.CreateRestAPI(apigw.CreateRestAPIInput{Name: "a"})
				require.NoError(t, err)
				d, err := w.apigw.CreateDeployment(api.ID, "", "v1")
				require.NoError(t, err)
				_, err = w.apigw.CreateStage(
					apigw.CreateStageInput{RestAPIID: api.ID, StageName: "prod", DeploymentID: d.ID},
				)
				require.NoError(t, err)

				return "arn:aws:apigateway:us-east-1::/restapis/" + api.ID + "/stages/prod"
			},
		},
		{
			name: "api gateway v2 stage", typ: "AWS::ApiGatewayV2::Stage",
			bogus: "arn:aws:apigateway:us-east-1::/apis/nope/stages/prod",
			want:  ready("ApiGatewayV2StageExists"),
			create: func(t *testing.T, w *evalWorld) string {
				t.Helper()

				api, err := w.apigwv2.CreateAPI(
					context.Background(),
					apigwv2.CreateAPIInput{Name: "a", ProtocolType: "HTTP"},
				)
				require.NoError(t, err)
				_, err = w.apigwv2.CreateStage(api.APIID, apigwv2.CreateStageInput{StageName: "prod"})
				require.NoError(t, err)

				return "arn:aws:apigateway:us-east-1::/apis/" + api.APIID + "/stages/prod"
			},
		},
		{
			name: "auto scaling group", typ: "AWS::AutoScaling::AutoScalingGroup",
			bogus: evalARN("autoscaling", "autoScalingGroup:x:autoScalingGroupName/nope"),
			want:  ready("AutoScalingGroupExists", "AutoScalingGroupCapacityMet"),
			create: func(t *testing.T, w *evalWorld) string {
				t.Helper()

				g, err := w.asg.CreateAutoScalingGroup(asg.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "web", MinSize: 0, MaxSize: 2, AvailabilityZones: []string{"us-east-1a"},
				})
				require.NoError(t, err)

				return g.AutoScalingGroupARN
			},
		},
		{
			name: "ec2 customer gateway", typ: "AWS::EC2::CustomerGateway",
			bogus: evalARN("ec2", "customer-gateway/cgw-nope"),
			want:  ready("Ec2CustomerGatewayExists", "Ec2CustomerGatewayAvailable"),
			create: func(t *testing.T, w *evalWorld) string {
				t.Helper()

				g, err := w.ec2.CreateCustomerGateway("ipsec.1", "203.0.113.5", "65000")
				require.NoError(t, err)

				return evalARN("ec2", "customer-gateway/"+g.CustomerGatewayID)
			},
		},
		{
			name: "ec2 volume", typ: "AWS::EC2::Volume",
			bogus: evalARN("ec2", "volume/vol-nope"),
			want:  ready("Ec2VolumeExists", "Ec2VolumeAvailable"),
			create: func(t *testing.T, w *evalWorld) string {
				t.Helper()

				v, err := w.ec2.CreateVolume("us-east-1a", "gp3", 8, "")
				require.NoError(t, err)

				return evalARN("ec2", "volume/"+v.ID)
			},
		},
		{
			name: "ec2 vpn gateway", typ: "AWS::EC2::VPNGateway",
			bogus: evalARN("ec2", "vpn-gateway/vgw-nope"),
			want:  ready("Ec2VpnGatewayExists", "Ec2VpnGatewayAvailable"),
			create: func(t *testing.T, w *evalWorld) string {
				t.Helper()

				g, err := w.ec2.CreateVpnGateway("ipsec.1", 0)
				require.NoError(t, err)

				return evalARN("ec2", "vpn-gateway/"+g.VpnGatewayID)
			},
		},
		{
			name: "ec2 vpn connection", typ: "AWS::EC2::VPNConnection",
			bogus: evalARN("ec2", "vpn-connection/vpn-nope"),
			want:  ready("Ec2VpnConnectionExists", "Ec2VpnConnectionAvailable"),
			create: func(t *testing.T, w *evalWorld) string {
				t.Helper()

				cgw, err := w.ec2.CreateCustomerGateway("ipsec.1", "203.0.113.6", "65000")
				require.NoError(t, err)
				vgw, err := w.ec2.CreateVpnGateway("ipsec.1", 0)
				require.NoError(t, err)
				c, err := w.ec2.CreateVpnConnection("ipsec.1", cgw.CustomerGatewayID, vgw.VpnGatewayID)
				require.NoError(t, err)

				return evalARN("ec2", "vpn-connection/"+c.VpnConnectionID)
			},
		},
		{
			name: "classic load balancer", typ: "AWS::ElasticLoadBalancing::LoadBalancer",
			bogus: evalARN("elasticloadbalancing", "loadbalancer/nope"),
			want:  ready("ElbLoadBalancerExists"),
			create: func(t *testing.T, w *evalWorld) string {
				t.Helper()

				_, err := w.elb.CreateLoadBalancer(context.Background(), elbbk.CreateLoadBalancerInput{
					LoadBalancerName: "classic", AvailabilityZones: []string{"us-east-1a"},
					Listeners: []elbbk.Listener{
						{Protocol: "HTTP", InstanceProtocol: "HTTP", LoadBalancerPort: 80, InstancePort: 8080},
					},
				})
				require.NoError(t, err)

				return evalARN("elasticloadbalancing", "loadbalancer/classic")
			},
		},
		{
			name: "msk cluster", typ: "AWS::MSK::Cluster",
			bogus: evalARN("kafka", "cluster/nope/1"),
			want:  ready("MskClusterExists", "MskClusterActive"),
			create: func(t *testing.T, w *evalWorld) string {
				t.Helper()

				c, err := w.kafka.CreateCluster(
					context.Background(), "msk", "3.9.1", 1, kafkabk.BrokerNodeGroupInfo{}, nil, nil)
				require.NoError(t, err)

				return c.ClusterArn
			},
		},
		{
			name: "rds db cluster", typ: "AWS::RDS::DBCluster",
			bogus: evalARN("rds", "cluster:nope"),
			want:  ready("RdsDbClusterExists", "RdsDbClusterAvailable"),
			create: func(t *testing.T, w *evalWorld) string {
				t.Helper()

				_, err := w.rds.CreateDBCluster(
					"aurora1", "aurora-postgresql", "admin", "", "", 0, nil, rdsbk.DBClusterOptions{})
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					cs, descErr := w.rds.DescribeDBClusters("aurora1")

					return descErr == nil && len(cs) == 1 && cs[0].Status == "available"
				}, 10*time.Second, 20*time.Millisecond)

				return evalARN("rds", "cluster:aurora1")
			},
		},
		{
			name: "sns subscription confirmed", typ: "AWS::SNS::Subscription",
			bogus: evalARN("sns", "t:00000000-0000-0000-0000-000000000000"),
			want:  ready("SnsSubscriptionExists", "SnsSubscriptionConfirmed"),
			create: func(t *testing.T, w *evalWorld) string {
				t.Helper()

				topic, err := w.sns.CreateTopic("t", nil)
				require.NoError(t, err)
				sub, err := w.sns.Subscribe(topic.TopicArn, "sqs", evalARN("sqs", "q"), "")
				require.NoError(t, err)

				return sub.SubscriptionArn
			},
		},
		{
			name: "sns subscription pending", typ: "AWS::SNS::Subscription",
			bogus: evalARN("sns", "t:00000000-0000-0000-0000-000000000000"),
			want: map[string]types.Readiness{
				"SnsSubscriptionExists": types.ReadinessReady, "SnsSubscriptionConfirmed": types.ReadinessNotReady,
			},
			create: func(t *testing.T, w *evalWorld) string {
				t.Helper()

				topic, err := w.sns.CreateTopic("t", nil)
				require.NoError(t, err)
				sub, err := w.sns.Subscribe(topic.TopicArn, "https", "https://example.com/hook", "")
				require.NoError(t, err)

				return sub.SubscriptionArn
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f, w := newEvalFixture(t)
			existing := tt.create(t, w)
			rc := f.checkOf(t, tt.typ,
				types.Resource{ResourceArn: aws.String(existing)}, types.Resource{ResourceArn: aws.String(tt.bogus)})

			assert.Equal(t, tt.want, f.rules(t, rc, existing))

			missing := f.rules(t, rc, tt.bogus)
			require.NotEmpty(t, missing)
			assert.Equal(t, types.ReadinessNotReady, missing[firstRule(tt.want)])

			status, err := f.client.GetReadinessCheckStatus(
				t.Context(), &sdk.GetReadinessCheckStatusInput{ReadinessCheckName: aws.String(rc)})
			require.NoError(t, err)
			assert.Equal(t, types.ReadinessNotReady, status.Readiness)
		})
	}
}

func firstRule(m map[string]types.Readiness) string {
	for id := range m {
		if strings.HasSuffix(id, "Exists") {
			return id
		}
	}

	return ""
}

func TestEveryResourceSetTypeHasRules(t *testing.T) {
	t.Parallel()

	typesInSDK := []string{
		"AWS::ApiGateway::Stage", "AWS::ApiGatewayV2::Stage", "AWS::AutoScaling::AutoScalingGroup",
		"AWS::CloudWatch::Alarm", "AWS::EC2::CustomerGateway", "AWS::DynamoDB::Table", "AWS::EC2::Volume",
		"AWS::ElasticLoadBalancing::LoadBalancer", "AWS::ElasticLoadBalancingV2::LoadBalancer",
		"AWS::Lambda::Function", "AWS::MSK::Cluster", "AWS::RDS::DBCluster", "AWS::Route53::HealthCheck",
		"AWS::SQS::Queue", "AWS::SNS::Topic", "AWS::SNS::Subscription", "AWS::EC2::VPC",
		"AWS::EC2::VPNConnection", "AWS::EC2::VPNGateway", "AWS::Route53RecoveryReadiness::DNSTargetResource",
	}

	f := newFixture(t)

	for _, typ := range typesInSDK {
		t.Run(typ, func(t *testing.T) {
			t.Parallel()

			out, err := f.client.ListRules(t.Context(), &sdk.ListRulesInput{ResourceType: aws.String(typ)})
			require.NoError(t, err)
			assert.NotEmpty(t, out.Rules, "no readiness rule evaluates %s", typ)
		})
	}
}
