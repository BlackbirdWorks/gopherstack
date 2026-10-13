package cloudformation

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
	accessanalyzerbackend "github.com/blackbirdworks/gopherstack/services/accessanalyzer"
	amplifybackend "github.com/blackbirdworks/gopherstack/services/amplify"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	appconfigbackend "github.com/blackbirdworks/gopherstack/services/appconfig"
	appautoscalingbackend "github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	athenabackend "github.com/blackbirdworks/gopherstack/services/athena"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	awsconfigbackend "github.com/blackbirdworks/gopherstack/services/awsconfig"
	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
	bedrockruntimebackend "github.com/blackbirdworks/gopherstack/services/bedrockruntime"
	cloudtrailbackend "github.com/blackbirdworks/gopherstack/services/cloudtrail"
	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	codedeploybackend "github.com/blackbirdworks/gopherstack/services/codedeploy"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	datasyncbackend "github.com/blackbirdworks/gopherstack/services/datasync"
	directconnectbackend "github.com/blackbirdworks/gopherstack/services/directconnect"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	guarddutybackend "github.com/blackbirdworks/gopherstack/services/guardduty"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	kafkaconnectbackend "github.com/blackbirdworks/gopherstack/services/kafkaconnect"
	kinesisvideobackend "github.com/blackbirdworks/gopherstack/services/kinesisvideo"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	macie2backend "github.com/blackbirdworks/gopherstack/services/macie2"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	servicediscoverybackend "github.com/blackbirdworks/gopherstack/services/servicediscovery"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	swfbackend "github.com/blackbirdworks/gopherstack/services/swf"
	transferbackend "github.com/blackbirdworks/gopherstack/services/transfer"
)

// EnableRegions makes h serve every other region through lazily built per-region
// siblings whose resource creator provisions into that region's service backends.
func (h *Handler) EnableRegions() {
	home, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return
	}

	h.peers = regionpeers.New(home.region, func(region string) *Handler {
		nb := NewInMemoryBackendWithConfig(home.accountID, region, home.creator.forRegion(region))
		nb.inheritWiring(home)

		return NewHandler(nb)
	})
}

// BackendFor returns the backend serving region: the home backend, or the sibling built on first use.
func (h *Handler) BackendFor(region string) StorageBackend {
	if p := h.peers.Get(region); p != nil {
		return p.Backend
	}

	return h.Backend
}

// RegionBackends returns the home backend followed by every sibling built so far.
func (h *Handler) RegionBackends() []StorageBackend {
	peers := h.peers.All()
	out := make([]StorageBackend, 0, 1+len(peers))
	out = append(out, h.Backend)

	for _, p := range peers {
		out = append(out, p.Backend)
	}

	return out
}

func (b *InMemoryBackend) inheritWiring(home *InMemoryBackend) {
	home.mu.RLock("inheritWiring")
	defer home.mu.RUnlock()

	b.orgDirectory = home.orgDirectory
}

// forRegion returns a creator whose backends are those serving region.
func (rc *ResourceCreator) forRegion(region string) *ResourceCreator {
	if rc == nil {
		return nil
	}

	out := &ResourceCreator{createHook: rc.createHook, deleteHook: rc.deleteHook}
	if rc.backends != nil {
		out.backends = rc.backends.forRegion(region)
	}

	return out
}

// regionHandler picks the sibling handler of h serving region, or nil when h is nil.
func regionHandler[T any](h *T, pick func(*T, string) *T, region string) *T {
	if h == nil {
		return nil
	}

	return pick(h, region)
}

// forRegion returns a copy that resolves region-sibling services to region and carries it as Region.
func (sb *ServiceBackends) forRegion(region string) *ServiceBackends {
	out := *sb
	out.Region = region

	out.EC2 = regionHandler(sb.EC2, (*ec2backend.Handler).RegionHandler, region)
	out.ECS = regionHandler(sb.ECS, (*ecsbackend.Handler).RegionHandler, region)
	out.ECR = regionHandler(sb.ECR, (*ecrbackend.Handler).RegionHandler, region)
	out.Glue = regionHandler(sb.Glue, (*gluebackend.Handler).RegionHandler, region)
	out.Athena = regionHandler(sb.Athena, (*athenabackend.Handler).RegionHandler, region)
	out.Backup = regionHandler(sb.Backup, (*backupbackend.Handler).RegionHandler, region)
	out.CognitoIDP = regionHandler(sb.CognitoIDP, (*cognitoidpbackend.Handler).RegionHandler, region)
	out.ServiceDiscovery = regionHandler(
		sb.ServiceDiscovery, (*servicediscoverybackend.Handler).RegionHandler, region)
	out.APIGateway = regionHandler(sb.APIGateway, (*apigwbackend.Handler).RegionHandler, region)
	out.APIGatewayV2 = regionHandler(sb.APIGatewayV2, (*apigatewayv2backend.Handler).RegionHandler, region)
	out.CloudWatch = regionHandler(sb.CloudWatch, (*cloudwatchbackend.Handler).RegionHandler, region)
	out.IoT = regionHandler(sb.IoT, (*iotbackend.Handler).RegionHandler, region)
	out.ELBv2 = regionHandler(sb.ELBv2, (*elbv2backend.Handler).RegionHandler, region)
	out.Autoscaling = regionHandler(sb.Autoscaling, (*autoscalingbackend.Handler).RegionHandler, region)
	out.CodeDeploy = regionHandler(sb.CodeDeploy, (*codedeploybackend.Handler).RegionHandler, region)
	out.Lambda = regionHandler(sb.Lambda, (*lambdabackend.Handler).RegionHandler, region)
	out.EKS = regionHandler(sb.EKS, (*eksbackend.Handler).RegionHandler, region)
	out.Redshift = regionHandler(sb.Redshift, (*redshiftbackend.Handler).RegionHandler, region)
	out.OpenSearch = regionHandler(sb.OpenSearch, (*opensearchbackend.Handler).RegionHandler, region)
	out.AppSync = regionHandler(sb.AppSync, (*appsyncbackend.Handler).RegionHandler, region)
	out.SES = regionHandler(sb.SES, (*sesbackend.Handler).RegionHandler, region)

	out.AppAutoScaling = regionHandler(sb.AppAutoScaling, (*appautoscalingbackend.Handler).RegionHandler, region)
	out.AWSConfig = regionHandler(sb.AWSConfig, (*awsconfigbackend.Handler).RegionHandler, region)
	out.CloudTrail = regionHandler(sb.CloudTrail, (*cloudtrailbackend.Handler).RegionHandler, region)
	out.CodeBuild = regionHandler(sb.CodeBuild, (*codebuildbackend.Handler).RegionHandler, region)
	out.GuardDuty = regionHandler(sb.GuardDuty, (*guarddutybackend.Handler).RegionHandler, region)
	out.Transfer = regionHandler(sb.Transfer, (*transferbackend.Handler).RegionHandler, region)

	out.AccessAnalyzer = regionHandler(sb.AccessAnalyzer, (*accessanalyzerbackend.Handler).RegionHandler, region)
	out.Amplify = regionHandler(sb.Amplify, (*amplifybackend.Handler).RegionHandler, region)
	out.AppConfig = regionHandler(sb.AppConfig, (*appconfigbackend.Handler).RegionHandler, region)
	out.BedrockRuntime = regionHandler(sb.BedrockRuntime, (*bedrockruntimebackend.Handler).RegionHandler, region)
	out.DataSync = regionHandler(sb.DataSync, (*datasyncbackend.Handler).RegionHandler, region)

	out.DirectConnect = regionHandler(sb.DirectConnect, (*directconnectbackend.Handler).RegionHandler, region)
	out.KafkaConnect = regionHandler(sb.KafkaConnect, (*kafkaconnectbackend.Handler).RegionHandler, region)
	out.KinesisVideo = regionHandler(sb.KinesisVideo, (*kinesisvideobackend.Handler).RegionHandler, region)
	out.Macie2 = regionHandler(sb.Macie2, (*macie2backend.Handler).RegionHandler, region)
	out.SWF = regionHandler(sb.SWF, (*swfbackend.Handler).RegionHandler, region)

	if rs, ok := sb.ResilienceHub.(interface {
		RegionResilienceHub(region string) ResilienceHubBackend
	}); ok {
		out.ResilienceHub = rs.RegionResilienceHub(region)
	}

	return &out
}

// withStackRegion carries the stack's region on ctx for backends that scope by request region.
func (rc *ResourceCreator) withStackRegion(ctx context.Context) context.Context {
	if rc.backends == nil || rc.backends.Region == "" {
		return ctx
	}

	m := *awsmeta.Get(ctx)
	m.Region = rc.backends.Region

	return awsmeta.Set(ctx, &m)
}
