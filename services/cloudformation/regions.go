package cloudformation

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	athenabackend "github.com/blackbirdworks/gopherstack/services/athena"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	codedeploybackend "github.com/blackbirdworks/gopherstack/services/codedeploy"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	servicediscoverybackend "github.com/blackbirdworks/gopherstack/services/servicediscovery"
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
