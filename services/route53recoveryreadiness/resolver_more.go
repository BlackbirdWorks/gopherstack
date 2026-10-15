package route53recoveryreadiness

import (
	"context"
	"strings"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	apigwv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	asgbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
	kafkabackend "github.com/blackbirdworks/gopherstack/services/kafka"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
)

const (
	stateAvailable   = "available"
	stateInUse       = "in-use"
	stateActive      = "ACTIVE"
	lifecycleInSvc   = "InService"
	statusDeleting   = "Delete in progress"
	suffixAfterSlash = "/"
)

// sibling resolves a sibling handler of type *H from cfg at call time; get picks the accessor off cfg.
func sibling[P any, H any](cfg any, get func(P) service.Registerable) (*H, bool) {
	p, ok := cfg.(P)
	if !ok {
		return nil, false
	}

	h, ok := any(get(p)).(*H)

	return h, ok && h != nil
}

type (
	apigwSiblings   interface{ GetAPIGatewayHandler() service.Registerable }
	apigwV2Siblings interface{ GetAPIGatewayV2Handler() service.Registerable }
	asgSiblings     interface{ GetAutoscalingHandler() service.Registerable }
	ec2Siblings     interface{ GetEC2Handler() service.Registerable }
	elbSiblings     interface{ GetELBHandler() service.Registerable }
	kafkaSiblings   interface{ GetKafkaHandler() service.Registerable }
	rdsSiblings     interface{ GetRDSHandler() service.Registerable }
	snsSiblings     interface{ GetSNSHandler() service.Registerable }
)

func missing(what string) probe {
	return probe{answered: true, detail: "the " + what + " does not exist"}
}

func found(healthy bool, detail string) probe {
	return probe{answered: true, exists: true, healthy: healthy, detail: detail}
}

// lookupMore dispatches the resource types that need a sibling beyond the original set.
func (r resolver) lookupMore(ctx context.Context, typ string, p awsarn.ARN, full string) (probe, bool) {
	switch typ {
	case typeAPIGWStage:
		return r.lookupAPIGWStage(p), true
	case typeAPIGWV2Stage:
		return r.lookupAPIGWV2Stage(p), true
	case typeASG:
		return r.lookupASG(p), true
	case typeCGW:
		return r.lookupCustomerGateway(p), true
	case typeVolume:
		return r.lookupVolume(p), true
	case typeVPNGateway:
		return r.lookupVPNGateway(p), true
	case typeVPNConn:
		return r.lookupVPNConnection(p), true
	case typeELBClassic:
		return r.lookupClassicELB(ctx, p), true
	case typeMSK:
		return r.lookupMSK(ctx, full), true
	case typeRDSCluster:
		return r.lookupRDSCluster(p), true
	case typeSNSSub:
		return r.lookupSNSSubscription(full), true
	default:
		return probe{}, false
	}
}

func lastSegment(resource string) string {
	return resource[strings.LastIndex(resource, suffixAfterSlash)+1:]
}

func (r resolver) lookupAPIGWStage(p awsarn.ARN) probe {
	h, ok := sibling[apigwSiblings, apigwbackend.Handler](r.cfg, apigwSiblings.GetAPIGatewayHandler)
	if !ok {
		return probe{}
	}

	parts := strings.Split(strings.Trim(p.Resource, suffixAfterSlash), suffixAfterSlash)
	if len(parts) != 4 || parts[0] != "restapis" || parts[2] != "stages" {
		return probe{answered: true, detail: "the stage ARN is not valid"}
	}

	if _, err := h.BackendFor(p.Region).GetStage(parts[1], parts[3]); err != nil {
		return missing("stage")
	}

	return found(true, "")
}

func (r resolver) lookupAPIGWV2Stage(p awsarn.ARN) probe {
	h, ok := sibling[apigwV2Siblings, apigwv2backend.Handler](r.cfg, apigwV2Siblings.GetAPIGatewayV2Handler)
	if !ok {
		return probe{}
	}

	parts := strings.Split(strings.Trim(p.Resource, suffixAfterSlash), suffixAfterSlash)
	if len(parts) != 4 || parts[0] != "apis" || parts[2] != "stages" {
		return probe{answered: true, detail: "the stage ARN is not valid"}
	}

	if _, err := h.BackendFor(p.Region).GetStage(parts[1], parts[3]); err != nil {
		return missing("stage")
	}

	return found(true, "")
}

func (r resolver) lookupASG(p awsarn.ARN) probe {
	h, ok := sibling[asgSiblings, asgbackend.Handler](r.cfg, asgSiblings.GetAutoscalingHandler)
	if !ok {
		return probe{}
	}

	_, name, _ := strings.Cut(p.Resource, "autoScalingGroupName/")

	groups, err := h.BackendFor(p.Region).DescribeAutoScalingGroups([]string{name}, nil)
	if err != nil || len(groups) == 0 {
		return missing("Auto Scaling group")
	}

	g := groups[0]
	inService := int32(0)

	for i := range g.Instances {
		if g.Instances[i].LifecycleState == lifecycleInSvc {
			inService++
		}
	}

	met := g.Status != statusDeleting && inService >= g.MinSize

	return found(met, "group status is "+g.Status)
}

func (r resolver) ec2(region string) (ec2backend.Backend, bool) {
	h, ok := sibling[ec2Siblings, ec2backend.Handler](r.cfg, ec2Siblings.GetEC2Handler)
	if !ok {
		return nil, false
	}

	return h.BackendFor(region), true
}

func (r resolver) lookupCustomerGateway(p awsarn.ARN) probe {
	b, ok := r.ec2(p.Region)
	if !ok {
		return probe{}
	}

	gws, err := b.DescribeCustomerGateways([]string{lastSegment(p.Resource)})
	if err != nil || len(gws) == 0 {
		return missing("customer gateway")
	}

	return found(gws[0].State == stateAvailable, "customer gateway state is "+gws[0].State)
}

func (r resolver) lookupVolume(p awsarn.ARN) probe {
	b, ok := r.ec2(p.Region)
	if !ok {
		return probe{}
	}

	vols := b.DescribeVolumes([]string{lastSegment(p.Resource)})
	if len(vols) == 0 {
		return missing("volume")
	}

	state := vols[0].State

	return found(state == stateAvailable || state == stateInUse, "volume state is "+state)
}

func (r resolver) lookupVPNGateway(p awsarn.ARN) probe {
	b, ok := r.ec2(p.Region)
	if !ok {
		return probe{}
	}

	id := lastSegment(p.Resource)

	for _, g := range b.DescribeVpnGateways([]string{id}) {
		if g.VpnGatewayID == id {
			return found(g.State == stateAvailable, "VPN gateway state is "+g.State)
		}
	}

	return missing("VPN gateway")
}

func (r resolver) lookupVPNConnection(p awsarn.ARN) probe {
	b, ok := r.ec2(p.Region)
	if !ok {
		return probe{}
	}

	id := lastSegment(p.Resource)

	for _, c := range b.DescribeVpnConnections([]string{id}) {
		if c.VpnConnectionID == id {
			return found(c.State == stateAvailable, "VPN connection state is "+c.State)
		}
	}

	return missing("VPN connection")
}

func (r resolver) lookupClassicELB(ctx context.Context, p awsarn.ARN) probe {
	h, ok := sibling[elbSiblings, elbbackend.Handler](r.cfg, elbSiblings.GetELBHandler)
	if !ok {
		return probe{}
	}

	name := lastSegment(p.Resource)

	lbs, err := h.Backend.DescribeLoadBalancers(awsmeta.WithRegion(ctx, p.Region), []string{name})
	if err != nil || len(lbs) == 0 {
		return missing("load balancer")
	}

	return found(true, "")
}

func (r resolver) lookupMSK(ctx context.Context, full string) probe {
	h, ok := sibling[kafkaSiblings, kafkabackend.Handler](r.cfg, kafkaSiblings.GetKafkaHandler)
	if !ok {
		return probe{}
	}

	c, err := h.Backend.DescribeCluster(ctx, full)
	if err != nil || c == nil {
		return missing("cluster")
	}

	return found(c.State == stateActive, "cluster state is "+c.State)
}

func (r resolver) lookupRDSCluster(p awsarn.ARN) probe {
	h, ok := sibling[rdsSiblings, rdsbackend.Handler](r.cfg, rdsSiblings.GetRDSHandler)
	if !ok {
		return probe{}
	}

	id := strings.TrimPrefix(p.Resource, "cluster:")

	cs, err := h.BackendFor(p.Region).DescribeDBClusters(id)
	if err != nil || len(cs) == 0 {
		return missing("DB cluster")
	}

	return found(cs[0].Status == stateAvailable, "DB cluster status is "+cs[0].Status)
}

func (r resolver) lookupSNSSubscription(full string) probe {
	h, ok := sibling[snsSiblings, snsbackend.Handler](r.cfg, snsSiblings.GetSNSHandler)
	if !ok {
		return probe{}
	}

	attrs, err := h.Backend.GetSubscriptionAttributes(full)
	if err != nil {
		return missing("subscription")
	}

	return found(
		attrs["PendingConfirmation"] != "true",
		"subscription pending confirmation is "+attrs["PendingConfirmation"],
	)
}
