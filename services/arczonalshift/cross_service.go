package arczonalshift

import (
	"strconv"
	"strings"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
)

// siblingServices is matched structurally against *CLI, which cannot be imported here.
type siblingServices interface {
	GetELBv2Handler() service.Registerable
	GetCloudWatchHandler() service.Registerable
}

type managedResource struct {
	ARN       string
	Name      string
	ZoneNames []string
	ZoneIDs   []string
}

func (b *InMemoryBackend) elbv2() (*elbv2backend.Handler, bool) {
	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetELBv2Handler().(*elbv2backend.Handler)

	return h, ok && h != nil
}

func toManaged(lb *elbv2backend.LoadBalancer) managedResource {
	m := managedResource{ARN: lb.LoadBalancerArn, Name: lb.LoadBalancerName}

	for _, az := range lb.AvailabilityZones {
		m.ZoneNames = append(m.ZoneNames, az.ZoneName)
		m.ZoneIDs = append(m.ZoneIDs, zoneID(az.ZoneName))
	}

	return m
}

// supportedLB reports whether arn names an Application or Network Load Balancer, the ELBv2 types zonal shift supports.
func supportedLB(arn string) (awsarn.ARN, bool) {
	p, err := awsarn.Parse(arn)
	if err != nil || p.Service != "elasticloadbalancing" {
		return p, false
	}

	return p, strings.HasPrefix(p.Resource, "loadbalancer/app/") || strings.HasPrefix(p.Resource, "loadbalancer/net/")
}

// managedFor resolves a resource identifier to a live managed resource.
func (b *InMemoryBackend) managedFor(arn string) (managedResource, error) {
	p, ok := supportedLB(arn)
	if !ok {
		return managedResource{}, invalidReason(
			"InvalidResourceIdentifier",
			"Resource identifier %q is not a supported resource",
			arn,
		)
	}

	h, ok := b.elbv2()
	if !ok {
		return managedResource{}, restroute.NotFound("Managed resource %s was not found", arn)
	}

	lbs, err := h.BackendFor(p.Region).DescribeLoadBalancers([]string{arn}, nil)
	if err != nil || len(lbs) == 0 {
		return managedResource{}, restroute.NotFound("Managed resource %s was not found", arn)
	}

	return toManaged(&lbs[0]), nil
}

// listManaged returns the load balancers of region.
func (b *InMemoryBackend) listManaged(region string) []managedResource {
	h, ok := b.elbv2()
	if !ok {
		return nil
	}

	lbs, err := h.BackendFor(region).DescribeLoadBalancers(nil, nil)
	if err != nil {
		return nil
	}

	out := make([]managedResource, 0, len(lbs))

	for i := range lbs {
		if _, supported := supportedLB(lbs[i].LoadBalancerArn); supported {
			out = append(out, toManaged(&lbs[i]))
		}
	}

	return out
}

// alarmState returns the CloudWatch alarm state for an alarm ARN and whether the alarm exists.
func (b *InMemoryBackend) alarmState(alarmArn string) (string, bool) {
	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return "", false
	}

	h, ok := s.GetCloudWatchHandler().(*cwbackend.Handler)
	if !ok || h == nil {
		return "", false
	}

	p, err := awsarn.Parse(alarmArn)
	if err != nil || p.Service != "cloudwatch" || !strings.HasPrefix(p.Resource, "alarm:") {
		return "", false
	}

	metric, _, _, err := h.BackendFor(p.Region).DescribeAlarms(
		[]string{strings.TrimPrefix(p.Resource, "alarm:")}, nil, "", "", "", 0, "", "", "")
	if err != nil || len(metric.Data) == 0 {
		return "", false
	}

	return metric.Data[0].StateValue, true
}

// zoneID maps a zone name like "us-east-1a" to its zone ID like "use1-az1"; other inputs are returned unchanged.
func zoneID(name string) string {
	if len(name) < len("x-y-1a") {
		return name
	}

	letter := name[len(name)-1]
	region := name[:len(name)-1]
	parts := strings.Split(region, "-")

	if letter < 'a' || letter > 'z' || len(parts) < 3 || region[len(region)-1] < '0' || region[len(region)-1] > '9' {
		return name
	}

	var sb strings.Builder

	sb.WriteString(parts[0])

	for _, p := range parts[1 : len(parts)-1] {
		switch p {
		case "northeast":
			sb.WriteString("ne")
		case "northwest":
			sb.WriteString("nw")
		case "southeast":
			sb.WriteString("se")
		case "southwest":
			sb.WriteString("sw")
		default:
			sb.WriteString(p[:1])
		}
	}

	sb.WriteString(parts[len(parts)-1])
	sb.WriteString("-az")
	sb.WriteString(strconv.Itoa(int(letter-'a') + 1))

	return sb.String()
}
