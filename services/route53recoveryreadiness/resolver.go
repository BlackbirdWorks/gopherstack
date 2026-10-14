package route53recoveryreadiness

import (
	"context"
	"strings"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	r53backend "github.com/blackbirdworks/gopherstack/services/route53"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

// siblingServices is matched structurally against *CLI, which cannot be imported here.
type siblingServices interface {
	GetCloudWatchHandler() service.Registerable
	GetDynamoDBHandler() service.Registerable
	GetEC2Handler() service.Registerable
	GetELBv2Handler() service.Registerable
	GetLambdaHandler() service.Registerable
	GetRoute53Handler() service.Registerable
	GetSNSHandler() service.Registerable
	GetSQSHandler() service.Registerable
}

// probe is the outcome of one sibling lookup: whether the sibling could answer, and what it found.
type probe struct {
	detail   string
	answered bool
	exists   bool
	healthy  bool
}

type resolver struct{ cfg any }

func (r resolver) siblings() (siblingServices, bool) {
	s, ok := r.cfg.(siblingServices)

	return s, ok
}

func parseARN(a string) (awsarn.ARN, bool) {
	p, err := awsarn.Parse(a)

	return p, err == nil
}

// lookup dispatches on the resource type; unsupported types answer with answered=false.
func (r resolver) lookup(ctx context.Context, typ, resourceArn string) probe {
	s, ok := r.siblings()
	if !ok {
		return probe{}
	}

	p, parsed := parseARN(resourceArn)
	if !parsed {
		return probe{answered: true, detail: "the resource ARN is not valid"}
	}

	switch typ {
	case typeELBv2:
		return r.lookupELBv2(s, p, resourceArn)
	case typeHealthCheck:
		return r.lookupHealthCheck(s, p)
	case typeSQS:
		return r.lookupSQS(s, resourceArn)
	case typeSNS:
		return r.lookupSNS(s, resourceArn)
	case typeLambda:
		return r.lookupLambda(s, p)
	case typeVPC:
		return r.lookupVPC(s, p)
	case typeAlarm:
		return r.lookupAlarm(s, p)
	case typeTable:
		return r.lookupTable(ctx, s, p)
	}

	return probe{}
}

func (resolver) lookupELBv2(s siblingServices, p awsarn.ARN, full string) probe {
	h, ok := s.GetELBv2Handler().(*elbv2backend.Handler)
	if !ok || h == nil {
		return probe{}
	}

	lbs, err := h.BackendFor(p.Region).DescribeLoadBalancers([]string{full}, nil)
	if err != nil || len(lbs) == 0 {
		return probe{answered: true, detail: "the load balancer does not exist"}
	}

	return probe{
		answered: true,
		exists:   true,
		healthy:  lbs[0].State.Code == "active",
		detail:   "load balancer state is " + lbs[0].State.Code,
	}
}

func (resolver) lookupHealthCheck(s siblingServices, p awsarn.ARN) probe {
	h, ok := s.GetRoute53Handler().(*r53backend.Handler)
	if !ok || h == nil {
		return probe{}
	}

	id := strings.TrimPrefix(p.Resource, "healthcheck/")

	status, err := h.Backend.GetHealthCheckStatus(id)
	if err != nil {
		return probe{answered: true, detail: "the health check does not exist"}
	}

	return probe{answered: true, exists: true, healthy: status == "Healthy", detail: "health check status is " + status}
}

func (resolver) lookupSQS(s siblingServices, full string) probe {
	h, ok := s.GetSQSHandler().(*sqsbackend.Handler)
	if !ok || h == nil {
		return probe{}
	}

	q, ok := h.Backend.(interface{ QueueExists(string) bool })
	if !ok {
		return probe{}
	}

	if !q.QueueExists(full) {
		return probe{answered: true, detail: "the queue does not exist"}
	}

	return probe{answered: true, exists: true, healthy: true}
}

func (resolver) lookupSNS(s siblingServices, full string) probe {
	h, ok := s.GetSNSHandler().(*snsbackend.Handler)
	if !ok || h == nil {
		return probe{}
	}

	if _, err := h.Backend.GetTopicAttributes(full); err != nil {
		return probe{answered: true, detail: "the topic does not exist"}
	}

	return probe{answered: true, exists: true, healthy: true}
}

func (resolver) lookupLambda(s siblingServices, p awsarn.ARN) probe {
	h, ok := s.GetLambdaHandler().(*lambdabackend.Handler)
	if !ok || h == nil {
		return probe{}
	}

	name := strings.TrimPrefix(p.Resource, "function:")
	if i := strings.Index(name, ":"); i >= 0 {
		name = name[:i]
	}

	if _, err := h.BackendFor(p.Region).GetFunction(name); err != nil {
		return probe{answered: true, detail: "the function does not exist"}
	}

	return probe{answered: true, exists: true, healthy: true}
}

func (resolver) lookupVPC(s siblingServices, p awsarn.ARN) probe {
	h, ok := s.GetEC2Handler().(*ec2backend.Handler)
	if !ok || h == nil {
		return probe{}
	}

	id := strings.TrimPrefix(p.Resource, "vpc/")
	for _, v := range h.BackendFor(p.Region).DescribeVpcs([]string{id}) {
		if v.ID == id {
			return probe{answered: true, exists: true, healthy: true}
		}
	}

	return probe{answered: true, detail: "the VPC does not exist"}
}

func (resolver) lookupAlarm(s siblingServices, p awsarn.ARN) probe {
	h, ok := s.GetCloudWatchHandler().(*cwbackend.Handler)
	if !ok || h == nil {
		return probe{}
	}

	name := strings.TrimPrefix(p.Resource, "alarm:")

	metric, _, _, err := h.BackendFor(p.Region).DescribeAlarms([]string{name}, nil, "", "", "", 0, "", "", "")
	if err != nil || len(metric.Data) == 0 {
		return probe{answered: true, detail: "the alarm does not exist"}
	}

	state := metric.Data[0].StateValue

	return probe{answered: true, exists: true, healthy: state != "ALARM", detail: "alarm state is " + state}
}

func (resolver) lookupTable(ctx context.Context, s siblingServices, p awsarn.ARN) probe {
	h, ok := s.GetDynamoDBHandler().(*ddbbackend.DynamoDBHandler)
	if !ok || h == nil {
		return probe{}
	}

	name := strings.TrimPrefix(p.Resource, "table/")
	if i := strings.Index(name, "/"); i >= 0 {
		name = name[:i]
	}

	out, err := h.Backend.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &name})
	if err != nil || out == nil || out.Table == nil {
		return probe{answered: true, detail: "the table does not exist"}
	}

	status := string(out.Table.TableStatus)

	return probe{answered: true, exists: true, healthy: status == "ACTIVE", detail: "table status is " + status}
}

func (b *InMemoryBackend) lookupDNS(d *DNSTarget) probe {
	s, ok := resolver{cfg: b.appConfig}.siblings()
	if !ok {
		return probe{}
	}

	h, ok := s.GetRoute53Handler().(*r53backend.Handler)
	if !ok || h == nil {
		return probe{}
	}

	zone := d.HostedZoneArn
	if i := strings.LastIndex(zone, "/"); i >= 0 {
		zone = zone[i+1:]
	}

	if zone == "" {
		return probe{answered: true, detail: "the DNS target has no hosted zone"}
	}

	page, err := h.Backend.ListResourceRecordSets(zone, "", "", "", dnsRecordScan)
	if err != nil {
		return probe{answered: true, detail: "the hosted zone does not exist"}
	}

	want := strings.TrimSuffix(strings.ToLower(d.DomainName), ".")

	for _, rr := range page.Records {
		if strings.TrimSuffix(strings.ToLower(rr.Name), ".") != want {
			continue
		}

		if (d.RecordType == "" || rr.Type == d.RecordType) &&
			(d.RecordSetID == "" || rr.SetIdentifier == d.RecordSetID) {
			return probe{answered: true, exists: true, healthy: true}
		}
	}

	return probe{answered: true, detail: "no matching record set exists in the hosted zone"}
}

const dnsRecordScan = 1000
