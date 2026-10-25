package applicationautoscaling

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/service"

	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// siblingServices is matched structurally against *CLI to avoid an import
// cycle; see services/grafana/cross_service.go for the reference pattern.
type siblingServices interface {
	GetDynamoDBHandler() service.Registerable
}

// SetAppConfig records ctx.Config so the DynamoDB backend can be resolved lazily.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.appConfig = cfg
}

// dynamoDBBackend returns the DynamoDB backend, if wired.
func (b *InMemoryBackend) dynamoDBBackend() (ddbbackend.StorageBackend, bool) {
	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetDynamoDBHandler().(*ddbbackend.DynamoDBHandler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	return h.Backend, true
}

type cloudWatchSibling interface {
	GetCloudWatchHandler() service.Registerable
}

func (b *InMemoryBackend) cloudWatchBackend() (cwbackend.StorageBackend, bool) {
	s, ok := b.appConfig.(cloudWatchSibling)
	if !ok {
		return nil, false
	}

	h, ok := s.GetCloudWatchHandler().(*cwbackend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.BackendFor(b.region), true
}

// attachAlarms fills p.Alarms for step-scaling policies with the CloudWatch
// alarms whose AlarmActions name the policy ARN. Must not hold b.mu.
func (b *InMemoryBackend) attachAlarms(p *ScalingPolicy) {
	if p.PolicyType != policyTypeStepScaling {
		return
	}

	cw, ok := b.cloudWatchBackend()
	if !ok {
		return
	}

	var out []Alarm

	token := ""

	for {
		metric, _, _, err := cw.DescribeAlarms(nil, nil, "", "", token, 0, p.ARN, "", "")
		if err != nil {
			return
		}

		for _, a := range metric.Data {
			if slices.Contains(a.AlarmActions, p.ARN) {
				out = append(out, Alarm{AlarmARN: a.AlarmArn, AlarmName: a.AlarmName})
			}
		}

		if metric.Next == "" {
			break
		}

		token = metric.Next
	}

	p.Alarms = out
}
