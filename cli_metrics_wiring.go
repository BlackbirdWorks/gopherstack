package main

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	apigwv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const (
	serviceMetricQueueSize = 8192
	serviceMetricWindow    = time.Second
)

// statisticSetEmitter publishes folded points as CloudWatch statistic sets and passes single points through.
func statisticSetEmitter(cwH *cwbackend.Handler) cwmetric.EmitterFunc {
	single := regionalMetricEmitter(cwH)

	return func(p cwmetric.Point) error {
		if p.Samples <= 0 {
			return single(p)
		}

		dims := make([]cwbackend.Dimension, len(p.Dimensions))
		for i, d := range p.Dimensions {
			dims[i] = cwbackend.Dimension{Name: d.Name, Value: d.Value}
		}

		return cwH.BackendFor(p.Region).PutMetricData(p.Namespace, []cwbackend.MetricDatum{{
			MetricName: p.Name, Namespace: p.Namespace, Unit: p.Unit, Dimensions: dims,
			Count: p.Samples, Sum: p.Sum, Min: p.Min, Max: p.Max,
			HasStatisticSet: true, Timestamp: time.Now(),
		}})
	}
}

// wireServiceMetrics publishes in-process AWS metrics for Kinesis, DynamoDB, API Gateway (REST, HTTP,
// WebSocket), SNS, EventBridge, Step Functions, S3 and EC2 through one bounded async emitter.
func wireServiceMetrics(ctx context.Context, byName map[string]service.Registerable) {
	cwH, ok := byName["CloudWatch"].(*cwbackend.Handler)
	if !ok {
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	e := cwmetric.NewAsync(ctx, statisticSetEmitter(cwH), serviceMetricQueueSize, serviceMetricWindow)

	if h, hok := byName["Kinesis"].(*kinesisbackend.Handler); hok {
		if bk, bok := h.Backend.(*kinesisbackend.InMemoryBackend); bok {
			bk.SetMetricEmitter(e)
		}
	}

	if h, hok := byName["DynamoDB"].(*ddbbackend.DynamoDBHandler); hok {
		if bk, bok := h.Backend.(*ddbbackend.InMemoryDB); bok {
			bk.SetMetricEmitter(e)
		}
	}

	if h, hok := byName["APIGateway"].(*apigwbackend.Handler); hok {
		h.SetMetricEmitter(e)
	}

	if h, hok := byName["APIGatewayV2"].(*apigwv2backend.Handler); hok {
		h.SetMetricEmitter(e)
	}

	wireMessagingMetrics(e, byName)
	wireStorageAndComputeMetrics(e, byName)
}

func wireMessagingMetrics(e cwmetric.Emitter, byName map[string]service.Registerable) {
	if h, ok := byName["SNS"].(*snsbackend.Handler); ok {
		if bk, bok := h.Backend.(*snsbackend.InMemoryBackend); bok {
			bk.SetMetricEmitter(e)
		}
	}

	if h, ok := byName["EventBridge"].(*ebbackend.Handler); ok {
		if bk, bok := h.Backend.(*ebbackend.InMemoryBackend); bok {
			bk.SetMetricEmitter(e)
		}
	}

	if h, ok := byName["StepFunctions"].(*sfnbackend.Handler); ok {
		if bk, bok := h.Backend.(*sfnbackend.InMemoryBackend); bok {
			bk.SetMetricEmitter(e)
		}
	}
}

func wireStorageAndComputeMetrics(e cwmetric.Emitter, byName map[string]service.Registerable) {
	if h, ok := byName["S3"].(*s3backend.S3Handler); ok {
		h.SetMetricEmitter(e)

		if bk, bok := h.Backend.(*s3backend.InMemoryBackend); bok {
			bk.SetMetricEmitter(e)
		}
	}

	if h, ok := byName["EC2"].(*ec2backend.Handler); ok {
		for _, bk := range h.RegionBackends() {
			if mem, mok := bk.(*ec2backend.InMemoryBackend); mok {
				mem.SetMetricEmitter(e)
			}
		}
	}
}
