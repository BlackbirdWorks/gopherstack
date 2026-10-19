package replicator

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func (r *run) copyEventSourceMappings(ctx context.Context, name string) {
	pager := lambda.NewListEventSourceMappingsPaginator(
		r.src.lambda, &lambda.ListEventSourceMappingsInput{FunctionName: &name},
	)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.fail(TypeLambda, name+":event-source-mappings", fmt.Errorf("list mappings: %w", err))

			return
		}

		for i := range page.EventSourceMappings {
			r.copyESM(ctx, name, &page.EventSourceMappings[i])
		}
	}
}

// functionRef returns name or name:qualifier from a function ARN.
func functionRef(fnARN, fallback string) string {
	p, err := awsarn.Parse(fnARN)
	if err != nil {
		return fallback
	}

	if ref, ok := strings.CutPrefix(p.Resource, "function:"); ok {
		return ref
	}

	return fallback
}

func (r *run) copyESM(ctx context.Context, name string, m *lambdatypes.EventSourceMappingConfiguration) {
	srcARN := aws.ToString(m.EventSourceArn)
	id := "esm/" + name + "/" + srcARN

	p, err := awsarn.Parse(srcARN)
	if err != nil {
		r.fail(TypeLambda, id, err)

		return
	}

	dstSource, skip, err := r.targetEventSource(ctx, p)

	switch {
	case err != nil:
		r.fail(TypeLambda, id, err)

		return
	case skip != "":
		r.add(TypeLambda, id, ActionSkipped, skip)

		return
	}

	fn := functionRef(aws.ToString(m.FunctionArn), name)

	existing, err := r.dst.lambda.ListEventSourceMappings(ctx, &lambda.ListEventSourceMappingsInput{
		FunctionName: &fn, EventSourceArn: &dstSource,
	})
	if err != nil && !isNotFound(err) {
		r.fail(TypeLambda, id, err)

		return
	}

	exists := err == nil && len(existing.EventSourceMappings) > 0

	if r.cfg.DryRun {
		r.add(TypeLambda, id, upsert(exists), "")

		return
	}

	if exists {
		_, err = r.dst.lambda.UpdateEventSourceMapping(ctx, r.esmUpdate(m, existing.EventSourceMappings[0].UUID))
	} else {
		_, err = r.dst.lambda.CreateEventSourceMapping(ctx, r.esmCreate(m, fn, dstSource))
	}

	r.done(TypeLambda, id, upsert(exists), err)
}

// targetEventSource resolves the target ARN of a source SQS queue, Kinesis stream or DynamoDB
// stream. A non-empty skip reason means the source cannot be mapped.
func (r *run) targetEventSource(ctx context.Context, p awsarn.ARN) (string, string, error) {
	var (
		dst string
		err error
	)

	switch p.Service {
	case "sqs":
		_, err = r.dst.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &p.Resource})
	case "kinesis":
		dst = r.rm.ARN(p.String())
		_, err = r.dst.kin.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamARN: &dst})
	case "dynamodb":
		return r.targetDDBStream(ctx, p)
	default:
		return "", "event source type " + p.Service + " is not supported", nil
	}

	if isNotFound(err) {
		return "", "event source " + p.Resource + " not found in target (replicate it first)", nil
	}

	if err != nil {
		return "", "", err
	}

	if dst == "" {
		dst = r.rm.ARN(p.String())
	}

	return dst, "", nil
}

func (r *run) targetDDBStream(ctx context.Context, p awsarn.ARN) (string, string, error) {
	table, _, _ := strings.Cut(strings.TrimPrefix(p.Resource, "table/"), "/")

	out, err := r.dst.ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &table})

	switch {
	case isNotFound(err):
		return "", "event source table " + table + " not found in target (replicate it first)", nil
	case err != nil:
		return "", "", err
	case out.Table.LatestStreamArn == nil:
		return "", "target table " + table + " has no stream", nil
	}

	return aws.ToString(out.Table.LatestStreamArn), "", nil
}

func esmEnabled(m *lambdatypes.EventSourceMappingConfiguration) bool {
	s := strings.ToLower(aws.ToString(m.State))

	return s != "disabled" && s != "disabling"
}

func (r *run) esmDestination(d *lambdatypes.DestinationConfig) *lambdatypes.DestinationConfig {
	if d == nil || d.OnFailure == nil {
		return nil
	}

	return &lambdatypes.DestinationConfig{
		OnFailure: &lambdatypes.OnFailure{Destination: aws.String(r.rm.ARN(aws.ToString(d.OnFailure.Destination)))},
	}
}

func (r *run) esmCreate(
	m *lambdatypes.EventSourceMappingConfiguration, fn, source string,
) *lambda.CreateEventSourceMappingInput {
	return &lambda.CreateEventSourceMappingInput{
		FunctionName: &fn, EventSourceArn: &source, Enabled: aws.Bool(esmEnabled(m)),
		BatchSize: m.BatchSize, MaximumBatchingWindowInSeconds: m.MaximumBatchingWindowInSeconds,
		StartingPosition: m.StartingPosition, StartingPositionTimestamp: m.StartingPositionTimestamp,
		FilterCriteria: m.FilterCriteria, FunctionResponseTypes: m.FunctionResponseTypes,
		MaximumRecordAgeInSeconds: m.MaximumRecordAgeInSeconds, MaximumRetryAttempts: m.MaximumRetryAttempts,
		BisectBatchOnFunctionError: m.BisectBatchOnFunctionError, ParallelizationFactor: m.ParallelizationFactor,
		TumblingWindowInSeconds: m.TumblingWindowInSeconds, ScalingConfig: m.ScalingConfig,
		DestinationConfig: r.esmDestination(m.DestinationConfig),
	}
}

func (r *run) esmUpdate(
	m *lambdatypes.EventSourceMappingConfiguration,
	uuid *string,
) *lambda.UpdateEventSourceMappingInput {
	return &lambda.UpdateEventSourceMappingInput{
		UUID: uuid, Enabled: aws.Bool(esmEnabled(m)),
		BatchSize: m.BatchSize, MaximumBatchingWindowInSeconds: m.MaximumBatchingWindowInSeconds,
		FilterCriteria: m.FilterCriteria, FunctionResponseTypes: m.FunctionResponseTypes,
		MaximumRecordAgeInSeconds: m.MaximumRecordAgeInSeconds, MaximumRetryAttempts: m.MaximumRetryAttempts,
		BisectBatchOnFunctionError: m.BisectBatchOnFunctionError, ParallelizationFactor: m.ParallelizationFactor,
		TumblingWindowInSeconds: m.TumblingWindowInSeconds, ScalingConfig: m.ScalingConfig,
		DestinationConfig: r.esmDestination(m.DestinationConfig),
	}
}
