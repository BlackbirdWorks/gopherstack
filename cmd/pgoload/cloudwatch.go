package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

const (
	cwNamespace          = "pgoload"
	cwMetricName         = "Ops"
	cwStatWindow         = 10 * time.Minute
	cwStatPeriodSeconds  = 60
	cwDatapointsPerBatch = 3
)

// cloudwatchWorker repeatedly runs a mix of CloudWatch operations, staggered
// by workerID, until ctx is done.
func cloudwatchWorker(ctx context.Context, cl *cloudwatch.Client, workerID int, c *opCounter, log *slog.Logger) {
	ops := []opFunc{
		func(ctx context.Context, workerID, i int) error { return cwPutMetricDataOp(ctx, cl, workerID, i) },
		func(ctx context.Context, workerID, i int) error { return cwPutMetricDataOp(ctx, cl, workerID, i) },
		func(ctx context.Context, workerID, i int) error { return cwListMetricsOp(ctx, cl) },
		func(ctx context.Context, workerID, i int) error { return cwGetMetricStatisticsOp(ctx, cl) },
		func(ctx context.Context, workerID, i int) error { return cwDescribeAlarmsOp(ctx, cl) },
	}

	runOpLoop(ctx, workerID, ops, c, "cloudwatch", log)
}

func cwPutMetricDataOp(ctx context.Context, cl *cloudwatch.Client, workerID, i int) error {
	data := make([]cwtypes.MetricDatum, 0, cwDatapointsPerBatch)
	for j := range cwDatapointsPerBatch {
		data = append(data, cwtypes.MetricDatum{
			MetricName: aws.String(cwMetricName),
			Value:      aws.Float64(float64(i + j)),
			Dimensions: []cwtypes.Dimension{
				{Name: aws.String("Worker"), Value: aws.String(fmt.Sprintf("%d", workerID))},
			},
		})
	}

	_, err := cl.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace:  aws.String(cwNamespace),
		MetricData: data,
	})

	return err
}

func cwListMetricsOp(ctx context.Context, cl *cloudwatch.Client) error {
	_, err := cl.ListMetrics(ctx, &cloudwatch.ListMetricsInput{Namespace: aws.String(cwNamespace)})

	return err
}

func cwGetMetricStatisticsOp(ctx context.Context, cl *cloudwatch.Client) error {
	now := time.Now()

	_, err := cl.GetMetricStatistics(ctx, &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String(cwNamespace),
		MetricName: aws.String(cwMetricName),
		StartTime:  aws.Time(now.Add(-cwStatWindow)),
		EndTime:    aws.Time(now),
		Period:     aws.Int32(cwStatPeriodSeconds),
		Statistics: []cwtypes.Statistic{cwtypes.StatisticSum, cwtypes.StatisticAverage},
	})

	return err
}

func cwDescribeAlarmsOp(ctx context.Context, cl *cloudwatch.Client) error {
	_, err := cl.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{})

	return err
}
