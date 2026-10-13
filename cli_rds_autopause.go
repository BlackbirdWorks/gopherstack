package main

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
)

// wireRDSAutoPauseMetrics publishes the zeroed ServerlessDatabaseCapacity, ACUUtilization and CPUUtilization
// metrics of auto-paused Aurora Serverless v2 instances to CloudWatch.
func wireRDSAutoPauseMetrics(ctx context.Context, byName map[string]service.Registerable) {
	cwH, ok := byName["CloudWatch"].(*cwbackend.Handler)
	if !ok {
		return
	}

	h, ok := byName["RDS"].(*rdsbackend.Handler)
	if !ok {
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	h.SetMetricEmitter(cwmetric.NewAsync(ctx, statisticSetEmitter(cwH), serviceMetricQueueSize, serviceMetricWindow))
}
