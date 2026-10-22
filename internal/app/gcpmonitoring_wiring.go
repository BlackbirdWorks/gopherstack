package app

import (
	"cloud.google.com/go/logging/apiv2/loggingpb"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcpmonitoringbackend "github.com/blackbirdworks/gopherstack/services/gcpmonitoring"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
)

const logMetricPageSize = 1000

// logMetricSource exposes Cloud Logging's log-based metrics to Cloud Monitoring.
type logMetricSource struct {
	b *gcploggingbackend.InMemoryBackend
}

func (s logMetricSource) LogMetrics(project string) []gcpmonitoringbackend.LogMetric {
	parent := "projects/" + project

	var out []gcpmonitoringbackend.LogMetric

	token := ""

	for {
		ms, next, err := s.b.ListLogMetrics(&loggingpb.ListLogMetricsRequest{
			Parent: parent, PageToken: token, PageSize: logMetricPageSize,
		})
		if err != nil {
			return out
		}

		for _, m := range ms {
			out = append(out, s.metric(parent, m))
		}

		if next == "" {
			return out
		}

		token = next
	}
}

func (s logMetricSource) metric(parent string, m *loggingpb.LogMetric) gcpmonitoringbackend.LogMetric {
	lm := gcpmonitoringbackend.LogMetric{
		Name: m.GetName(), Descriptor: m.GetMetricDescriptor(), CreateTime: m.GetCreateTime().AsTime(),
	}

	pts, err := s.b.MetricPoints(parent + "/metrics/" + m.GetName())
	if err != nil {
		return lm
	}

	for _, p := range pts {
		lm.Series = append(lm.Series, gcpmonitoringbackend.LogMetricSeries{
			Labels: p.Labels, BucketOptions: m.GetBucketOptions(), BucketCounts: p.BucketCounts,
			Count: p.Count, Sum: p.Sum, SumSqDev: p.SumSqDev,
		})
	}

	return lm
}

// wireGCPMonitoring connects Cloud Monitoring to Pub/Sub notification channels and Cloud Logging's log-based metrics.
func wireGCPMonitoring(byName map[string]service.Registerable) {
	monH, ok := byName["GCPMonitoring"].(*gcpmonitoringbackend.Handler)
	if !ok {
		return
	}

	if ps, isPS := byName["GCPPubSub"].(*gcppubsubbackend.Handler); isPS {
		monH.Backend.SetPublisher(logSinkPublisher{b: ps.Backend})
	}

	if lg, isLG := byName["GCPLogging"].(*gcploggingbackend.Handler); isLG {
		monH.Backend.SetLogMetricSource(logMetricSource{b: lg.Backend})
	}
}
