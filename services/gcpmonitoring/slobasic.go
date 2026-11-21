package gcpmonitoring

import (
	"strconv"
	"strings"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
)

const (
	msPerSec         = 1000.0
	labelLocationRes = "resource.labels.location"

	istioRequestCount      = "istio.io/service/server/request_count"
	istioResponseLatencies = "istio.io/service/server/response_latencies"
)

// basicTelemetry names the metrics a service type reports for availability and latency SLIs.
type basicTelemetry struct {
	count, latency, scope, codeLabel string
	locationLabel                    string
	badCode                          string
}

func telemetryOf(svc *monitoringpb.Service) (basicTelemetry, bool) {
	switch {
	case svc.GetCloudRun() != nil:
		return basicTelemetry{
			count: "run.googleapis.com/request_count", latency: "run.googleapis.com/request_latencies",
			scope: `resource.type = "cloud_run_revision" AND resource.labels.service_name = ` +
				strconv.Quote(svc.GetCloudRun().GetServiceName()),
			locationLabel: labelLocationRes, codeLabel: "metric.labels.response_code_class", badCode: "5xx",
		}, true
	case svc.GetCloudEndpoints() != nil:
		return basicTelemetry{
			count:   "serviceruntime.googleapis.com/api/request_count",
			latency: "serviceruntime.googleapis.com/api/request_latencies",
			scope: `resource.type = "api" AND resource.labels.service = ` +
				strconv.Quote(svc.GetCloudEndpoints().GetService()),
			locationLabel: labelLocationRes, codeLabel: "metric.labels.response_code_class", badCode: "5xx",
		}, true
	case svc.GetAppEngine() != nil:
		return basicTelemetry{
			count:   "appengine.googleapis.com/http/server/response_count",
			latency: "appengine.googleapis.com/http/server/response_latencies",
			scope: `resource.type = "gae_app" AND resource.labels.module_id = ` +
				strconv.Quote(svc.GetAppEngine().GetModuleId()),
			locationLabel: labelLocationRes, codeLabel: "metric.labels.response_code", badCode: "5",
		}, true
	}

	return istioTelemetryOf(svc)
}

func istioTelemetryOf(svc *monitoringpb.Service) (basicTelemetry, bool) {
	t := basicTelemetry{
		count: istioRequestCount, latency: istioResponseLatencies,
		locationLabel: labelLocationRes, codeLabel: "metric.labels.response_code", badCode: "5",
	}

	switch {
	case svc.GetClusterIstio() != nil:
		c := svc.GetClusterIstio()
		t.scope = `resource.labels.location = ` + strconv.Quote(c.GetLocation()) +
			` AND resource.labels.cluster_name = ` + strconv.Quote(c.GetClusterName()) +
			` AND metric.labels.destination_service_namespace = ` + strconv.Quote(c.GetServiceNamespace()) +
			` AND metric.labels.destination_service_name = ` + strconv.Quote(c.GetServiceName())
	case svc.GetMeshIstio() != nil:
		m := svc.GetMeshIstio()
		t.scope = `metric.labels.mesh_uid = ` + strconv.Quote(m.GetMeshUid()) +
			` AND metric.labels.destination_service_namespace = ` + strconv.Quote(m.GetServiceNamespace()) +
			` AND metric.labels.destination_service_name = ` + strconv.Quote(m.GetServiceName())
	case svc.GetIstioCanonicalService() != nil:
		c := svc.GetIstioCanonicalService()
		t.scope = `metric.labels.mesh_uid = ` + strconv.Quote(c.GetMeshUid()) +
			` AND metric.labels.destination_canonical_service_namespace = ` +
			strconv.Quote(c.GetCanonicalServiceNamespace()) +
			` AND metric.labels.destination_canonical_service_name = ` + strconv.Quote(c.GetCanonicalService())
	default:
		return basicTelemetry{}, false
	}

	return t, true
}

func (t basicTelemetry) scoped(metric string, locations []string) string {
	f := `metric.type = ` + strconv.Quote(metric) + ` AND ` + t.scope
	if len(locations) > 0 {
		quoted := make([]string, len(locations))
		for i, l := range locations {
			quoted[i] = strconv.Quote(l)
		}

		f += ` AND ` + t.locationLabel + ` = one_of(` + strings.Join(quoted, ",") + `)`
	}

	return f
}

// expandBasicSli spells a BasicSli out as the RequestBasedSli it computes for the service type.
func expandBasicSli(svc *monitoringpb.Service, basic *monitoringpb.BasicSli) (*monitoringpb.RequestBasedSli, bool) {
	t, ok := telemetryOf(svc)
	if !ok {
		return nil, false
	}

	switch c := basic.GetSliCriteria().(type) {
	case *monitoringpb.BasicSli_Availability:
		total := t.scoped(t.count, basic.GetLocation())
		good := total + ` AND NOT ` + t.codeLabel + ` = starts_with(` + strconv.Quote(t.badCode) + `)`

		return &monitoringpb.RequestBasedSli{Method: &monitoringpb.RequestBasedSli_GoodTotalRatio{
			GoodTotalRatio: &monitoringpb.TimeSeriesRatio{GoodServiceFilter: good, TotalServiceFilter: total},
		}}, true
	case *monitoringpb.BasicSli_Latency:
		return &monitoringpb.RequestBasedSli{Method: &monitoringpb.RequestBasedSli_DistributionCut{
			DistributionCut: &monitoringpb.DistributionCut{
				DistributionFilter: t.scoped(t.latency, basic.GetLocation()),
				Range:              &monitoringpb.Range{Max: thresholdMillis(c.Latency)},
			},
		}}, true
	}

	return nil, false
}

// explicitSLOLocked returns the objective with any BasicSli spelled out as a RequestBasedSli (view EXPLICIT).
func (b *InMemoryBackend) explicitSLOLocked(
	slo *monitoringpb.ServiceLevelObjective,
) *monitoringpb.ServiceLevelObjective {
	out := cloneSLO(slo)
	svc := b.services[sloServiceOf(slo.GetName())]

	if t, ok := out.GetServiceLevelIndicator().GetType().(*monitoringpb.ServiceLevelIndicator_BasicSli); ok {
		if r, expanded := expandBasicSli(svc, t.BasicSli); expanded {
			out.ServiceLevelIndicator = &monitoringpb.ServiceLevelIndicator{
				Type: &monitoringpb.ServiceLevelIndicator_RequestBased{RequestBased: r},
			}
		}
	}

	return out
}

func thresholdMillis(l *monitoringpb.BasicSli_LatencyCriteria) float64 {
	return l.GetThreshold().AsDuration().Seconds() * msPerSec
}
