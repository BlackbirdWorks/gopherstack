package gcpmonitoring_test

import (
	"maps"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestBasicSliAvailabilityOnIstio(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		svc    *monitoringpb.Service
		labels map[string]string
		res    string
	}{
		{
			"cluster",
			&monitoringpb.Service{Identifier: &monitoringpb.Service_ClusterIstio_{
				ClusterIstio: &monitoringpb.Service_ClusterIstio{
					Location: "us-central1", ClusterName: "c1", ServiceNamespace: "ns", ServiceName: "web",
				},
			}},
			map[string]string{"destination_service_namespace": "ns", "destination_service_name": "web"},
			"k8s_container",
		},
		{
			"mesh",
			&monitoringpb.Service{Identifier: &monitoringpb.Service_MeshIstio_{
				MeshIstio: &monitoringpb.Service_MeshIstio{
					MeshUid: "m1", ServiceNamespace: "ns", ServiceName: "web",
				},
			}},
			map[string]string{
				"mesh_uid": "m1", "destination_service_namespace": "ns", "destination_service_name": "web",
			},
			"gce_instance",
		},
		{
			"canonical",
			&monitoringpb.Service{Identifier: &monitoringpb.Service_IstioCanonicalService_{
				IstioCanonicalService: &monitoringpb.Service_IstioCanonicalService{
					MeshUid: "m1", CanonicalServiceNamespace: "ns", CanonicalService: "web",
				},
			}},
			map[string]string{
				"mesh_uid": "m1", "destination_canonical_service_namespace": "ns",
				"destination_canonical_service_name": "web",
			},
			"istio_canonical_service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tt.svc.DisplayName = "Istio"
			svc := e.createService(t, "istio", tt.svc)
			slo := e.createSLO(t, svc.GetName(), "avail", availabilitySLO("avail", 0.9))
			t0 := epoch()
			e.clk.set(t0)

			res := &monitoredres.MonitoredResource{Type: tt.res, Labels: map[string]string{
				"instance_id": "1", "zone": "us-central1-a",
			}}
			if tt.res == "k8s_container" {
				res.Labels = map[string]string{
					"location": "us-central1", "cluster_name": "c1", "namespace_name": "ns",
					"pod_name": "p", "container_name": "c",
				}
			}

			if tt.res == "istio_canonical_service" {
				res.Labels = map[string]string{
					"location": "us-central1", "mesh_uid": "m1", "namespace_name": "ns",
					"canonical_service_name": "web",
				}
			}

			for code, n := range map[string]int64{"200": 90, "503": 10} {
				ts := intSeries(
					"istio.io/service/server/request_count", metricpb.MetricDescriptor_DELTA, res,
					t0.Add(-time.Minute), t0, n,
				)
				ts.Metric.Labels = map[string]string{"response_code": code}

				maps.Copy(ts.GetMetric().GetLabels(), tt.labels)

				require.NoError(t, e.metric(t).CreateServiceTimeSeries(t.Context(),
					&monitoringpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: []*monitoringpb.TimeSeries{ts}}))
			}

			other := intSeries(
				"istio.io/service/server/request_count", metricpb.MetricDescriptor_DELTA, res,
				t0.Add(-time.Minute), t0, 1000,
			)
			other.Metric.Labels = map[string]string{
				"response_code": "500", "destination_service_name": "other", "mesh_uid": "x",
				"destination_canonical_service_name": "other",
			}
			require.NoError(t, e.metric(t).CreateServiceTimeSeries(t.Context(),
				&monitoringpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: []*monitoringpb.TimeSeries{other}}))

			st, err := e.handler.Backend.SLOStatus(slo.GetName())
			require.NoError(t, err)
			assert.InDelta(t, 0.9, st.Compliance, 1e-9)
			assert.InDelta(t, 100, st.Total, 1e-9)
		})
	}
}

func TestSLOExplicitViewExpandsIstioBasicSli(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		svc  *monitoringpb.Service
		want string
	}{
		{
			"cluster",
			&monitoringpb.Service{Identifier: &monitoringpb.Service_ClusterIstio_{
				ClusterIstio: &monitoringpb.Service_ClusterIstio{
					Location: "us-central1", ClusterName: "c1", ServiceNamespace: "ns", ServiceName: "web",
				},
			}},
			`resource.labels.cluster_name = "c1" AND metric.labels.destination_service_namespace = "ns"`,
		},
		{
			"mesh",
			&monitoringpb.Service{Identifier: &monitoringpb.Service_MeshIstio_{
				MeshIstio: &monitoringpb.Service_MeshIstio{MeshUid: "m1", ServiceNamespace: "ns", ServiceName: "web"},
			}},
			`metric.labels.mesh_uid = "m1" AND metric.labels.destination_service_namespace = "ns"`,
		},
		{
			"canonical",
			&monitoringpb.Service{Identifier: &monitoringpb.Service_IstioCanonicalService_{
				IstioCanonicalService: &monitoringpb.Service_IstioCanonicalService{
					MeshUid: "m1", CanonicalServiceNamespace: "ns", CanonicalService: "web",
				},
			}},
			`metric.labels.destination_canonical_service_name = "web"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tt.svc.DisplayName = "Istio"
			svc := e.createService(t, "istio", tt.svc)

			latency := availabilitySLO("latency", 0.9)
			latency.ServiceLevelIndicator.GetBasicSli().Location = []string{"us-east1"}
			latency.ServiceLevelIndicator.GetBasicSli().SliCriteria = &monitoringpb.BasicSli_Latency{
				Latency: &monitoringpb.BasicSli_LatencyCriteria{Threshold: durationpb.New(250 * time.Millisecond)},
			}
			lat := e.createSLO(t, svc.GetName(), "lat", latency)

			got, err := e.svcs(t).GetServiceLevelObjective(t.Context(), &monitoringpb.GetServiceLevelObjectiveRequest{
				Name: lat.GetName(), View: monitoringpb.ServiceLevelObjective_EXPLICIT,
			})
			require.NoError(t, err)

			cut := got.GetServiceLevelIndicator().GetRequestBased().GetDistributionCut()
			assert.Contains(t, cut.GetDistributionFilter(), "istio.io/service/server/response_latencies")
			assert.Contains(t, cut.GetDistributionFilter(), tt.want)
			assert.Contains(t, cut.GetDistributionFilter(), `resource.labels.location = one_of("us-east1")`)
			assert.InDelta(t, 250, cut.GetRange().GetMax(), 1e-9)
		})
	}
}
