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
			"k8s_container",
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
