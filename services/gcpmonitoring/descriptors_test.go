package gcpmonitoring_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/api/label"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/grpc/codes"
)

func TestMetricDescriptorLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.metric(t)

	d := &metricpb.MetricDescriptor{
		Type: "custom.googleapis.com/app/latency", MetricKind: metricpb.MetricDescriptor_GAUGE,
		ValueType: metricpb.MetricDescriptor_DOUBLE, Unit: "ms", Description: "latency", DisplayName: "Latency",
		Labels: []*label.LabelDescriptor{{Key: "route", ValueType: label.LabelDescriptor_STRING}},
	}

	created, err := c.CreateMetricDescriptor(t.Context(),
		&monitoringpb.CreateMetricDescriptorRequest{Name: proj, MetricDescriptor: d})
	require.NoError(t, err)
	assert.Equal(t, proj+"/metricDescriptors/custom.googleapis.com/app/latency", created.GetName())

	again, err := c.CreateMetricDescriptor(t.Context(),
		&monitoringpb.CreateMetricDescriptorRequest{Name: proj, MetricDescriptor: d})
	require.NoError(t, err)
	assert.Equal(t, created.GetName(), again.GetName())

	changed := &metricpb.MetricDescriptor{
		Type: d.GetType(), MetricKind: metricpb.MetricDescriptor_GAUGE, ValueType: metricpb.MetricDescriptor_INT64,
	}
	_, err = c.CreateMetricDescriptor(t.Context(),
		&monitoringpb.CreateMetricDescriptorRequest{Name: proj, MetricDescriptor: changed})
	assert.Equal(t, "AlreadyExists", codeOf(err).String())

	got, err := c.GetMetricDescriptor(t.Context(), &monitoringpb.GetMetricDescriptorRequest{Name: created.GetName()})
	require.NoError(t, err)
	assert.Equal(t, "ms", got.GetUnit())

	it := c.ListMetricDescriptors(t.Context(), &monitoringpb.ListMetricDescriptorsRequest{
		Name: proj, Filter: `metric.type = starts_with("custom.googleapis.com/")`,
	})
	first, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, d.GetType(), first.GetType())
	_, err = it.Next()
	assert.Equal(t, iterator.Done, err)

	require.NoError(
		t,
		c.DeleteMetricDescriptor(t.Context(), &monitoringpb.DeleteMetricDescriptorRequest{Name: created.GetName()}),
	)
	_, err = c.GetMetricDescriptor(t.Context(), &monitoringpb.GetMetricDescriptorRequest{Name: created.GetName()})
	assert.Equal(t, "NotFound", codeOf(err).String())
}

func TestMetricDescriptorErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.metric(t)

	tests := []struct {
		d    *metricpb.MetricDescriptor
		name string
	}{
		{
			&metricpb.MetricDescriptor{Type: "compute.googleapis.com/x", ValueType: metricpb.MetricDescriptor_INT64},
			"builtin prefix",
		},
		{&metricpb.MetricDescriptor{Type: "custom.googleapis.com/x"}, "no value type"},
		{&metricpb.MetricDescriptor{
			Type: "custom.googleapis.com/x", MetricKind: metricpb.MetricDescriptor_DELTA,
			ValueType: metricpb.MetricDescriptor_BOOL,
		}, "delta bool"},
		{&metricpb.MetricDescriptor{
			Type: "custom.googleapis.com/x", ValueType: metricpb.MetricDescriptor_INT64,
			Labels: []*label.LabelDescriptor{{Key: "a"}, {Key: "a"}},
		}, "duplicate labels"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := c.CreateMetricDescriptor(t.Context(),
				&monitoringpb.CreateMetricDescriptorRequest{Name: proj, MetricDescriptor: tt.d})
			assert.Equal(t, "InvalidArgument", codeOf(err).String())
		})
	}

	_, err := c.GetMetricDescriptor(t.Context(), &monitoringpb.GetMetricDescriptorRequest{
		Name: proj + "/metricDescriptors/custom.googleapis.com/none",
	})
	assert.Equal(t, "NotFound", codeOf(err).String())

	err = c.DeleteMetricDescriptor(t.Context(), &monitoringpb.DeleteMetricDescriptorRequest{
		Name: proj + "/metricDescriptors/run.googleapis.com/request_count",
	})
	assert.Equal(t, "InvalidArgument", codeOf(err).String())
}

func TestBuiltinAndResourceDescriptors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.metric(t)

	d, err := c.GetMetricDescriptor(t.Context(), &monitoringpb.GetMetricDescriptorRequest{
		Name: proj + "/metricDescriptors/compute.googleapis.com/instance/cpu/utilization",
	})
	require.NoError(t, err)
	assert.Equal(t, metricpb.MetricDescriptor_GAUGE, d.GetMetricKind())
	assert.Equal(t, metricpb.MetricDescriptor_DOUBLE, d.GetValueType())

	r, err := c.GetMonitoredResourceDescriptor(t.Context(), &monitoringpb.GetMonitoredResourceDescriptorRequest{
		Name: proj + "/monitoredResourceDescriptors/gce_instance",
	})
	require.NoError(t, err)
	assert.Equal(t, "gce_instance", r.GetType())

	var types []string

	it := c.ListMonitoredResourceDescriptors(t.Context(), &monitoringpb.ListMonitoredResourceDescriptorsRequest{
		Name: proj, Filter: `resource.type = starts_with("cloud_")`,
	})

	for {
		rd, nextErr := it.Next()
		if errors.Is(nextErr, iterator.Done) {
			break
		}

		require.NoError(t, nextErr)

		types = append(types, rd.GetType())
	}

	assert.Contains(t, types, "cloud_run_revision")
	assert.NotContains(t, types, "gce_instance")

	_, err = c.GetMonitoredResourceDescriptor(t.Context(), &monitoringpb.GetMonitoredResourceDescriptorRequest{
		Name: proj + "/monitoredResourceDescriptors/nope",
	})
	assert.Equal(t, "NotFound", codeOf(err).String())
}

func TestExplicitDescriptorRejectsUnknownLabels(t *testing.T) {
	t.Parallel()

	const typ = "custom.googleapis.com/app/explicit"

	tests := []struct {
		name     string
		explicit bool
		restore  bool
		labels   map[string]string
		wantCode codes.Code
	}{
		{"declared label", true, false, map[string]string{"route": "/a"}, codes.OK},
		{"unknown label", true, false, map[string]string{"route": "/a", "extra": "x"}, codes.InvalidArgument},
		{"unknown label after restore", true, true, map[string]string{"extra": "x"}, codes.InvalidArgument},
		{"auto-created descriptor grows", false, false, map[string]string{"route": "/a", "extra": "x"}, codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			c := e.metric(t)

			if tt.explicit {
				_, err := c.CreateMetricDescriptor(t.Context(), &monitoringpb.CreateMetricDescriptorRequest{
					Name: proj, MetricDescriptor: &metricpb.MetricDescriptor{
						Type: typ, MetricKind: metricpb.MetricDescriptor_GAUGE, ValueType: metricpb.MetricDescriptor_DOUBLE,
						Labels: []*label.LabelDescriptor{{Key: "route"}},
					},
				})
				require.NoError(t, err)
			}

			if tt.restore {
				snap := e.handler.Backend.Snapshot(t.Context())
				e = newEnv(t)
				require.NoError(t, e.handler.Backend.Restore(t.Context(), snap))
				c = e.metric(t)
			}

			err := c.CreateTimeSeries(t.Context(), &monitoringpb.CreateTimeSeriesRequest{
				Name: proj, TimeSeries: []*monitoringpb.TimeSeries{
					gaugeDouble(typ, tt.labels, instance("i-1"), epoch(), 1),
				},
			})
			assert.Equal(t, tt.wantCode, codeOf(err))

			if tt.wantCode == codes.OK && !tt.explicit {
				got, gerr := c.GetMetricDescriptor(t.Context(),
					&monitoringpb.GetMetricDescriptorRequest{Name: proj + "/metricDescriptors/" + typ})
				require.NoError(t, gerr)
				assert.Len(t, got.GetLabels(), 2)
			}
		})
	}
}
