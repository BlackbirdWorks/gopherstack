package ocimonitoring_test

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const childCompartment = "ocid1.compartment.oc1..child"

func subtreeEnv(t *testing.T) *env {
	t.Helper()

	e := newEnv(t, envOpts{})
	e.backend.SetCompartmentTree(testTenancy, func(id, ancestor string) bool {
		return ancestor == testTenancy || (id == childCompartment && ancestor == testCompartment)
	})

	d := e.metric(ns, metric, map[string]string{"host": "a"}, pt{-min1(2), 10, 0})
	d.CompartmentId = new(childCompartment)

	_, err := e.mon.PostMetricData(t.Context(), monitoring.PostMetricDataRequest{
		MetricData: []monitoring.MetricDataDetails{d},
	})
	require.NoError(t, err)

	return e
}

func TestCompartmentSubtreeMetrics(t *testing.T) {
	t.Parallel()

	e := subtreeEnv(t)

	tests := []struct {
		name        string
		compartment string
		errCode     string
		want        int
		subtree     bool
	}{
		{"exact misses child", testCompartment, "", 0, false},
		{"tenancy subtree finds child", testTenancy, "", 1, true},
		{"tenancy exact misses child", testTenancy, "", 0, false},
		{"non tenancy subtree rejected", testCompartment, "InvalidParameter", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			list, err := e.mon.ListMetrics(t.Context(), monitoring.ListMetricsRequest{
				CompartmentId: new(tt.compartment), CompartmentIdInSubtree: new(tt.subtree),
			})

			if tt.errCode != "" {
				assert.Equal(t, tt.errCode, svcErr(t, err).GetCode())

				return
			}

			require.NoError(t, err)
			assert.Len(t, list.Items, tt.want)

			sum, err := e.mon.SummarizeMetricsData(t.Context(), monitoring.SummarizeMetricsDataRequest{
				CompartmentId: new(tt.compartment), CompartmentIdInSubtree: new(tt.subtree),
				Namespace: new(ns), Query: new(metric + "[1m].max()"),
			})
			require.NoError(t, err)
			assert.Len(t, sum.Items, tt.want)
		})
	}
}

func TestCompartmentSubtreeAlarms(t *testing.T) {
	t.Parallel()

	e := subtreeEnv(t)

	_, err := e.mon.CreateAlarm(t.Context(), monitoring.CreateAlarmRequest{
		CreateAlarmDetails: e.alarmDetails(topicOCID, func(d *monitoring.CreateAlarmDetails) {
			d.MetricCompartmentIdInSubtree = new(true)
		}),
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	e.mkAlarm(t, topicOCID, func(d *monitoring.CreateAlarmDetails) {
		d.CompartmentId = new(childCompartment)
		d.MetricCompartmentId = new(testTenancy)
		d.MetricCompartmentIdInSubtree = new(true)
		d.Query = new(metric + "[1m].max() > 5")
	})

	list, err := e.mon.ListAlarms(t.Context(), monitoring.ListAlarmsRequest{
		CompartmentId: new(testTenancy), CompartmentIdInSubtree: new(true),
	})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)

	list, err = e.mon.ListAlarms(t.Context(), monitoring.ListAlarmsRequest{CompartmentId: new(testTenancy)})
	require.NoError(t, err)
	assert.Empty(t, list.Items)

	status, err := e.mon.ListAlarmsStatus(t.Context(), monitoring.ListAlarmsStatusRequest{
		CompartmentId: new(testTenancy), CompartmentIdInSubtree: new(true),
	})
	require.NoError(t, err)
	assert.Len(t, status.Items, 1)
}
