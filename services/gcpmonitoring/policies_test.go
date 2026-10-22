package gcpmonitoring_test

import (
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestAlertPolicyCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.alerts(t)
	created := e.createPolicy(t, queuePolicy(nil, monitoringpb.ComparisonType_COMPARISON_GT, 3))

	assert.Contains(t, created.GetName(), proj+"/alertPolicies/")
	assert.True(t, created.GetEnabled().GetValue())
	assert.Contains(t, created.GetConditions()[0].GetName(), created.GetName()+"/conditions/")
	assert.NotNil(t, created.GetCreationRecord())

	got, err := c.GetAlertPolicy(t.Context(), &monitoringpb.GetAlertPolicyRequest{Name: created.GetName()})
	require.NoError(t, err)
	assert.Equal(t, "queue too deep", got.GetDisplayName())

	next := queuePolicy(nil, monitoringpb.ComparisonType_COMPARISON_GT, 10)
	next.Name = created.GetName()
	next.DisplayName = "renamed"
	next.Conditions[0].Name = created.GetConditions()[0].GetName()

	upd, err := c.UpdateAlertPolicy(t.Context(), &monitoringpb.UpdateAlertPolicyRequest{
		UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"display_name", "conditions"}},
		AlertPolicy: next,
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", upd.GetDisplayName())
	assert.Equal(t, created.GetConditions()[0].GetName(), upd.GetConditions()[0].GetName(), "condition id is stable")
	assert.InDelta(t, 10.0, upd.GetConditions()[0].GetConditionThreshold().GetThresholdValue(), 0.0001)

	it := c.ListAlertPolicies(t.Context(), &monitoringpb.ListAlertPoliciesRequest{
		Name: proj, Filter: `display_name = "renamed"`,
	})
	first, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, created.GetName(), first.GetName())
	_, err = it.Next()
	assert.Equal(t, iterator.Done, err)

	require.NoError(
		t,
		c.DeleteAlertPolicy(t.Context(), &monitoringpb.DeleteAlertPolicyRequest{Name: created.GetName()}),
	)

	_, err = c.GetAlertPolicy(t.Context(), &monitoringpb.GetAlertPolicyRequest{Name: created.GetName()})
	assert.Equal(t, "NotFound", codeOf(err).String())
}

func TestAlertPolicyValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.alerts(t)

	tests := []struct {
		mutate func(*monitoringpb.AlertPolicy)
		name   string
	}{
		{func(p *monitoringpb.AlertPolicy) { p.DisplayName = "" }, "no display name"},
		{func(p *monitoringpb.AlertPolicy) { p.Conditions = nil }, "no conditions"},
		{func(p *monitoringpb.AlertPolicy) { p.Combiner = 0 }, "no combiner"},
		{func(p *monitoringpb.AlertPolicy) { p.Conditions[0].DisplayName = "" }, "no condition name"},
		{
			func(p *monitoringpb.AlertPolicy) { p.NotificationChannels = []string{proj + "/notificationChannels/1"} },
			"unknown channel",
		},
		{func(p *monitoringpb.AlertPolicy) {
			p.Conditions[0].GetConditionThreshold().Filter = `resource.type = "x"`
		}, "no metric type"},
		{
			func(p *monitoringpb.AlertPolicy) { p.Conditions[0].GetConditionThreshold().Filter = `metric.type =` },
			"bad filter",
		},
		{func(p *monitoringpb.AlertPolicy) { p.Conditions[0].GetConditionThreshold().Comparison = 0 }, "no comparison"},
		{func(p *monitoringpb.AlertPolicy) {
			p.Conditions[0].GetConditionThreshold().Duration = durationpb.New(90 * time.Second)
		}, "odd duration"},
		{func(p *monitoringpb.AlertPolicy) {
			p.Conditions[0].GetConditionThreshold().Aggregations[0].AlignmentPeriod = durationpb.New(time.Second)
		}, "short period"},
		{func(p *monitoringpb.AlertPolicy) { p.Conditions[0].Condition = nil }, "no condition type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := queuePolicy(nil, monitoringpb.ComparisonType_COMPARISON_GT, 3)
			tt.mutate(p)

			_, err := c.CreateAlertPolicy(
				t.Context(),
				&monitoringpb.CreateAlertPolicyRequest{Name: proj, AlertPolicy: p},
			)
			assert.Equal(t, "InvalidArgument", codeOf(err).String(), "%v", err)
		})
	}
}
