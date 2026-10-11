package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestResourceCreator_DeleteEventsRuleWithTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		targets []any
	}{
		{name: "one_target", targets: []any{
			map[string]any{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:000000000000:q1"},
		}},
		{name: "two_targets", targets: []any{
			map[string]any{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:000000000000:q1"},
			map[string]any{"Id": "t2", "Arn": "arn:aws:sqs:us-east-1:000000000000:q2"},
		}},
		{name: "no_targets", targets: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends(t)
			rc := cloudformation.NewResourceCreator(backends)

			props := map[string]any{"Name": "r-" + tt.name, "ScheduleExpression": "rate(5 minutes)"}
			if tt.targets != nil {
				props["Targets"] = tt.targets
			}

			id, err := rc.Create(t.Context(), "Rule", "AWS::Events::Rule", props, nil, nil)
			require.NoError(t, err)

			require.NoError(t, rc.Delete(t.Context(), "AWS::Events::Rule", id, props, nil))

			rules, _, err := backends.EventBridge.Backend.ListRules(t.Context(), "default", "", "", 0)
			require.NoError(t, err)
			assert.Empty(t, rules)
		})
	}
}
