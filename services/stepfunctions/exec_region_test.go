package stepfunctions_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

type regionRecordingECS struct {
	region atomic.Value
}

func (r *regionRecordingECS) SFNRunTask(ctx context.Context, _ map[string]any) (any, error) {
	r.region.Store(awsmeta.Region(ctx))

	return map[string]any{"Tasks": []any{}, "Failures": []any{}}, nil
}

func TestExecutionContextCarriesBackendRegion(t *testing.T) {
	t.Parallel()

	const definition = `{"StartAt":"Run","States":{"Run":{"Type":"Task",` +
		`"Resource":"arn:aws:states:::ecs:runTask","Parameters":{"TaskDefinition":"td","Cluster":"c"},"End":true}}}`

	tests := []struct {
		name   string
		region string
	}{
		{name: "home", region: "us-east-1"},
		{name: "other-region", region: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", tc.region)
				rec := &regionRecordingECS{}
				b.SetECSIntegration(rec)

				sm, err := b.CreateStateMachine(context.Background(), "sm", definition, "arn:role", "STANDARD")
				require.NoError(t, err)

				_, err = b.StartExecution(sm.StateMachineArn, "e1", "{}")
				require.NoError(t, err)

				synctest.Wait()

				assert.Equal(t, tc.region, rec.region.Load())
			})
		})
	}
}
