package stepfunctions_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const (
	isolationDef     = `{"StartAt":"S","States":{"S":{"Type":"Pass","End":true}}}`
	isolationRoleARN = "arn:aws:iam::123456789012:role/role"
)

func regionCtx(region string) context.Context {
	return context.WithValue(context.Background(), stepfunctions.RegionContextKeyForTest{}, region)
}

// TestStepFunctionsRegionIsolation verifies that resources created in one region
// are not visible when listing from a different region.
func TestStepFunctionsRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		createFn func(b *stepfunctions.InMemoryBackend, ctx context.Context) error
		listFn   func(b *stepfunctions.InMemoryBackend, ctx context.Context) (int, error)
		regionA  string
		regionB  string
	}{
		{
			name:    "state_machine_isolated_by_region",
			regionA: "us-east-1",
			regionB: "eu-west-1",
			createFn: func(b *stepfunctions.InMemoryBackend, ctx context.Context) error {
				_, err := b.CreateStateMachine(ctx, "iso-sm", isolationDef, isolationRoleARN, "STANDARD")

				return err
			},
			listFn: func(b *stepfunctions.InMemoryBackend, ctx context.Context) (int, error) {
				sms, _, err := b.ListStateMachines(ctx, "", 0)

				return len(sms), err
			},
		},
		{
			name:    "activity_isolated_by_region",
			regionA: "us-east-1",
			regionB: "ap-southeast-1",
			createFn: func(b *stepfunctions.InMemoryBackend, ctx context.Context) error {
				_, err := b.CreateActivity(ctx, "iso-activity")

				return err
			},
			listFn: func(b *stepfunctions.InMemoryBackend, ctx context.Context) (int, error) {
				acts, _, err := b.ListActivities(ctx, "", 0)

				return len(acts), err
			},
		},
		{
			name:    "same_name_allowed_in_different_regions",
			regionA: "us-east-1",
			regionB: "us-west-2",
			createFn: func(b *stepfunctions.InMemoryBackend, ctx context.Context) error {
				_, err := b.CreateStateMachine(ctx, "shared-name-sm", isolationDef, isolationRoleARN, "STANDARD")

				return err
			},
			listFn: func(b *stepfunctions.InMemoryBackend, ctx context.Context) (int, error) {
				sms, _, err := b.ListStateMachines(ctx, "", 0)

				return len(sms), err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := stepfunctions.NewInMemoryBackend()
			ctxA := regionCtx(tt.regionA)
			ctxB := regionCtx(tt.regionB)

			// Create resource in region A.
			err := tt.createFn(b, ctxA)
			require.NoError(t, err)

			// Also create the same named resource in region B for the same-name test
			// (only matters for the same_name_allowed test, others ignore second creation).
			if tt.name == "same_name_allowed_in_different_regions" {
				err = tt.createFn(b, ctxB)
				require.NoError(t, err, "same name in different region must succeed")
			}

			// List from region A — must find exactly 1.
			countA, err := tt.listFn(b, ctxA)
			require.NoError(t, err)
			assert.Equal(t, 1, countA, "region A must see its own resource")

			// List from region B — must find 0 (or 1 for the same-name test).
			countB, err := tt.listFn(b, ctxB)
			require.NoError(t, err)

			if tt.name == "same_name_allowed_in_different_regions" {
				assert.Equal(t, 1, countB, "region B must see its own resource")
			} else {
				assert.Equal(t, 0, countB, "region B must not see region A resources")
			}
		})
	}
}

// TestStepFunctionsSameNameDifferentRegions verifies that two state machines
// with the same name can coexist in different regions without conflict.
func TestStepFunctionsSameNameDifferentRegions(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	ctxEast := regionCtx("us-east-1")
	ctxWest := regionCtx("us-west-2")

	sm1, err := b.CreateStateMachine(ctxEast, "my-machine", isolationDef, isolationRoleARN, "STANDARD")
	require.NoError(t, err)

	sm2, err := b.CreateStateMachine(ctxWest, "my-machine", isolationDef, isolationRoleARN, "STANDARD")
	require.NoError(t, err)

	assert.NotEqual(t, sm1.StateMachineArn, sm2.StateMachineArn, "ARNs must differ (different regions)")
	assert.Contains(t, sm1.StateMachineArn, "us-east-1")
	assert.Contains(t, sm2.StateMachineArn, "us-west-2")
}

// TestStepFunctionsActivitySameNameDifferentRegions verifies that two activities
// with the same name can coexist in different regions without conflict.
func TestStepFunctionsActivitySameNameDifferentRegions(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackend()
	ctxEast := regionCtx("us-east-1")
	ctxWest := regionCtx("us-west-2")

	a1, err := b.CreateActivity(ctxEast, "my-activity")
	require.NoError(t, err)

	a2, err := b.CreateActivity(ctxWest, "my-activity")
	require.NoError(t, err)

	assert.NotEqual(t, a1.ActivityArn, a2.ActivityArn, "ARNs must differ (different regions)")
	assert.Contains(t, a1.ActivityArn, "us-east-1")
	assert.Contains(t, a2.ActivityArn, "us-west-2")
}
