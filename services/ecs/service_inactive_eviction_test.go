package ecs_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// pastInactiveServiceTTL is strictly greater than the modeled one-hour
// inactiveServiceTTL, so sweepServiceTransitionsLocked always evicts an
// INACTIVE service by the time it fires.
const pastInactiveServiceTTL = time.Hour + time.Second

// TestDeleteService_InactiveServiceEvictedAfterTTL locks in the fix for
// gopherstack's unbounded b.services growth: api_op_DeleteService.go
// (ecs@v1.96.0) documents that "in the future, INACTIVE services may be
// cleaned up and purged from Amazon ECS record keeping, and
// DescribeServices calls on those services return a ServiceNotFoundException
// error" -- this backend now evicts an INACTIVE service inactiveServiceTTL
// past the DRAINING->INACTIVE transition instead of keeping it forever.
func TestDeleteService_InactiveServiceEvictedAfterTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, tdArn := newDrainTestBackend(t)

		_, err := b.CreateService(ecs.CreateServiceInput{
			ServiceName:    "evict-svc",
			TaskDefinition: tdArn,
			DesiredCount:   0,
		})
		require.NoError(t, err)

		_, err = b.DeleteService("", "evict-svc", false)
		require.NoError(t, err)

		time.Sleep(pastServiceDrainDelay)

		svcs, failures, err := b.DescribeServices("", []string{"evict-svc"})
		require.NoError(t, err)
		require.Empty(t, failures)
		require.Len(t, svcs, 1)
		require.Equal(t, "INACTIVE", svcs[0].Status)

		time.Sleep(pastInactiveServiceTTL)

		svcs, failures, err = b.DescribeServices("", []string{"evict-svc"})
		require.NoError(t, err)
		assert.Empty(t, svcs)
		require.Len(t, failures, 1)
		assert.Equal(t, "evict-svc", failures[0].Arn)
	})
}

// TestDeleteService_InactiveServiceKeptWithinTTL proves the eviction above
// does not fire early: an INACTIVE service must stay describable for the
// whole of inactiveServiceTTL.
func TestDeleteService_InactiveServiceKeptWithinTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b, tdArn := newDrainTestBackend(t)

		_, err := b.CreateService(ecs.CreateServiceInput{
			ServiceName:    "keep-svc",
			TaskDefinition: tdArn,
			DesiredCount:   0,
		})
		require.NoError(t, err)

		_, err = b.DeleteService("", "keep-svc", false)
		require.NoError(t, err)

		time.Sleep(pastServiceDrainDelay)

		svcs, failures, err := b.DescribeServices("", []string{"keep-svc"})
		require.NoError(t, err)
		require.Empty(t, failures)
		require.Len(t, svcs, 1)
		assert.Equal(t, "INACTIVE", svcs[0].Status)
	})
}
