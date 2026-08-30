package elbv2_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestInMemoryBackend_SnapshotRestore exercises a full-state Snapshot->Restore
// round-trip covering every resource kind on the backend: the five
// store.Table-backed resources (load balancers, target groups, listeners,
// rules, trust stores), the plain resourcePolicies map (no identity field of
// its own), and the doubly-nested targetDrainingUntil map (Phase 3.3
// datalayer conversion; see store_setup.go for why these two are left as raw
// maps instead of store.Tables). This is the parity-sweep fix for
// targetDrainingUntil persistence -- a restored backend must still drain and
// remove targets that were mid-deregistration when the snapshot was taken.
func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "full backend state survives a snapshot/restore round-trip"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			src := elbv2.NewInMemoryBackend("123456789012", config.DefaultRegion)
			defer src.Close()

			lb, err := src.CreateLoadBalancer(elbv2.CreateLoadBalancerInput{
				Name:    "snap-lb",
				Subnets: []string{"subnet-1", "subnet-2"},
			})
			require.NoError(t, err)

			tg, err := src.CreateTargetGroup(elbv2.CreateTargetGroupInput{
				Name:     "snap-tg",
				Protocol: "HTTP",
				Port:     80,
				VpcID:    "vpc-1",
			})
			require.NoError(t, err)

			listener, err := src.CreateListener(elbv2.CreateListenerInput{
				LoadBalancerArn: lb.LoadBalancerArn,
				Protocol:        "HTTP",
				Port:            80,
				DefaultActions:  []elbv2.Action{{Type: "forward", TargetGroupArn: tg.TargetGroupArn}},
			})
			require.NoError(t, err)

			rule, err := src.CreateRule(elbv2.CreateRuleInput{
				ListenerArn: listener.ListenerArn,
				Priority:    "1",
				Actions:     []elbv2.Action{{Type: "forward", TargetGroupArn: tg.TargetGroupArn}},
				Conditions:  []elbv2.Condition{{Field: "path-pattern", Values: []string{"/api/*"}}},
			})
			require.NoError(t, err)

			trustStore, err := src.CreateTrustStore("snap-ts", nil, "", "", "")
			require.NoError(t, err)

			addedBeforeSnapshot, err := src.AddTrustStoreRevocations(
				trustStore.TrustStoreArn,
				[]elbv2.RevocationContentInput{
					{S3Bucket: "my-bucket", S3Key: "revocations.crl", RevocationType: "CRL"},
				},
			)
			require.NoError(t, err)
			require.Len(t, addedBeforeSnapshot, 1)

			const policyDoc = `{"Version":"2012-10-17"}`
			require.NoError(t, src.PutResourcePolicy(lb.LoadBalancerArn, policyDoc))

			// Register then immediately deregister a target so it lands in the
			// "draining" state, populating the raw targetDrainingUntil map.
			require.NoError(t, src.RegisterTargets(tg.TargetGroupArn, []elbv2.Target{{ID: "10.0.0.1", Port: 80}}))
			require.NoError(t, src.DeregisterTargets(tg.TargetGroupArn, []elbv2.Target{{ID: "10.0.0.1", Port: 80}}))

			data := src.Snapshot(ctx)
			require.NotNil(t, data)

			dst := elbv2.NewInMemoryBackend("123456789012", config.DefaultRegion)
			defer dst.Close()

			require.NoError(t, dst.Restore(ctx, data))

			// Load balancer (store.Table).
			lbs, err := dst.DescribeLoadBalancers(nil, nil)
			require.NoError(t, err)
			require.Len(t, lbs, 1)
			assert.Equal(t, lb.LoadBalancerArn, lbs[0].LoadBalancerArn)
			assert.Equal(t, lb.LoadBalancerName, lbs[0].LoadBalancerName)

			// Target group (store.Table).
			tgs, err := dst.DescribeTargetGroups(nil, nil, "")
			require.NoError(t, err)
			require.Len(t, tgs, 1)
			assert.Equal(t, tg.TargetGroupArn, tgs[0].TargetGroupArn)

			// Listener (store.Table, indexed by loadBalancer).
			listeners, err := dst.DescribeListeners(lb.LoadBalancerArn, nil)
			require.NoError(t, err)
			require.Len(t, listeners, 1)
			assert.Equal(t, listener.ListenerArn, listeners[0].ListenerArn)

			// Rules (store.Table, indexed by listener): the auto-created default
			// rule plus the one explicitly created above.
			rules, err := dst.DescribeRules(listener.ListenerArn, nil)
			require.NoError(t, err)
			require.Len(t, rules, 2)

			var foundRule bool
			for _, r := range rules {
				if r.RuleArn == rule.RuleArn {
					foundRule = true
					assert.Equal(t, rule.Priority, r.Priority)
				}
			}
			assert.True(t, foundRule, "explicitly created rule must survive restore")

			// Trust store (store.Table).
			trustStores, err := dst.DescribeTrustStores(nil, nil)
			require.NoError(t, err)
			require.Len(t, trustStores, 1)
			assert.Equal(t, trustStore.TrustStoreArn, trustStores[0].TrustStoreArn)

			// The revocation added before the snapshot survives with its RevocationId
			// intact (int64, not re-numbered).
			revocations, err := dst.DescribeTrustStoreRevocations(trustStore.TrustStoreArn, nil)
			require.NoError(t, err)
			require.Len(t, revocations, 1)
			assert.Equal(t, addedBeforeSnapshot[0].RevocationID, revocations[0].RevocationID)

			// revocationIDCounter (scalar, not store.Table-backed) must also survive
			// the round-trip: a revocation added post-restore must get a fresh,
			// strictly-increasing RevocationId rather than colliding with (or
			// resetting behind) the one assigned before the snapshot.
			addedAfterRestore, err := dst.AddTrustStoreRevocations(
				trustStore.TrustStoreArn,
				[]elbv2.RevocationContentInput{
					{S3Bucket: "my-bucket", S3Key: "more-revocations.crl", RevocationType: "CRL"},
				},
			)
			require.NoError(t, err)
			require.Len(t, addedAfterRestore, 1)
			assert.Greater(t, addedAfterRestore[0].RevocationID, addedBeforeSnapshot[0].RevocationID)

			// resourcePolicies: plain map (value has no identity field), still persisted.
			gotPolicy, err := dst.GetResourcePolicy(lb.LoadBalancerArn)
			require.NoError(t, err)
			assert.JSONEq(t, policyDoc, gotPolicy)

			// targetDrainingUntil: doubly-nested map, parity-sweep fix. The
			// deregistered target must still read back as draining post-restore
			// rather than snapping back to registered/healthy or disappearing.
			health, err := dst.DescribeTargetHealth(tg.TargetGroupArn)
			require.NoError(t, err)
			require.Len(t, health, 1)
			assert.Equal(t, "draining", health[0].HealthState)
		})
	}
}
