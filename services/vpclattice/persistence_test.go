package vpclattice_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

// persistenceFixtureIDs carries every identifier newPersistenceTestBackend
// creates, so the round-trip assertions in
// TestInMemoryBackend_SnapshotRestore_FullState can look each resource back
// up after Restore without re-deriving IDs.
type persistenceFixtureIDs struct {
	serviceID         string
	serviceARN        string
	serviceNetworkID  string
	snsaID            string
	snvaID            string
	listenerID        string
	ruleID            string
	targetGroupID     string
	targetID          string
	alsID             string
	authPolicyResID   string
	resourcePolicyArn string
}

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (and, transitively, every secondary store.Index) plus
// every raw map left un-converted (targets, authPolicies, resourcePolicies,
// tags), so a Snapshot from it exercises the entire persisted surface of the
// backend.
func newPersistenceTestBackend(t *testing.T) (*vpclattice.InMemoryBackend, persistenceFixtureIDs) {
	t.Helper()

	b := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	ctx := t.Context()

	svc, err := b.CreateService(ctx, "svc1", "", "", "", map[string]string{"env": "test"})
	require.NoError(t, err)

	sn, err := b.CreateServiceNetwork(ctx, "sn1", "", nil)
	require.NoError(t, err)

	snsa, err := b.CreateServiceNetworkServiceAssociation(ctx, sn.ID, svc.ID, nil)
	require.NoError(t, err)

	snva, err := b.CreateServiceNetworkVpcAssociation(ctx, sn.ID, "vpc-1", []string{"sg-1"}, true, nil, nil)
	require.NoError(t, err)

	listener, err := b.CreateListener(svc.ID, "listener1", "HTTP", 80, nil, nil)
	require.NoError(t, err)

	rule, err := b.CreateRule(svc.ID, listener.ID, "rule1", 10, nil, nil, nil)
	require.NoError(t, err)

	tg, err := b.CreateTargetGroup(ctx, "tg1", "INSTANCE", nil, nil)
	require.NoError(t, err)

	_, err = b.RegisterTargets(tg.ID, []*vpclattice.Target{{ID: "i-1", Port: 8080}})
	require.NoError(t, err)

	als, err := b.CreateAccessLogSubscription(ctx, svc.ID, "arn:aws:s3:::bucket", "", nil)
	require.NoError(t, err)

	_, err = b.PutAuthPolicy(svc.ARN, `{"Version":"2012-10-17"}`)
	require.NoError(t, err)

	require.NoError(t, b.PutResourcePolicy(svc.ARN, `{"Version":"2012-10-17"}`))

	require.NoError(t, b.TagResource(svc.ARN, map[string]string{"team": "vpclattice"}))

	return b, persistenceFixtureIDs{
		serviceID:         svc.ID,
		serviceARN:        svc.ARN,
		serviceNetworkID:  sn.ID,
		snsaID:            snsa.ID,
		snvaID:            snva.ID,
		listenerID:        listener.ID,
		ruleID:            rule.ID,
		targetGroupID:     tg.ID,
		targetID:          "i-1",
		alsID:             als.ID,
		authPolicyResID:   svc.ARN,
		resourcePolicyArn: svc.ARN,
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table (services, serviceNetworks, snsas, snvas, listeners, rules,
// targetGroups, alss), every secondary store.Index derived from them
// (byName on services/serviceNetworks/targetGroups, byService on listeners,
// byListener on rules), and every raw map left un-converted (targets,
// authPolicies, resourcePolicies, tags) through Snapshot -> Restore into a
// fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original, ids := newPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// services table + servicesByName index (exercised by the duplicate-name check).
	svc, err := fresh.GetService(ids.serviceID)
	require.NoError(t, err)
	assert.Equal(t, ids.serviceID, svc.ID)
	assert.NotEmpty(t, svc.HostedZoneID)

	_, err = fresh.CreateService(t.Context(), "svc1", "", "", "", nil)
	require.ErrorIs(t, err, vpclattice.ErrAlreadyExists)

	// serviceNetworks table + networksByName index.
	sn, err := fresh.GetServiceNetwork(ids.serviceNetworkID)
	require.NoError(t, err)
	assert.Equal(t, ids.serviceNetworkID, sn.ID)

	_, err = fresh.CreateServiceNetwork(t.Context(), "sn1", "", nil)
	require.ErrorIs(t, err, vpclattice.ErrAlreadyExists)

	// snsas table.
	snsa, err := fresh.GetServiceNetworkServiceAssociation(ids.snsaID)
	require.NoError(t, err)
	assert.Equal(t, ids.snsaID, snsa.ID)

	// snvas table.
	snva, err := fresh.GetServiceNetworkVpcAssociation(ids.snvaID)
	require.NoError(t, err)
	assert.Equal(t, ids.snvaID, snva.ID)
	assert.True(t, snva.PrivateDNSEnabled)

	// listeners table + listenersByService index.
	listener, err := fresh.GetListener(ids.serviceID, ids.listenerID)
	require.NoError(t, err)
	assert.Equal(t, ids.listenerID, listener.ID)

	listeners, _, err := fresh.ListListeners(ids.serviceID, 0, "")
	require.NoError(t, err)
	require.Len(t, listeners, 1)

	// rules table + rulesByListener index. CreateListener implicitly creates
	// a "default" rule alongside the explicit "rule1", so two are expected.
	rule, err := fresh.GetRule(ids.serviceID, ids.listenerID, ids.ruleID)
	require.NoError(t, err)
	assert.Equal(t, ids.ruleID, rule.ID)

	rules, _, err := fresh.ListRules(ids.serviceID, ids.listenerID, 0, "")
	require.NoError(t, err)
	require.Len(t, rules, 2)

	// targetGroups table + tgsByName index.
	tg, err := fresh.GetTargetGroup(ids.targetGroupID)
	require.NoError(t, err)
	assert.Equal(t, ids.targetGroupID, tg.ID)

	_, err = fresh.CreateTargetGroup(t.Context(), "tg1", "INSTANCE", nil, nil)
	require.ErrorIs(t, err, vpclattice.ErrAlreadyExists)

	// targets raw map (order-sensitive slice-map, left un-converted).
	targets, _, err := fresh.ListTargets(t.Context(), ids.targetGroupID, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, targets, 1)
	assert.Equal(t, ids.targetID, targets[0].ID)

	// alss table.
	als, err := fresh.GetAccessLogSubscription(ids.alsID)
	require.NoError(t, err)
	assert.Equal(t, ids.alsID, als.ID)

	// authPolicies raw map.
	authPolicy, err := fresh.GetAuthPolicy(ids.authPolicyResID)
	require.NoError(t, err)
	assert.NotEmpty(t, authPolicy.Policy)

	// resourcePolicies raw map.
	policy, err := fresh.GetResourcePolicy(ids.resourcePolicyArn)
	require.NoError(t, err)
	assert.NotEmpty(t, policy)

	// tags raw map.
	tags, err := fresh.ListTagsForResource(ids.serviceARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "vpclattice", tags["team"])

	// Sanity: account/region carried through too.
	assert.Equal(t, "us-east-1", fresh.Region())
	assert.Equal(t, "000000000000", fresh.AccountID())
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b, ids := newPersistenceTestBackend(t)

	// A syntactically valid but version-mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, vpclattice.ServiceCount(b))
	assert.Equal(t, 0, vpclattice.ServiceNetworkCount(b))
	assert.Equal(t, 0, vpclattice.TargetGroupCount(b))
	assert.Equal(t, 0, vpclattice.ListenerCount(b))

	_, err = b.GetService(ids.serviceID)
	require.ErrorIs(t, err, vpclattice.ErrNotFound)

	_, err = b.GetAuthPolicy(ids.authPolicyResID)
	require.ErrorIs(t, err, vpclattice.ErrNotFound)

	_, err = b.ListTagsForResource(ids.serviceARN)
	require.NoError(t, err) // tags reset to an empty map, not an error.
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend. Before Phase 3.3, VPC Lattice had no such
// delegation even though InMemoryBackend implemented both methods -- dead
// wiring, since nothing ever called them through the Handler/Persistable
// path the rest of the fleet uses (see cli.go's setupPersistence).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend, ids := newPersistenceTestBackend(t)
	h := vpclattice.NewHandler(backend)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := vpclattice.NewHandler(vpclattice.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	svc, err := h2.Backend.GetService(ids.serviceID)
	require.NoError(t, err)
	assert.Equal(t, ids.serviceID, svc.ID)
}
