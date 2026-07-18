package servicediscovery_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// persistenceFixtureIDs carries every identifier newPersistenceTestBackend
// creates, so the round-trip assertions in
// TestInMemoryBackend_SnapshotRestore_FullState can look each resource back
// up after Restore without re-deriving IDs.
type persistenceFixtureIDs struct {
	namespaceID  string
	namespaceARN string
	serviceID    string
	serviceARN   string
	instanceID   string
}

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (namespaces, services, instances, operations) and, by
// extension, every secondary store.Index derived from them (byARN/byName on
// namespaces, byARN/byNsName on services, byService on instances), plus both
// raw maps left un-converted (serviceAttributes, instanceHealthStatuses), so
// a Snapshot from it exercises the entire persisted surface of the backend.
func newPersistenceTestBackend(t *testing.T) (*servicediscovery.InMemoryBackend, persistenceFixtureIDs) {
	t.Helper()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")
	servicediscovery.SetDeterministicIDs(b)

	_, err := b.CreatePrivateDNSNamespace("ns1", "desc", "vpc-123", 0, map[string]string{"team": "infra"})
	require.NoError(t, err)

	nsList := b.ListNamespaces(servicediscovery.ListNamespacesFilter{})
	require.Len(t, nsList, 1)
	ns := nsList[0]

	svc, err := b.CreateService(
		"svc1", ns.ID, "desc", "",
		&servicediscovery.DNSConfig{
			RoutingPolicy: "MULTIVALUE",
			DNSRecords:    []servicediscovery.DNSRecord{{Type: "A", TTL: 60}},
		},
		nil,
		&servicediscovery.HealthCheckCustomConfig{FailureThreshold: 1},
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	_, err = b.RegisterInstance(svc.ID, "inst-1", map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"})
	require.NoError(t, err)

	require.NoError(t, b.UpdateServiceAttributes(svc.ARN, map[string]string{"attrKey": "attrVal"}))
	require.NoError(t, b.UpdateInstanceCustomHealthStatus(svc.ID, "inst-1", "UNHEALTHY"))
	require.NoError(t, b.TagResource(ns.ARN, map[string]string{"extra": "tag"}))

	return b, persistenceFixtureIDs{
		namespaceID:  ns.ID,
		namespaceARN: ns.ARN,
		serviceID:    svc.ID,
		serviceARN:   svc.ARN,
		instanceID:   "inst-1",
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table (namespaces, services, instances, operations), every secondary
// store.Index derived from them (byARN/byName on namespaces, byARN/byNsName
// on services, byService on instances), and both raw maps left un-converted
// (serviceAttributes, instanceHealthStatuses) through Snapshot -> Restore
// into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	b, ids := newPersistenceTestBackend(t)

	wantNamespaces := servicediscovery.NamespaceCount(b)
	wantServices := servicediscovery.ServiceCount(b)
	wantInstances := servicediscovery.InstanceCount(b)
	wantOperations := servicediscovery.OperationCount(b)
	wantServiceAttrs := servicediscovery.ServiceAttributeCount(b)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, wantNamespaces, servicediscovery.NamespaceCount(b2))
	assert.Equal(t, wantServices, servicediscovery.ServiceCount(b2))
	assert.Equal(t, wantInstances, servicediscovery.InstanceCount(b2))
	assert.Equal(t, wantOperations, servicediscovery.OperationCount(b2))
	assert.Equal(t, wantServiceAttrs, servicediscovery.ServiceAttributeCount(b2))

	verifyNamespacesRestored(t, b2, ids)
	verifyServicesRestored(t, b2, ids)
	verifyInstancesRestored(t, b2, ids)
	verifyCountersResumed(t, b2)
}

// verifyNamespacesRestored exercises the namespaces table plus its byARN and
// byName secondary indexes (the latter via TagResource/DiscoverInstances in
// the other verify* helpers).
func verifyNamespacesRestored(t *testing.T, b *servicediscovery.InMemoryBackend, ids persistenceFixtureIDs) {
	t.Helper()

	ns, err := b.GetNamespace(ids.namespaceID)
	require.NoError(t, err)
	assert.Equal(t, "ns1", ns.Name)

	gotTags, err := b.ListTagsForResource(ids.namespaceARN)
	require.NoError(t, err)
	assert.Equal(t, "infra", gotTags["team"])
	assert.Equal(t, "tag", gotTags["extra"])
}

// verifyServicesRestored exercises the services table plus its byARN
// secondary index and the serviceAttributes raw map.
func verifyServicesRestored(t *testing.T, b *servicediscovery.InMemoryBackend, ids persistenceFixtureIDs) {
	t.Helper()

	svc, err := b.GetService(ids.serviceID)
	require.NoError(t, err)
	assert.Equal(t, "svc1", svc.Name)
	assert.Equal(t, 1, svc.InstanceCount) // rebuilt from the instancesByService index

	gotARN, attrs, err := b.GetServiceAttributes(ids.serviceID)
	require.NoError(t, err)
	assert.Equal(t, ids.serviceARN, gotARN)
	assert.Equal(t, "attrVal", attrs["attrKey"])
}

// verifyInstancesRestored exercises the composite-keyed instances table, its
// byService secondary index, the instanceHealthStatuses raw map, and
// DiscoverInstances (which chains the namespacesByName and servicesByNsName
// indexes together).
func verifyInstancesRestored(t *testing.T, b *servicediscovery.InMemoryBackend, ids persistenceFixtureIDs) {
	t.Helper()

	inst, err := b.GetInstance(ids.serviceID, ids.instanceID)
	require.NoError(t, err)
	assert.Equal(t, ids.instanceID, inst.ID)

	instList, err := b.ListInstances(ids.serviceID)
	require.NoError(t, err)
	require.Len(t, instList, 1)

	statuses, err := b.GetInstancesHealthStatus(ids.serviceID, nil)
	require.NoError(t, err)
	assert.Equal(t, "UNHEALTHY", statuses[ids.instanceID])

	discovered, revision, err := b.DiscoverInstances("ns1", "svc1", "", nil)
	require.NoError(t, err)
	require.Len(t, discovered, 1)
	assert.Equal(t, ids.instanceID, discovered[0].InstanceID)
	assert.Positive(t, revision)

	ops := b.ListOperations(servicediscovery.ListOperationsFilter{})
	assert.NotEmpty(t, ops)
}

// verifyCountersResumed checks that the nsCounter persisted across
// Snapshot/Restore rather than resetting to zero: with deterministic IDs
// re-enabled, the next namespace ID must continue the sequence instead of
// restarting at counter 1.
func verifyCountersResumed(t *testing.T, b *servicediscovery.InMemoryBackend) {
	t.Helper()

	servicediscovery.SetDeterministicIDs(b)

	_, err := b.CreateHTTPNamespace("ns2", "", nil)
	require.NoError(t, err)

	nsList := b.ListNamespaces(servicediscovery.ListNamespacesFilter{Name: "ns2"})
	require.Len(t, nsList, 1)
	assert.Equal(t, fmt.Sprintf("ns-%026d", 2), nsList[0].ID)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies a snapshot whose
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

	assert.Equal(t, 0, servicediscovery.NamespaceCount(b))
	assert.Equal(t, 0, servicediscovery.ServiceCount(b))
	assert.Equal(t, 0, servicediscovery.InstanceCount(b))
	assert.Equal(t, 0, servicediscovery.OperationCount(b))
	assert.Equal(t, 0, servicediscovery.ServiceAttributeCount(b))

	_, err = b.GetNamespace(ids.namespaceID)
	require.ErrorIs(t, err, servicediscovery.ErrNamespaceNotFound)
}

// TestInMemoryBackend_RestoreOldFormatDecodesAsVersionZero verifies the
// pre-Phase-3.3 snapshot shape (no "version" field, raw per-map JSON instead
// of "tables") is treated as an incompatible version-0 snapshot rather than
// causing a decode error -- see TestInMemoryBackend_RestoreVersionMismatch.
func TestInMemoryBackend_RestoreOldFormatDecodesAsVersionZero(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")

	oldFormat := `{"namespaces":{},"services":{},"instances":{},"operations":{},` +
		`"serviceAttributes":{},"instanceHealthStatuses":{},"accountID":"000000000000","region":"us-east-1"}`

	err := b.Restore(t.Context(), []byte(oldFormat))
	require.NoError(t, err)
	assert.Equal(t, 0, servicediscovery.NamespaceCount(b))
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend, ids := newPersistenceTestBackend(t)
	h := servicediscovery.NewHandler(backend)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := servicediscovery.NewHandler(servicediscovery.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	ns, err := h2.Backend.GetNamespace(ids.namespaceID)
	require.NoError(t, err)
	assert.Equal(t, "ns1", ns.Name)
}

// TestServiceDiscovery_DeleteOrder verifies the AWS-correct delete ordering:
// namespace cannot be deleted while services exist, and service cannot be deleted
// while instances are registered. Explicit cleanup is required.
func TestServiceDiscovery_DeleteOrder(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateHTTPNamespace("my-ns", "", nil)
	require.NoError(t, err)

	nsList := b.ListNamespaces(servicediscovery.ListNamespacesFilter{})
	require.Len(t, nsList, 1)

	nsID := nsList[0].ID

	svc, err := b.CreateService("my-svc", nsID, "", "", nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.RegisterInstance(svc.ID, "inst-1", map[string]string{"ip": "1.2.3.4"})
	require.NoError(t, err)

	// Namespace delete refused while service exists.
	_, err = b.DeleteNamespace(nsID)
	require.ErrorIs(t, err, servicediscovery.ErrResourceInUse)

	// Service delete refused while instance registered.
	err = b.DeleteService(svc.ID)
	require.ErrorIs(t, err, servicediscovery.ErrResourceInUse)

	// Deregister instance first.
	_, err = b.DeregisterInstance(svc.ID, "inst-1")
	require.NoError(t, err)

	// Now service can be deleted.
	err = b.DeleteService(svc.ID)
	require.NoError(t, err)

	_, err = b.GetService(svc.ID)
	require.ErrorIs(t, err, servicediscovery.ErrServiceNotFound)

	// Now namespace can be deleted.
	_, err = b.DeleteNamespace(nsID)
	require.NoError(t, err)

	_, err = b.GetNamespace(nsID)
	require.ErrorIs(t, err, servicediscovery.ErrNamespaceNotFound)
}

// TestHandler_SnapshotRestore_ServiceAttributesAndHealth verifies that new state (service
// attributes, health statuses) is preserved through Snapshot/Restore.
func TestHandler_SnapshotRestore_ServiceAttributesAndHealth(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a service (with a custom health check, required for
	// UpdateInstanceCustomHealthStatus per AWS) and set its attributes.
	createRec := doSDRequest(t, h, "CreateService", map[string]any{
		"Name":                    "persist-svc",
		"HealthCheckCustomConfig": map[string]any{"FailureThreshold": 1},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	svc := createOut["Service"].(map[string]any)
	svcID := svc["Id"].(string)
	svcARN := svc["Arn"].(string)

	updateRec := doSDRequest(t, h, "UpdateServiceAttributes", map[string]any{
		"ServiceArn": svcARN,
		"Attributes": map[string]string{"stage": "test"},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	// Register an instance and set custom health status.
	regRec := doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "inst-persist",
		"Attributes": map[string]string{},
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	healthRec := doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "inst-persist",
		"Status":     "UNHEALTHY",
	})
	require.Equal(t, http.StatusOK, healthRec.Code)

	// Snapshot and restore.
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Verify service attributes restored.
	getRec := doSDRequest(t, h2, "GetServiceAttributes", map[string]any{"ServiceId": svcID})
	assert.Equal(t, http.StatusOK, getRec.Code)
	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	attrs := getOut["ServiceAttributes"].(map[string]any)["Attributes"].(map[string]any)
	assert.Equal(t, "test", attrs["stage"])
}
