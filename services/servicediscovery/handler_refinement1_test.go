package servicediscovery_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// newBackendAndHandler returns both handler and backend for refinement tests
// that need direct backend access.
func newBackendAndHandler(t *testing.T) (*servicediscovery.InMemoryBackend, *servicediscovery.Handler) {
	t.Helper()

	b := servicediscovery.NewInMemoryBackend("123456789012", "us-east-1")

	return b, servicediscovery.NewHandler(b)
}

// TestRefinement1_AccountID verifies that AccountID() method is exposed on the backend.
func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("999999999999", "eu-west-1")
	assert.Equal(t, "999999999999", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestRefinement1_ErrNilAppContext verifies that Provider.Init rejects nil context.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &servicediscovery.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, servicediscovery.ErrNilAppContext)
}

// TestRefinement1_GetSupportedOperationsSorted verifies that GetSupportedOperations
// returns a sorted, deterministic list with the expected count.
func TestRefinement1_GetSupportedOperationsSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Equal(t, 30, servicediscovery.HandlerOpsLen(h), "expected 30 supported operations")

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i], "operations should be sorted; %q > %q", ops[i-1], ops[i])
	}
}

// TestRefinement1_NamespaceTagsInGetResponse verifies that Tags and CreateDate
// are included in GetNamespace responses.
func TestRefinement1_NamespaceTagsInGetResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create namespace with tags.
	createRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{
		"Name": "tagged-ns",
		"Tags": []map[string]any{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	require.Equal(t, 200, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	opID := createResp["OperationId"].(string)
	opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})

	var opResp map[string]any
	require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))
	nsID := opResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	// Get namespace and check tags are present.
	getRec := doSDRequest(t, h, "GetNamespace", map[string]any{"Id": nsID})
	require.Equal(t, 200, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	ns := getResp["Namespace"].(map[string]any)
	assert.NotNil(t, ns["Tags"], "Tags must be present in GetNamespace response")
	assert.NotZero(t, ns["CreateDate"], "CreateDate must be present in GetNamespace response")

	tags := ns["Tags"].([]any)
	assert.Len(t, tags, 2, "expected 2 tags")
	first := tags[0].(map[string]any)
	assert.Equal(t, "env", first["Key"])
	assert.Equal(t, "prod", first["Value"])
}

// TestRefinement1_ServiceTagsInGetResponse verifies that Tags and CreateDate
// are included in GetService and CreateService responses.
func TestRefinement1_ServiceTagsInGetResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSDRequest(t, h, "CreateService", map[string]any{
		"Name": "tagged-svc",
		"Tags": []map[string]any{
			{"Key": "version", "Value": "v2"},
		},
	})
	require.Equal(t, 200, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	svc := createResp["Service"].(map[string]any)
	assert.NotNil(t, svc["Tags"], "Tags must be in CreateService response")
	assert.NotZero(t, svc["CreateDate"], "CreateDate must be in CreateService response")

	svcID := svc["Id"].(string)
	getRec := doSDRequest(t, h, "GetService", map[string]any{"Id": svcID})
	require.Equal(t, 200, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	svcGet := getResp["Service"].(map[string]any)
	tags := svcGet["Tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "version", tags[0].(map[string]any)["Key"])
}

// TestRefinement1_MapToTagEntriesSorted verifies that tag entries are sorted deterministically.
func TestRefinement1_MapToTagEntriesSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSDRequest(t, h, "CreateService", map[string]any{
		"Name": "sort-svc",
		"Tags": []map[string]any{
			{"Key": "zzz", "Value": "last"},
			{"Key": "aaa", "Value": "first"},
			{"Key": "mmm", "Value": "middle"},
		},
	})

	listRec := doSDRequest(t, h, "ListServices", map[string]any{})
	require.Equal(t, 200, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	svcs := listResp["Services"].([]any)
	require.Len(t, svcs, 1)

	tags := svcs[0].(map[string]any)["Tags"].([]any)
	require.Len(t, tags, 3)
	assert.Equal(t, "aaa", tags[0].(map[string]any)["Key"])
	assert.Equal(t, "mmm", tags[1].(map[string]any)["Key"])
	assert.Equal(t, "zzz", tags[2].(map[string]any)["Key"])
}

// TestRefinement1_RegisterInstanceRealOperationID verifies that RegisterInstance
// returns a real, non-hardcoded operation ID that can be retrieved.
func TestRefinement1_RegisterInstanceRealOperationID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create namespace and service first.
	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-reg"})
	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	nsOpID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
	nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-reg", "NamespaceId": nsID})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	// Register instance.
	regRec := doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-001",
		"Attributes": map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"},
	})
	require.Equal(t, 200, regRec.Code)

	var regResp map[string]any
	require.NoError(t, json.Unmarshal(regRec.Body.Bytes(), &regResp))

	opID := regResp["OperationId"].(string)
	assert.NotEqual(t, "op-register", opID, "must not be hardcoded op ID")
	assert.NotEmpty(t, opID)

	// Verify the operation can be retrieved.
	opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
	require.Equal(t, 200, opRec.Code)

	var opResp map[string]any
	require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))
	assert.Equal(t, "REGISTER_INSTANCE", opResp["Operation"].(map[string]any)["Type"])
}

// TestRefinement1_DeregisterInstanceRealOperationID verifies that DeregisterInstance
// returns a real operation ID for polling.
func TestRefinement1_DeregisterInstanceRealOperationID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Setup.
	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-dereg"})
	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	nsOpID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
	nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-dereg", "NamespaceId": nsID})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-002",
		"Attributes": map[string]string{},
	})

	// Deregister.
	deregRec := doSDRequest(t, h, "DeregisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-002",
	})
	require.Equal(t, 200, deregRec.Code)

	var deregResp map[string]any
	require.NoError(t, json.Unmarshal(deregRec.Body.Bytes(), &deregResp))

	opID := deregResp["OperationId"].(string)
	assert.NotEqual(t, "op-deregister", opID)
	assert.NotEmpty(t, opID)

	opRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})
	require.Equal(t, 200, opRec.Code)

	var opResp map[string]any
	require.NoError(t, json.Unmarshal(opRec.Body.Bytes(), &opResp))
	assert.Equal(t, "DEREGISTER_INSTANCE", opResp["Operation"].(map[string]any)["Type"])
}

// TestRefinement1_GetInstancesHealthStatusStoredValue verifies that UpdateInstanceCustomHealthStatus
// changes the value returned by GetInstancesHealthStatus.
func TestRefinement1_GetInstancesHealthStatusStoredValue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Setup service and instances.
	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-health"})
	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	nsOpID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
	nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{
		"Name":                    "svc-health",
		"NamespaceId":             nsID,
		"HealthCheckCustomConfig": map[string]any{"FailureThreshold": 1},
	})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "inst-healthy", "Attributes": map[string]string{},
	})
	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "inst-unhealthy", "Attributes": map[string]string{},
	})

	// Mark one instance unhealthy.
	doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "inst-unhealthy",
		"Status":     "UNHEALTHY",
	})

	// Check statuses.
	statusRec := doSDRequest(t, h, "GetInstancesHealthStatus", map[string]any{
		"ServiceId": svcID,
	})
	require.Equal(t, 200, statusRec.Code)

	var statusResp map[string]any
	require.NoError(t, json.Unmarshal(statusRec.Body.Bytes(), &statusResp))

	statuses := statusResp["Status"].(map[string]any)
	assert.Equal(t, "HEALTHY", statuses["inst-healthy"])
	assert.Equal(t, "UNHEALTHY", statuses["inst-unhealthy"])
}

// TestRefinement1_GetInstancesHealthStatusFilterByIDs verifies the Instances filter.
func TestRefinement1_GetInstancesHealthStatusFilterByIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-filter-health"})
	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	nsOpID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
	nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-filter", "NamespaceId": nsID})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	for _, iid := range []string{"a", "b", "c"} {
		doSDRequest(t, h, "RegisterInstance", map[string]any{
			"ServiceId": svcID, "InstanceId": iid, "Attributes": map[string]string{},
		})
	}

	// Filter to only b and c.
	rec := doSDRequest(t, h, "GetInstancesHealthStatus", map[string]any{
		"ServiceId": svcID,
		"Instances": []string{"b", "c"},
	})
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	statuses := resp["Status"].(map[string]any)
	assert.Len(t, statuses, 2)
	assert.Contains(t, statuses, "b")
	assert.Contains(t, statuses, "c")
	assert.NotContains(t, statuses, "a")
}

// TestRefinement1_DiscoverInstancesHealthStatusFilter verifies that DiscoverInstances
// filters by HealthStatus.
func TestRefinement1_DiscoverInstancesHealthStatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-discover-health"})
	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	nsOpID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
	nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{
		"Name":                    "svc-discover-health",
		"NamespaceId":             nsID,
		"HealthCheckCustomConfig": map[string]any{"FailureThreshold": 1},
	})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "healthy-i", "Attributes": map[string]string{},
	})
	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "unhealthy-i", "Attributes": map[string]string{},
	})

	doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "unhealthy-i",
		"Status":     "UNHEALTHY",
	})

	// Discover with HEALTHY filter.
	recHealthy := doSDRequest(t, h, "DiscoverInstances", map[string]any{
		"NamespaceName": "ns-discover-health",
		"ServiceName":   "svc-discover-health",
		"HealthStatus":  "HEALTHY",
	})
	require.Equal(t, 200, recHealthy.Code)

	var respH map[string]any
	require.NoError(t, json.Unmarshal(recHealthy.Body.Bytes(), &respH))
	assert.Len(t, respH["Instances"].([]any), 1)
	assert.Equal(t, "healthy-i", respH["Instances"].([]any)[0].(map[string]any)["InstanceId"])

	// Discover with ALL filter - should return both.
	recAll := doSDRequest(t, h, "DiscoverInstances", map[string]any{
		"NamespaceName": "ns-discover-health",
		"ServiceName":   "svc-discover-health",
		"HealthStatus":  "ALL",
	})
	var respAll map[string]any
	require.NoError(t, json.Unmarshal(recAll.Body.Bytes(), &respAll))
	assert.Len(t, respAll["Instances"].([]any), 2)
}

// TestRefinement1_DiscoverInstancesQueryParameters verifies attribute filtering.
func TestRefinement1_DiscoverInstancesQueryParameters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": "ns-qp"})
	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))
	nsOpID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": nsOpID})
	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))
	nsID := nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-qp", "NamespaceId": nsID})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "i-web",
		"Attributes": map[string]string{"tier": "web", "color": "blue"},
	})
	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "i-db",
		"Attributes": map[string]string{"tier": "db", "color": "green"},
	})

	rec := doSDRequest(t, h, "DiscoverInstances", map[string]any{
		"NamespaceName":   "ns-qp",
		"ServiceName":     "svc-qp",
		"QueryParameters": map[string]string{"tier": "web"},
	})
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	insts := resp["Instances"].([]any)
	assert.Len(t, insts, 1)
	assert.Equal(t, "i-web", insts[0].(map[string]any)["InstanceId"])
}

// TestRefinement1_ListServicesNamespaceFilter verifies that ListServices filters by NAMESPACE_ID.
func TestRefinement1_ListServicesNamespaceFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create two namespaces.
	nsA := createNamespaceHelper(t, h, "ns-A")
	nsB := createNamespaceHelper(t, h, "ns-B")

	// Create services in each namespace.
	doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-A1", "NamespaceId": nsA})
	doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-A2", "NamespaceId": nsA})
	doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-B1", "NamespaceId": nsB})

	// List without filter - should return all 3.
	recAll := doSDRequest(t, h, "ListServices", map[string]any{})
	var respAll map[string]any
	require.NoError(t, json.Unmarshal(recAll.Body.Bytes(), &respAll))
	assert.Len(t, respAll["Services"].([]any), 3)

	// List with NAMESPACE_ID filter - should return only 2.
	recFiltered := doSDRequest(t, h, "ListServices", map[string]any{
		"Filters": []map[string]any{
			{"Name": "NAMESPACE_ID", "Values": []string{nsA}, "Condition": "EQ"},
		},
	})
	require.Equal(t, 200, recFiltered.Code)

	var respFiltered map[string]any
	require.NoError(t, json.Unmarshal(recFiltered.Body.Bytes(), &respFiltered))
	assert.Len(t, respFiltered["Services"].([]any), 2)
}

// TestRefinement1_ErrResourceNotFoundTagging verifies that tagging a non-existent
// resource ARN returns the correct error type.
func TestRefinement1_ErrResourceNotFoundTagging(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": "arn:aws:servicediscovery:us-east-1:000000000000:namespace/does-not-exist",
	})
	assert.Equal(t, 400, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// TestRefinement1_TagResourceNotFound verifies TagResource returns 400 for unknown ARN.
func TestRefinement1_TagResourceNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": "arn:aws:servicediscovery:us-east-1:000000000000:service/no-such",
		"Tags":        []map[string]any{{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, 400, rec.Code)
}

// TestRefinement1_UntagResourceNotFound verifies UntagResource returns 400 for unknown ARN.
func TestRefinement1_UntagResourceNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": "arn:aws:servicediscovery:us-east-1:000000000000:namespace/no-such",
		"TagKeys":     []string{"k"},
	})
	assert.Equal(t, 400, rec.Code)
}

// TestRefinement1_CascadeDeleteUsesCorrectPrefix verifies that cascade delete
// does NOT incorrectly delete instances from services with a common ID prefix.
func TestRefinement1_CascadeDeleteUsesCorrectPrefix(t *testing.T) {
	t.Parallel()

	b, h := newBackendAndHandler(t)

	// Seed two services with IDs that share a prefix.
	svc1 := servicediscovery.NewServiceForTest("svc-0000001", "svc-one", "")
	svc2 := servicediscovery.NewServiceForTest("svc-00000010", "svc-ten", "") // prefix of svc1

	servicediscovery.AddServiceInternal(b, svc1)
	servicediscovery.AddServiceInternal(b, svc2)

	// Seed instance under svc-0000001 only.
	inst1 := servicediscovery.NewInstanceForTest("i-1", "svc-0000001", map[string]string{})
	servicediscovery.AddInstanceInternal(b, inst1)

	// Seed instance under svc-00000010.
	inst2 := servicediscovery.NewInstanceForTest("i-2", "svc-00000010", map[string]string{})
	servicediscovery.AddInstanceInternal(b, inst2)

	assert.Equal(t, 2, servicediscovery.InstanceCount(b))

	// Delete svc-0000001 via HTTP.
	rec := doSDRequest(t, h, "DeleteService", map[string]any{"Id": "svc-0000001"})
	require.Equal(t, 200, rec.Code)

	// svc-00000010's instance should survive.
	assert.Equal(t, 1, servicediscovery.InstanceCount(b), "instance from svc-00000010 must not be deleted")
}

// TestRefinement1_ExportCountHelpers verifies that count helpers work correctly.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b, h := newBackendAndHandler(t)

	assert.Equal(t, 0, servicediscovery.NamespaceCount(b))
	assert.Equal(t, 0, servicediscovery.ServiceCount(b))
	assert.Equal(t, 0, servicediscovery.InstanceCount(b))
	assert.Equal(t, 0, servicediscovery.OperationCount(b))
	assert.Equal(t, 0, servicediscovery.ServiceAttributeCount(b))
	assert.Equal(t, 30, servicediscovery.HandlerOpsLen(h))
}

// TestRefinement1_StorageBackendInterfaceCompiles verifies that Handler.Backend
// is the StorageBackend interface type (compile-time check via assignment).
func TestRefinement1_StorageBackendInterfaceCompiles(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")
	var _ servicediscovery.StorageBackend = b // compile-time assertion

	h := servicediscovery.NewHandler(b)
	assert.NotNil(t, h.Backend)
}

// TestRefinement1_AddSeedHelpers verifies AddNamespaceInternal / AddServiceInternal work.
func TestRefinement1_AddSeedHelpers(t *testing.T) {
	t.Parallel()

	b, _ := newBackendAndHandler(t)

	ns := servicediscovery.NewNamespaceForTest("ns-seed-001", "seeded-ns", "HTTP")
	servicediscovery.AddNamespaceInternal(b, ns)
	assert.Equal(t, 1, servicediscovery.NamespaceCount(b))

	svc := servicediscovery.NewServiceForTest("svc-seed-001", "seeded-svc", "ns-seed-001")
	servicediscovery.AddServiceInternal(b, svc)
	assert.Equal(t, 1, servicediscovery.ServiceCount(b))

	inst := servicediscovery.NewInstanceForTest("inst-001", "svc-seed-001", map[string]string{"ip": "1.2.3.4"})
	servicediscovery.AddInstanceInternal(b, inst)
	assert.Equal(t, 1, servicediscovery.InstanceCount(b))
}

// TestRefinement1_Reset clears everything via Handler.Reset().
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b, h := newBackendAndHandler(t)

	ns := servicediscovery.NewNamespaceForTest("ns-r", "reset-ns", "HTTP")
	servicediscovery.AddNamespaceInternal(b, ns)
	assert.Equal(t, 1, servicediscovery.NamespaceCount(b))

	h.Reset()
	assert.Equal(t, 0, servicediscovery.NamespaceCount(b))
}

// TestRefinement1_ListNamespacesReturnsTags verifies ListNamespaces includes tags.
func TestRefinement1_ListNamespacesReturnsTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSDRequest(t, h, "CreateHttpNamespace", map[string]any{
		"Name": "ns-list-tags",
		"Tags": []map[string]any{{"Key": "k", "Value": "v"}},
	})

	rec := doSDRequest(t, h, "ListNamespaces", map[string]any{})
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	nsList := resp["Namespaces"].([]any)
	require.Len(t, nsList, 1)
	tags := nsList[0].(map[string]any)["Tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "k", tags[0].(map[string]any)["Key"])
}

// TestRefinement1_GetInstancesHealthStatusServiceNotFound verifies error on unknown service.
func TestRefinement1_GetInstancesHealthStatusServiceNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "GetInstancesHealthStatus", map[string]any{
		"ServiceId": "svc-does-not-exist",
	})
	assert.Equal(t, 400, rec.Code)
}

// TestRefinement1_DiscoverInstancesUnhealthyFilter verifies UNHEALTHY filter.
func TestRefinement1_DiscoverInstancesUnhealthyFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	nsID := createNamespaceHelper(t, h, "ns-unhealthy-disc")
	svcRec := doSDRequest(t, h, "CreateService", map[string]any{
		"Name":                    "svc-unhealthy-disc",
		"NamespaceId":             nsID,
		"HealthCheckCustomConfig": map[string]any{"FailureThreshold": 1},
	})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "u1", "Attributes": map[string]string{},
	})

	doSDRequest(t, h, "UpdateInstanceCustomHealthStatus", map[string]any{
		"ServiceId": svcID, "InstanceId": "u1", "Status": "UNHEALTHY",
	})

	rec := doSDRequest(t, h, "DiscoverInstances", map[string]any{
		"NamespaceName": "ns-unhealthy-disc",
		"ServiceName":   "svc-unhealthy-disc",
		"HealthStatus":  "UNHEALTHY",
	})
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["Instances"].([]any), 1)
}

// TestRefinement1_CreatePrivateDNSNamespaceNoVpc verifies that CreatePrivateDnsNamespace
// succeeds without Vpc (implementation is lenient - AWS validates Vpc server-side).
func TestRefinement1_CreatePrivateDNSNamespaceNoVpc(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "CreatePrivateDnsNamespace", map[string]any{
		"Name": "private-ns",
	})
	// Implementation accepts missing Vpc (lenient mock); returns 200.
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_OperationCountIncrementsOnRegisterDeregister verifies that
// register and deregister both create new operations.
func TestRefinement1_OperationCountIncrementsOnRegisterDeregister(t *testing.T) {
	t.Parallel()

	b, h := newBackendAndHandler(t)

	nsID := createNamespaceHelper(t, h, "ns-opcount")
	svcRec := doSDRequest(t, h, "CreateService", map[string]any{"Name": "svc-opcount", "NamespaceId": nsID})
	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))
	svcID := svcResp["Service"].(map[string]any)["Id"].(string)

	opsBefore := servicediscovery.OperationCount(b)

	doSDRequest(t, h, "RegisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "op-inst", "Attributes": map[string]string{},
	})
	assert.Equal(t, opsBefore+1, servicediscovery.OperationCount(b))

	doSDRequest(t, h, "DeregisterInstance", map[string]any{
		"ServiceId": svcID, "InstanceId": "op-inst",
	})
	assert.Equal(t, opsBefore+2, servicediscovery.OperationCount(b))
}

// TestRefinement1_DiscoverInstancesEmptyWhenNamespaceNotFound verifies graceful empty result.
func TestRefinement1_DiscoverInstancesEmptyWhenNamespaceNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSDRequest(t, h, "DiscoverInstances", map[string]any{
		"NamespaceName": "nonexistent",
		"ServiceName":   "nonexistent",
	})
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Instances"].([]any))
}

// TestRefinement1_HandlerBackendInterface verifies that NewHandler accepts a StorageBackend.
func TestRefinement1_HandlerBackendInterface(t *testing.T) {
	t.Parallel()

	b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")

	// This should compile because *InMemoryBackend implements StorageBackend.
	var sb servicediscovery.StorageBackend = b
	h := servicediscovery.NewHandler(sb)
	assert.NotNil(t, h)
}

// createNamespaceHelper is a convenience function that creates an HTTP namespace
// and returns its ID.
func createNamespaceHelper(t *testing.T, h *servicediscovery.Handler, name string) string {
	t.Helper()

	nsRec := doSDRequest(t, h, "CreateHttpNamespace", map[string]any{"Name": name})
	require.Equal(t, 200, nsRec.Code)

	var nsResp map[string]any
	require.NoError(t, json.Unmarshal(nsRec.Body.Bytes(), &nsResp))

	opID := nsResp["OperationId"].(string)
	nsOpRec := doSDRequest(t, h, "GetOperation", map[string]any{"OperationId": opID})

	var nsOpResp map[string]any
	require.NoError(t, json.Unmarshal(nsOpRec.Body.Bytes(), &nsOpResp))

	return nsOpResp["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)
}
