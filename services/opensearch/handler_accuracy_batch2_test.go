package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// ---------------------------------------------------------------------------
// VPC Endpoints HTTP lifecycle
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_VpcEndpoints_CreateAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domARN := createDomainAndGetARN(t, h, "vpc-ep-domain")

	// Create a VPC endpoint.
	cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints",
		map[string]any{"DomainArn": domARN, "VpcOptions": map[string]any{"SubnetIds": []string{"subnet-1"}}})
	defer cr.Body.Close()
	require.Equal(t, http.StatusOK, cr.StatusCode)

	var cOut map[string]any
	require.NoError(t, json.NewDecoder(cr.Body).Decode(&cOut))
	ep := cOut["VpcEndpoint"].(map[string]any)
	assert.NotEmpty(t, ep["VpcEndpointId"])
	assert.Equal(t, "ACTIVE", ep["Status"])

	// List endpoints — must contain the created one.
	lr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/vpcEndpoints", nil)
	defer lr.Body.Close()
	require.Equal(t, http.StatusOK, lr.StatusCode)

	var lOut map[string]any
	require.NoError(t, json.NewDecoder(lr.Body).Decode(&lOut))
	eps, ok := lOut["VpcEndpoints"].([]any)
	require.True(t, ok)
	assert.Len(t, eps, 1)
}

func TestAccuracyBatch2_VpcEndpoints_DescribeByIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domARN := createDomainAndGetARN(t, h, "vpc-desc-domain")

	cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints",
		map[string]any{"DomainArn": domARN, "VpcOptions": map[string]any{}})
	var cOut map[string]any
	require.NoError(t, json.NewDecoder(cr.Body).Decode(&cOut))
	cr.Body.Close()
	epID := cOut["VpcEndpoint"].(map[string]any)["VpcEndpointId"].(string)

	// Describe by explicit ID.
	dr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints/describe",
		map[string]any{"VpcEndpointIds": []string{epID}})
	defer dr.Body.Close()
	require.Equal(t, http.StatusOK, dr.StatusCode)

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&dOut))
	described, ok := dOut["VpcEndpoints"].([]any)
	require.True(t, ok)
	assert.Len(t, described, 1)
	assert.Equal(t, epID, described[0].(map[string]any)["VpcEndpointId"])
}

func TestAccuracyBatch2_VpcEndpoints_UpdateAndDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domARN := createDomainAndGetARN(t, h, "vpc-ud-domain")

	cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints",
		map[string]any{"DomainArn": domARN, "VpcOptions": map[string]any{}})
	var cOut map[string]any
	require.NoError(t, json.NewDecoder(cr.Body).Decode(&cOut))
	cr.Body.Close()
	epID := cOut["VpcEndpoint"].(map[string]any)["VpcEndpointId"].(string)

	// Update the endpoint.
	ur := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/vpcEndpoints/"+epID,
		map[string]any{"VpcOptions": map[string]any{"SubnetIds": []string{"subnet-updated"}}})
	defer ur.Body.Close()
	require.Equal(t, http.StatusOK, ur.StatusCode)

	// Delete the endpoint.
	del := doRequest(t, h, http.MethodDelete, "/2021-01-01/opensearch/vpcEndpoints/"+epID, nil)
	defer del.Body.Close()
	require.Equal(t, http.StatusOK, del.StatusCode)

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(del.Body).Decode(&dOut))
	summary := dOut["VpcEndpointSummary"].(map[string]any)
	assert.Equal(t, epID, summary["VpcEndpointId"])

	// List should now be empty.
	lr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/vpcEndpoints", nil)
	defer lr.Body.Close()
	var lOut map[string]any
	require.NoError(t, json.NewDecoder(lr.Body).Decode(&lOut))
	eps := lOut["VpcEndpoints"].([]any)
	assert.Empty(t, eps)
}

// ---------------------------------------------------------------------------
// Scheduled Actions HTTP
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_ScheduledActions_ListWithDomainFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domain     string
		queryParam string
		wantLen    int
	}{
		{
			name:       "filter_returns_domain_actions",
			domain:     "sched-domain",
			queryParam: "sched-domain",
			wantLen:    1,
		},
		{
			name:       "filter_different_domain_returns_empty",
			domain:     "sched-domain",
			queryParam: "other-domain",
			wantLen:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Schedule an action for the domain.
			sr := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/scheduledActions/update",
				map[string]any{
					"DomainName": tt.domain,
					"ScheduledAction": map[string]any{
						"Id":            "action-1",
						"Type":          "JVM_HEAP_SIZE_TUNING",
						"ScheduledTime": "2026-07-01T00:00:00Z",
						"ScheduleAt":    "TIMESTAMP",
					},
				})
			sr.Body.Close()

			resp := doRequest(t, h, http.MethodGet,
				"/2021-01-01/opensearch/scheduledActions?DomainName="+tt.queryParam, nil)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			actions, ok := out["ScheduledActions"].([]any)
			require.True(t, ok)
			assert.Len(t, actions, tt.wantLen)
		})
	}
}

func TestAccuracyBatch2_ScheduledActions_UpdateReturnsAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/scheduledActions/update",
		map[string]any{
			"DomainName": "upd-sched-domain",
			"ScheduledAction": map[string]any{
				"Id":            "action-upd",
				"Type":          "SERVICE_SOFTWARE_UPDATE",
				"ScheduledTime": "2026-08-01T00:00:00Z",
				"ScheduleAt":    "TIMESTAMP",
			},
		})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	action, ok := out["ScheduledAction"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "action-upd", action["Id"])
}

// ---------------------------------------------------------------------------
// Reserved Instances HTTP
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_ReservedInstances_ListOfferingsAndPurchase(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// List offerings — must be non-empty.
	lor := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/reservedInstances/offerings", nil)
	defer lor.Body.Close()
	require.Equal(t, http.StatusOK, lor.StatusCode)

	var loOut map[string]any
	require.NoError(t, json.NewDecoder(lor.Body).Decode(&loOut))
	offerings, ok := loOut["ReservedInstanceOfferings"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, offerings, "must have at least one offering")

	offeringID := offerings[0].(map[string]any)["ReservedInstanceOfferingId"].(string)

	// Purchase the first offering.
	pr := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/reservedInstances/offerings/"+offeringID,
		map[string]any{"ReservationName": "my-reservation", "InstanceCount": 2})
	defer pr.Body.Close()
	require.Equal(t, http.StatusOK, pr.StatusCode)

	var pOut map[string]any
	require.NoError(t, json.NewDecoder(pr.Body).Decode(&pOut))
	assert.NotEmpty(t, pOut["ReservedInstanceId"])
	assert.Equal(t, "my-reservation", pOut["ReservationName"])

	// Purchased instance must appear in DescribeReservedInstances.
	dr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/reservedInstances", nil)
	defer dr.Body.Close()
	require.Equal(t, http.StatusOK, dr.StatusCode)

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&dOut))
	instances, ok := dOut["ReservedInstances"].([]any)
	require.True(t, ok)
	assert.Len(t, instances, 1)
}

func TestAccuracyBatch2_ReservedInstances_PurchaseNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/reservedInstances/offerings/nonexistent-offering",
		map[string]any{"ReservationName": "r1", "InstanceCount": 1})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// CC Outbound Connections HTTP
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_OutboundConnections_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create outbound connection.
	cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/cc/outboundConnection",
		map[string]any{
			"ConnectionAlias":  "my-alias",
			"LocalDomainInfo":  map[string]any{"DomainName": "local-dom"},
			"RemoteDomainInfo": map[string]any{"DomainName": "remote-dom"},
		})
	defer cr.Body.Close()
	require.Equal(t, http.StatusOK, cr.StatusCode)

	var cOut map[string]any
	require.NoError(t, json.NewDecoder(cr.Body).Decode(&cOut))
	connID := cOut["ConnectionId"].(string)
	assert.NotEmpty(t, connID)
	assert.Equal(t, "my-alias", cOut["ConnectionAlias"])

	// Describe returns the connection.
	dr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/cc/outboundConnection", nil)
	defer dr.Body.Close()
	require.Equal(t, http.StatusOK, dr.StatusCode)

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&dOut))
	conns, ok := dOut["Connections"].([]any)
	require.True(t, ok)
	assert.Len(t, conns, 1)

	// Delete the connection.
	del := doRequest(t, h, http.MethodDelete,
		"/2021-01-01/opensearch/cc/outboundConnection/"+connID, nil)
	defer del.Body.Close()
	require.Equal(t, http.StatusOK, del.StatusCode)

	var delOut map[string]any
	require.NoError(t, json.NewDecoder(del.Body).Decode(&delOut))
	conn := delOut["Connection"].(map[string]any)
	assert.Equal(t, connID, conn["ConnectionId"])
}

// ---------------------------------------------------------------------------
// CC Inbound Connections HTTP
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_InboundConnections_DescribeRejectDelete(t *testing.T) {
	t.Parallel()

	b, h := newTestHandlerAndBackend()

	// Seed connections (AWS creates inbound connections via remote outbound requests).
	opensearch.SeedInboundConnection(b, "conn-inb-1")
	opensearch.SeedInboundConnection(b, "conn-inb-2")

	// Accept conn-inb-1.
	ar := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/cc/inboundConnection/conn-inb-1/accept", nil)
	ar.Body.Close()

	// Describe returns it.
	dr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/cc/inboundConnection", nil)
	defer dr.Body.Close()
	require.Equal(t, http.StatusOK, dr.StatusCode)

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&dOut))
	conns, ok := dOut["Connections"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, conns)

	// Accept conn-inb-2, then reject it.
	ar2 := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/cc/inboundConnection/conn-inb-2/accept", nil)
	ar2.Body.Close()

	rr := doRequest(t, h, http.MethodPut,
		"/2021-01-01/opensearch/cc/inboundConnection/conn-inb-2/reject", nil)
	defer rr.Body.Close()
	require.Equal(t, http.StatusOK, rr.StatusCode)

	var rOut map[string]any
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&rOut))
	rConn := rOut["Connection"].(map[string]any)
	assert.Equal(t, "REJECTED", rConn["ConnectionStatus"].(map[string]any)["StatusCode"])

	// Delete the first connection.
	del := doRequest(t, h, http.MethodDelete,
		"/2021-01-01/opensearch/cc/inboundConnection/conn-inb-1", nil)
	defer del.Body.Close()
	require.Equal(t, http.StatusOK, del.StatusCode)

	var delOut map[string]any
	require.NoError(t, json.NewDecoder(del.Body).Decode(&delOut))
	delConn := delOut["Connection"].(map[string]any)
	assert.Equal(t, "conn-inb-1", delConn["ConnectionId"])
}

// ---------------------------------------------------------------------------
// DirectQuery data sources HTTP — Get/Delete/Update/List
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_DirectQuery_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/directQueryDataSource/nonexistent", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestAccuracyBatch2_DirectQuery_DeleteAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Add a direct-query data source.
	ar := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/directQueryDataSource",
		map[string]any{
			"DataSourceName": "dq-to-delete",
			"DataSourceType": map[string]any{"CloudWatchLogs": map[string]any{}},
			"Description":    "test",
		})
	ar.Body.Close()

	// List returns one entry.
	lr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/directQueryDataSource", nil)
	defer lr.Body.Close()
	require.Equal(t, http.StatusOK, lr.StatusCode)

	var lOut map[string]any
	require.NoError(t, json.NewDecoder(lr.Body).Decode(&lOut))
	sources, ok := lOut["DirectQueryDataSources"].([]any)
	require.True(t, ok)
	assert.Len(t, sources, 1)

	// Delete it.
	del := doRequest(t, h, http.MethodDelete,
		"/2021-01-01/opensearch/directQueryDataSource/dq-to-delete", nil)
	defer del.Body.Close()
	require.Equal(t, http.StatusOK, del.StatusCode)

	// List should now be empty.
	lr2 := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/directQueryDataSource", nil)
	defer lr2.Body.Close()
	var lOut2 map[string]any
	require.NoError(t, json.NewDecoder(lr2.Body).Decode(&lOut2))
	sources2 := lOut2["DirectQueryDataSources"].([]any)
	assert.Empty(t, sources2)
}

func TestAccuracyBatch2_DirectQuery_UpdateReturnsARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Add first.
	ar := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/directQueryDataSource",
		map[string]any{
			"DataSourceName": "dq-upd",
			"DataSourceType": map[string]any{"CloudWatchLogs": map[string]any{}},
			"Description":    "before",
		})
	ar.Body.Close()

	// Update.
	ur := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/directQueryDataSource/dq-upd",
		map[string]any{
			"DataSourceName": "dq-upd",
			"DataSourceType": map[string]any{"CloudWatchLogs": map[string]any{}},
			"Description":    "after",
		})
	defer ur.Body.Close()
	require.Equal(t, http.StatusOK, ur.StatusCode)

	var uOut map[string]any
	require.NoError(t, json.NewDecoder(ur.Body).Decode(&uOut))
	assert.Contains(t, uOut, "DataSourceArn")
}

// ---------------------------------------------------------------------------
// Domain Maintenance HTTP
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_DomainMaintenance_StartListGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "maint-http-domain")

	// Start maintenance.
	sr := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/maint-http-domain/maintenance",
		map[string]any{"Action": "REBOOT_NODE", "NodeId": "node-0"})
	defer sr.Body.Close()
	require.Equal(t, http.StatusOK, sr.StatusCode)

	var sOut map[string]any
	require.NoError(t, json.NewDecoder(sr.Body).Decode(&sOut))
	maintID := sOut["MaintenanceId"].(string)
	assert.NotEmpty(t, maintID)

	// List maintenances for domain.
	lr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/maint-http-domain/maintenance", nil)
	defer lr.Body.Close()
	require.Equal(t, http.StatusOK, lr.StatusCode)

	var lOut map[string]any
	require.NoError(t, json.NewDecoder(lr.Body).Decode(&lOut))
	maintenances, ok := lOut["DomainMaintenances"].([]any)
	require.True(t, ok)
	assert.Len(t, maintenances, 1)

	// Get maintenance status by ID.
	gr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/maint-http-domain/maintenance/"+maintID, nil)
	defer gr.Body.Close()
	require.Equal(t, http.StatusOK, gr.StatusCode)

	var gOut map[string]any
	require.NoError(t, json.NewDecoder(gr.Body).Decode(&gOut))
	assert.Equal(t, maintID, gOut["MaintenanceId"])
}

// ---------------------------------------------------------------------------
// DataSources HTTP — List/Get/Update/Delete
// ---------------------------------------------------------------------------

func TestAccuracyBatch2_DataSources_ListAndGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "ds-list-domain")

	// Add two data sources via HTTP.
	for _, name := range []string{"ds-alpha", "ds-beta"} {
		ar := doRequest(t, h, http.MethodPost,
			"/2021-01-01/opensearch/domain/ds-list-domain/dataSource",
			map[string]any{"Name": name, "DataSourceType": "S3GLUE", "Description": name})
		ar.Body.Close()
	}

	// List.
	lr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/ds-list-domain/dataSource", nil)
	defer lr.Body.Close()
	require.Equal(t, http.StatusOK, lr.StatusCode)

	var lOut map[string]any
	require.NoError(t, json.NewDecoder(lr.Body).Decode(&lOut))
	sources, ok := lOut["DataSources"].([]any)
	require.True(t, ok)
	assert.Len(t, sources, 2)

	// Get specific source.
	gr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/ds-list-domain/dataSource/ds-alpha", nil)
	defer gr.Body.Close()
	require.Equal(t, http.StatusOK, gr.StatusCode)

	var gOut map[string]any
	require.NoError(t, json.NewDecoder(gr.Body).Decode(&gOut))
	ds, ok := gOut["DataSource"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ds-alpha", ds["name"])
}

func TestAccuracyBatch2_DataSources_UpdateAndDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "ds-ud-domain")

	ar := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/ds-ud-domain/dataSource",
		map[string]any{"Name": "ds-target", "DataSourceType": "S3GLUE", "Description": "before"})
	ar.Body.Close()

	// Update returns success message.
	ur := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/ds-ud-domain/updateDataSource",
		map[string]any{"Name": "ds-target", "Description": "after"})
	defer ur.Body.Close()
	require.Equal(t, http.StatusOK, ur.StatusCode)

	var uOut map[string]any
	require.NoError(t, json.NewDecoder(ur.Body).Decode(&uOut))
	assert.Equal(t, "DataSource updated", uOut["Message"])

	// Delete.
	del := doRequest(t, h, http.MethodDelete,
		"/2021-01-01/opensearch/domain/ds-ud-domain/dataSource/ds-target", nil)
	defer del.Body.Close()
	require.Equal(t, http.StatusOK, del.StatusCode)

	var delOut map[string]any
	require.NoError(t, json.NewDecoder(del.Body).Decode(&delOut))
	assert.Equal(t, "DataSource deleted", delOut["Message"])

	// List should be empty after delete.
	lr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/ds-ud-domain/dataSource", nil)
	defer lr.Body.Close()
	var lOut map[string]any
	require.NoError(t, json.NewDecoder(lr.Body).Decode(&lOut))
	remaining := lOut["DataSources"].([]any)
	assert.Empty(t, remaining)
}
