package integration_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// route53Send sends a request to the Route 53 endpoint in the shared container.
func route53Send(t *testing.T, method, path, body string) *http.Response {
	t.Helper()

	url := endpoint + path
	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, url, bodyReader)
	require.NoError(t, err)

	if body != "" {
		req.Header.Set("Content-Type", "application/xml")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_Route53_CreateHostedZone(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>integration-test.example.com</Name>
  <CallerReference>integ-ref-1</CallerReference>
  <HostedZoneConfig>
    <Comment>integration test</Comment>
    <PrivateZone>false</PrivateZone>
  </HostedZoneConfig>
</CreateHostedZoneRequest>`

	resp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone", body)
	respBody := readBody(t, resp)

	assert.Equal(t, http.StatusCreated, resp.StatusCode, "body: %s", respBody)
	assert.Contains(t, respBody, "integration-test.example.com")
	assert.Contains(t, respBody, "INSYNC")
}

func TestIntegration_Route53_ListHostedZones(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	// Create a zone first.
	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>list-test.example.com</Name>
  <CallerReference>list-ref-1</CallerReference>
  <HostedZoneConfig><PrivateZone>false</PrivateZone></HostedZoneConfig>
</CreateHostedZoneRequest>`

	createResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone", createBody)
	require.Equal(t, http.StatusCreated, createResp.StatusCode)
	_ = readBody(t, createResp)

	// List all zones.
	listResp := route53Send(t, http.MethodGet, "/2013-04-01/hostedzone", "")
	listBody := readBody(t, listResp)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	assert.Contains(t, listBody, "ListHostedZonesResponse")
}

func TestIntegration_Route53_GetHostedZone(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>get-test.example.com</Name>
  <CallerReference>get-ref-1</CallerReference>
  <HostedZoneConfig><PrivateZone>false</PrivateZone></HostedZoneConfig>
</CreateHostedZoneRequest>`

	createResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone", createBody)
	createRespBody := readBody(t, createResp)
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	zoneID := integExtractZoneID(t, createRespBody)

	getResp := route53Send(t, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, "")
	getBody := readBody(t, getResp)
	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	assert.Contains(t, getBody, "get-test.example.com")
}

func TestIntegration_Route53_DeleteHostedZone(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>delete-test.example.com</Name>
  <CallerReference>delete-ref-1</CallerReference>
  <HostedZoneConfig><PrivateZone>false</PrivateZone></HostedZoneConfig>
</CreateHostedZoneRequest>`

	createResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone", createBody)
	createRespBody := readBody(t, createResp)
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	zoneID := integExtractZoneID(t, createRespBody)

	delResp := route53Send(t, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, "")
	_ = readBody(t, delResp)
	assert.Equal(t, http.StatusOK, delResp.StatusCode)

	// Verify it's gone.
	getResp := route53Send(t, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, "")
	_ = readBody(t, getResp)
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
}

func TestIntegration_Route53_ChangeResourceRecordSets(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	// Create zone.
	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>records-test.example.com</Name>
  <CallerReference>records-ref-1</CallerReference>
  <HostedZoneConfig><PrivateZone>false</PrivateZone></HostedZoneConfig>
</CreateHostedZoneRequest>`

	createResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone", createBody)
	createRespBody := readBody(t, createResp)
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	zoneID := integExtractZoneID(t, createRespBody)

	// Add A record.
	changeXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>www.records-test.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>203.0.113.1</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	changeResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", changeXML)
	changeBody := readBody(t, changeResp)
	assert.Equal(t, http.StatusOK, changeResp.StatusCode, "body: %s", changeBody)
	assert.Contains(t, changeBody, "INSYNC")
}

func TestIntegration_Route53_ListResourceRecordSets(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	// Create zone.
	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>listrr-test.example.com</Name>
  <CallerReference>listrr-ref-1</CallerReference>
  <HostedZoneConfig><PrivateZone>false</PrivateZone></HostedZoneConfig>
</CreateHostedZoneRequest>`

	createResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone", createBody)
	createRespBody := readBody(t, createResp)
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	zoneID := integExtractZoneID(t, createRespBody)

	// Add two records.
	changeXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>api.listrr-test.example.com</Name>
          <Type>A</Type>
          <TTL>60</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>10.0.0.1</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>www.listrr-test.example.com</Name>
          <Type>CNAME</Type>
          <TTL>60</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>api.listrr-test.example.com</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	changeResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", changeXML)
	_ = readBody(t, changeResp)
	require.Equal(t, http.StatusOK, changeResp.StatusCode)

	// List records.
	listResp := route53Send(t, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	listBody := readBody(t, listResp)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	assert.Contains(t, listBody, "api.listrr-test.example.com")
	assert.Contains(t, listBody, "www.listrr-test.example.com")
	assert.Contains(t, listBody, "10.0.0.1")
}

// integExtractZoneID parses the hosted zone ID from a CreateHostedZoneResponse XML body.
func integExtractZoneID(t *testing.T, body string) string {
	t.Helper()

	type createResp struct {
		HostedZone struct {
			ID string `xml:"Id"`
		} `xml:"HostedZone"`
	}

	var resp createResp
	require.NoError(t, xml.Unmarshal([]byte(body), &resp))

	parts := strings.Split(resp.HostedZone.ID, "/")
	require.NotEmpty(t, parts)

	return parts[len(parts)-1]
}

func TestIntegration_Route53_CreateHealthCheck(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>integ-hc-ref-1</CallerReference>
  <HealthCheckConfig>
    <Type>HTTP</Type>
    <IPAddress>192.0.2.100</IPAddress>
    <Port>80</Port>
    <ResourcePath>/health</ResourcePath>
    <RequestInterval>30</RequestInterval>
    <FailureThreshold>3</FailureThreshold>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

	resp := route53Send(t, http.MethodPost, "/2013-04-01/healthcheck", body)
	respBody := readBody(t, resp)

	assert.Equal(t, http.StatusCreated, resp.StatusCode, "body: %s", respBody)
	assert.Contains(t, respBody, "CreateHealthCheckResponse")
	assert.Contains(t, respBody, "192.0.2.100")
}

func TestIntegration_Route53_HealthCheck_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create health check.
	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>integ-hc-lifecycle</CallerReference>
  <HealthCheckConfig>
    <Type>HTTPS</Type>
    <FullyQualifiedDomainName>example.com</FullyQualifiedDomainName>
    <Port>443</Port>
    <ResourcePath>/healthz</ResourcePath>
    <FailureThreshold>2</FailureThreshold>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

	createResp := route53Send(t, http.MethodPost, "/2013-04-01/healthcheck", createBody)
	createRespBody := readBody(t, createResp)
	require.Equal(t, http.StatusCreated, createResp.StatusCode, "body: %s", createRespBody)

	hcID := integExtractHealthCheckID(t, createRespBody)

	// Get.
	getResp := route53Send(t, http.MethodGet, "/2013-04-01/healthcheck/"+hcID, "")
	getBody := readBody(t, getResp)
	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	assert.Contains(t, getBody, "example.com")

	// List.
	listResp := route53Send(t, http.MethodGet, "/2013-04-01/healthcheck", "")
	listBody := readBody(t, listResp)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	assert.Contains(t, listBody, "ListHealthChecksResponse")

	// Status.
	statusResp := route53Send(t, http.MethodGet, "/2013-04-01/healthcheck/"+hcID+"/status", "")
	statusBody := readBody(t, statusResp)
	assert.Equal(t, http.StatusOK, statusResp.StatusCode)
	assert.Contains(t, statusBody, "Healthy")

	// Update.
	updateBody := `<?xml version="1.0" encoding="UTF-8"?>
<UpdateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ResourcePath>/updated-healthz</ResourcePath>
  <FailureThreshold>5</FailureThreshold>
</UpdateHealthCheckRequest>`
	updateResp := route53Send(t, http.MethodPost, "/2013-04-01/healthcheck/"+hcID, updateBody)
	updateRespBody := readBody(t, updateResp)
	assert.Equal(t, http.StatusOK, updateResp.StatusCode, "body: %s", updateRespBody)

	// Delete.
	delResp := route53Send(t, http.MethodDelete, "/2013-04-01/healthcheck/"+hcID, "")
	_ = readBody(t, delResp)
	assert.Equal(t, http.StatusOK, delResp.StatusCode)

	// Verify gone.
	getGoneResp := route53Send(t, http.MethodGet, "/2013-04-01/healthcheck/"+hcID, "")
	_ = readBody(t, getGoneResp)
	assert.Equal(t, http.StatusNotFound, getGoneResp.StatusCode)
}

func TestIntegration_Route53_WeightedRouting(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>weighted-test.example.com</Name>
  <CallerReference>weighted-ref-1</CallerReference>
  <HostedZoneConfig><PrivateZone>false</PrivateZone></HostedZoneConfig>
</CreateHostedZoneRequest>`

	createResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone", createBody)
	createRespBody := readBody(t, createResp)
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	zoneID := integExtractZoneID(t, createRespBody)

	changeXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>app.weighted-test.example.com</Name>
          <Type>A</Type>
          <SetIdentifier>us-east</SetIdentifier>
          <Weight>80</Weight>
          <TTL>60</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>1.2.3.4</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>app.weighted-test.example.com</Name>
          <Type>A</Type>
          <SetIdentifier>us-west</SetIdentifier>
          <Weight>20</Weight>
          <TTL>60</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>5.6.7.8</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	changeResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", changeXML)
	changeBody := readBody(t, changeResp)
	require.Equal(t, http.StatusOK, changeResp.StatusCode, "body: %s", changeBody)

	listResp := route53Send(t, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	listBody := readBody(t, listResp)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	assert.Contains(t, listBody, "us-east")
	assert.Contains(t, listBody, "us-west")
	assert.Contains(t, listBody, "1.2.3.4")
	assert.Contains(t, listBody, "5.6.7.8")
}

func TestIntegration_Route53_FailoverRouting(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create health check.
	hcBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>failover-hc-ref</CallerReference>
  <HealthCheckConfig>
    <Type>TCP</Type>
    <IPAddress>10.0.1.1</IPAddress>
    <Port>443</Port>
    <FailureThreshold>3</FailureThreshold>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

	hcResp := route53Send(t, http.MethodPost, "/2013-04-01/healthcheck", hcBody)
	hcRespBody := readBody(t, hcResp)
	require.Equal(t, http.StatusCreated, hcResp.StatusCode)
	hcID := integExtractHealthCheckID(t, hcRespBody)

	// Create hosted zone.
	zoneBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>failover-test.example.com</Name>
  <CallerReference>failover-zone-ref</CallerReference>
  <HostedZoneConfig><PrivateZone>false</PrivateZone></HostedZoneConfig>
</CreateHostedZoneRequest>`

	zoneResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone", zoneBody)
	zoneRespBody := readBody(t, zoneResp)
	require.Equal(t, http.StatusCreated, zoneResp.StatusCode)
	zoneID := integExtractZoneID(t, zoneRespBody)

	changeXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>api.failover-test.example.com</Name>
          <Type>A</Type>
          <SetIdentifier>primary</SetIdentifier>
          <Failover>PRIMARY</Failover>
          <HealthCheckId>` + hcID + `</HealthCheckId>
          <TTL>60</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>10.0.1.1</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>api.failover-test.example.com</Name>
          <Type>A</Type>
          <SetIdentifier>secondary</SetIdentifier>
          <Failover>SECONDARY</Failover>
          <TTL>60</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>10.0.2.1</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	changeResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", changeXML)
	changeBody := readBody(t, changeResp)
	require.Equal(t, http.StatusOK, changeResp.StatusCode, "body: %s", changeBody)

	listResp := route53Send(t, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	listBody := readBody(t, listResp)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	assert.Contains(t, listBody, "PRIMARY")
	assert.Contains(t, listBody, "SECONDARY")
	assert.Contains(t, listBody, hcID)
}

// integExtractHealthCheckID parses the health check ID from a CreateHealthCheckResponse XML body.
func integExtractHealthCheckID(t *testing.T, body string) string {
	t.Helper()

	type createHCResp struct {
		HealthCheck struct {
			ID string `xml:"Id"`
		} `xml:"HealthCheck"`
	}

	var resp createHCResp
	require.NoError(t, xml.Unmarshal([]byte(body), &resp))
	require.NotEmpty(t, resp.HealthCheck.ID)

	return resp.HealthCheck.ID
}
