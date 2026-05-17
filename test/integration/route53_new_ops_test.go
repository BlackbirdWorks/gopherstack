package integration_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- helpers ----------------------------------------------------------------

func integCreateZoneForNewOps(t *testing.T, callerRef string) string {
	t.Helper()

	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>newops-%s.example.com</Name>
  <CallerReference>%s</CallerReference>
  <HostedZoneConfig><PrivateZone>false</PrivateZone></HostedZoneConfig>
</CreateHostedZoneRequest>`, callerRef, callerRef)

	resp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone", body)
	bodyStr := readBody(t, resp)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "body: %s", bodyStr)

	return integExtractZoneID(t, bodyStr)
}

func integCreateKSKForNewOps(t *testing.T, zoneID, name, callerRef string) {
	t.Helper()

	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>%s</CallerReference>
  <HostedZoneId>%s</HostedZoneId>
  <KeyManagementServiceArn>arn:aws:kms:us-east-1:123456789012:key/mrk-abc123</KeyManagementServiceArn>
  <Name>%s</Name>
  <Status>ACTIVE</Status>
</CreateKeySigningKeyRequest>`, callerRef, zoneID, name)

	resp := route53Send(t, http.MethodPost, "/2013-04-01/keysigningkey", body)
	bodyStr := readBody(t, resp)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "body: %s", bodyStr)
}

func integCreateCidrCollection(t *testing.T, name, callerRef string) string {
	t.Helper()

	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>%s</Name>
  <CallerReference>%s</CallerReference>
</CreateCidrCollectionRequest>`, name, callerRef)

	resp := route53Send(t, http.MethodPost, "/2013-04-01/cidrcollection", body)
	bodyStr := readBody(t, resp)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "body: %s", bodyStr)

	return integExtractCidrCollectionID(t, bodyStr)
}

func integCreateTrafficPolicy(t *testing.T, name string) string {
	t.Helper()

	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>%s</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}</Document>
  <Comment>integ test</Comment>
</CreateTrafficPolicyRequest>`, name)

	resp := route53Send(t, http.MethodPost, "/2013-04-01/trafficpolicy", body)
	bodyStr := readBody(t, resp)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "body: %s", bodyStr)

	return integExtractTrafficPolicyID(t, bodyStr)
}

func integCreateTrafficPolicyInstance(t *testing.T, zoneID, tpID string) string {
	t.Helper()

	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyInstanceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>%s</HostedZoneId>
  <Name>traffic.newops.example.com</Name>
  <TrafficPolicyId>%s</TrafficPolicyId>
  <TrafficPolicyVersion>1</TrafficPolicyVersion>
  <TTL>60</TTL>
</CreateTrafficPolicyInstanceRequest>`, zoneID, tpID)

	resp := route53Send(t, http.MethodPost, "/2013-04-01/trafficpolicyinstance", body)
	bodyStr := readBody(t, resp)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "body: %s", bodyStr)

	return integExtractTrafficPolicyInstanceID(t, bodyStr)
}

func integExtractCidrCollectionID(t *testing.T, body string) string {
	t.Helper()

	type createCidrResp struct {
		Collection struct {
			ID string `xml:"Id"`
		} `xml:"Collection"`
	}

	var resp createCidrResp
	require.NoError(t, xml.Unmarshal([]byte(body), &resp))
	require.NotEmpty(t, resp.Collection.ID)

	return resp.Collection.ID
}

func integExtractTrafficPolicyID(t *testing.T, body string) string {
	t.Helper()

	type createTPResp struct {
		TrafficPolicy struct {
			ID string `xml:"Id"`
		} `xml:"TrafficPolicy"`
	}

	var resp createTPResp
	require.NoError(t, xml.Unmarshal([]byte(body), &resp))
	require.NotEmpty(t, resp.TrafficPolicy.ID)

	return resp.TrafficPolicy.ID
}

func integExtractTrafficPolicyInstanceID(t *testing.T, body string) string {
	t.Helper()

	type createTPInstResp struct {
		TrafficPolicyInstance struct {
			ID string `xml:"Id"`
		} `xml:"TrafficPolicyInstance"`
	}

	var resp createTPInstResp
	require.NoError(t, xml.Unmarshal([]byte(body), &resp))
	require.NotEmpty(t, resp.TrafficPolicyInstance.ID)

	return resp.TrafficPolicyInstance.ID
}

// ---- DNSSEC -----------------------------------------------------------------

func TestIntegration_Route53_EnableDisableDNSSEC(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	zoneID := integCreateZoneForNewOps(t, "dnssec-enable-disable")

	// Create and activate a KSK (required before enabling DNSSEC).
	integCreateKSKForNewOps(t, zoneID, "main-ksk", "dnssec-ksk-ref-1")

	// Enable DNSSEC.
	enableResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/enable-dnssec", "")
	enableBody := readBody(t, enableResp)
	assert.Equal(t, http.StatusOK, enableResp.StatusCode)
	assert.Contains(t, enableBody, "EnableHostedZoneDNSSECResponse")
	assert.Contains(t, enableBody, "INSYNC")

	// GetDNSSEC should report SIGNING.
	getResp := route53Send(t, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/dnssec", "")
	getBody := readBody(t, getResp)
	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	assert.Contains(t, getBody, "GetDNSSECResponse")
	assert.Contains(t, getBody, "SIGNING")

	// Disable DNSSEC.
	disableResp := route53Send(t, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/disable-dnssec", "")
	disableBody := readBody(t, disableResp)
	assert.Equal(t, http.StatusOK, disableResp.StatusCode)
	assert.Contains(t, disableBody, "DisableHostedZoneDNSSECResponse")
	assert.Contains(t, disableBody, "INSYNC")

	// GetDNSSEC should now report NOT_SIGNING.
	getResp2 := route53Send(t, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/dnssec", "")
	getBody2 := readBody(t, getResp2)
	assert.Equal(t, http.StatusOK, getResp2.StatusCode)
	assert.Contains(t, getBody2, "NOT_SIGNING")
}

func TestIntegration_Route53_GetDNSSEC_ZoneNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := route53Send(t, http.MethodGet, "/2013-04-01/hostedzone/ZNONEXISTENT/dnssec", "")
	body := readBody(t, resp)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, body, "NoSuchHostedZone")
}

// ---- KeySigningKey deactivate/delete ----------------------------------------

func TestIntegration_Route53_DeactivateDeleteKSK(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	zoneID := integCreateZoneForNewOps(t, "ksk-deactivate-delete")
	kskName := "integ-ksk-1"
	integCreateKSKForNewOps(t, zoneID, kskName, "ksk-cal-ref-1")

	// Deactivate.
	deactivatePath := "/2013-04-01/keysigningkey/" + zoneID + "/" + kskName + "/deactivate"
	deactivateResp := route53Send(t, http.MethodPost, deactivatePath, "")
	deactivateBody := readBody(t, deactivateResp)
	assert.Equal(t, http.StatusOK, deactivateResp.StatusCode)
	assert.Contains(t, deactivateBody, "DeactivateKeySigningKeyResponse")
	assert.Contains(t, deactivateBody, "INSYNC")

	// Delete.
	deletePath := "/2013-04-01/keysigningkey/" + zoneID + "/" + kskName
	deleteResp := route53Send(t, http.MethodDelete, deletePath, "")
	deleteBody := readBody(t, deleteResp)
	assert.Equal(t, http.StatusOK, deleteResp.StatusCode)
	assert.Contains(t, deleteBody, "DeleteKeySigningKeyResponse")
}

func TestIntegration_Route53_DeleteKSK_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	zoneID := integCreateZoneForNewOps(t, "ksk-delete-notfound")
	deletePath := "/2013-04-01/keysigningkey/" + zoneID + "/nonexistent-ksk"
	resp := route53Send(t, http.MethodDelete, deletePath, "")
	body := readBody(t, resp)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.NotEmpty(t, body)
}

// ---- CIDR Collections -------------------------------------------------------

func TestIntegration_Route53_ListDeleteCidrCollections(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	colID := integCreateCidrCollection(t, "integ-cidr-col", "cidr-cal-ref-1")

	// List should contain the collection.
	listResp := route53Send(t, http.MethodGet, "/2013-04-01/cidrcollection", "")
	listBody := readBody(t, listResp)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	assert.Contains(t, listBody, "ListCidrCollectionsResponse")
	assert.Contains(t, listBody, colID)

	// Delete.
	deleteResp := route53Send(t, http.MethodDelete, "/2013-04-01/cidrcollection/"+colID, "")
	deleteBody := readBody(t, deleteResp)
	assert.Equal(t, http.StatusOK, deleteResp.StatusCode)
	assert.Contains(t, deleteBody, "DeleteCidrCollectionResponse")
}

func TestIntegration_Route53_DeleteCidrCollection_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := route53Send(t, http.MethodDelete, "/2013-04-01/cidrcollection/nonexistent-id", "")
	body := readBody(t, resp)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.NotEmpty(t, body)
}

// ---- Traffic Policies -------------------------------------------------------

func TestIntegration_Route53_ListTrafficPolicies(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tpID := integCreateTrafficPolicy(t, "integ-list-tp")

	listResp := route53Send(t, http.MethodGet, "/2013-04-01/trafficpolicies", "")
	listBody := readBody(t, listResp)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	assert.Contains(t, listBody, "ListTrafficPoliciesResponse")
	assert.Contains(t, listBody, tpID)
}

func TestIntegration_Route53_ListTrafficPolicyVersions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tpID := integCreateTrafficPolicy(t, "integ-list-tp-versions")

	listResp := route53Send(t, http.MethodGet, "/2013-04-01/trafficpolicies/"+tpID+"/versions", "")
	listBody := readBody(t, listResp)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	assert.Contains(t, listBody, "ListTrafficPolicyVersionsResponse")
	assert.Contains(t, listBody, tpID)
}

func TestIntegration_Route53_GetTrafficPolicy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tpID := integCreateTrafficPolicy(t, "integ-get-tp")

	getResp := route53Send(t, http.MethodGet, "/2013-04-01/trafficpolicy/"+tpID+"/1", "")
	getBody := readBody(t, getResp)
	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	assert.Contains(t, getBody, tpID)
}

func TestIntegration_Route53_DeleteTrafficPolicy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tpID := integCreateTrafficPolicy(t, "integ-delete-tp")

	deleteResp := route53Send(t, http.MethodDelete, "/2013-04-01/trafficpolicy/"+tpID+"/1", "")
	deleteBody := readBody(t, deleteResp)
	assert.Equal(t, http.StatusOK, deleteResp.StatusCode)
	assert.Contains(t, deleteBody, "DeleteTrafficPolicyResponse")

	// After deletion, get should return not found.
	getResp := route53Send(t, http.MethodGet, "/2013-04-01/trafficpolicy/"+tpID+"/1", "")
	getBody := readBody(t, getResp)
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
	assert.NotEmpty(t, getBody)
}

// ---- Traffic Policy Instances -----------------------------------------------

func TestIntegration_Route53_TrafficPolicyInstanceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	zoneID := integCreateZoneForNewOps(t, "tp-instance-lifecycle")
	tpID := integCreateTrafficPolicy(t, "integ-tp-for-instance")
	instID := integCreateTrafficPolicyInstance(t, zoneID, tpID)

	// GetTrafficPolicyInstance.
	getResp := route53Send(t, http.MethodGet, "/2013-04-01/trafficpolicyinstance/"+instID, "")
	getBody := readBody(t, getResp)
	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	assert.Contains(t, getBody, "TrafficPolicyInstanceResponse")
	assert.Contains(t, getBody, instID)

	// ListTrafficPolicyInstances.
	listResp := route53Send(t, http.MethodGet, "/2013-04-01/trafficpolicyinstances", "")
	listBody := readBody(t, listResp)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	assert.Contains(t, listBody, "ListTrafficPolicyInstancesResponse")
	assert.Contains(t, listBody, instID)

	// GetTrafficPolicyInstanceCount.
	countResp := route53Send(t, http.MethodGet, "/2013-04-01/trafficpolicyinstancecount", "")
	countBody := readBody(t, countResp)
	assert.Equal(t, http.StatusOK, countResp.StatusCode)
	assert.Contains(t, countBody, "GetTrafficPolicyInstanceCountResponse")

	// DeleteTrafficPolicyInstance.
	deleteResp := route53Send(t, http.MethodDelete, "/2013-04-01/trafficpolicyinstance/"+instID, "")
	deleteBody := readBody(t, deleteResp)
	assert.Equal(t, http.StatusOK, deleteResp.StatusCode)
	assert.Contains(t, deleteBody, "DeleteTrafficPolicyInstanceResponse")

	// After deletion, get should return not found.
	getResp2 := route53Send(t, http.MethodGet, "/2013-04-01/trafficpolicyinstance/"+instID, "")
	getBody2 := readBody(t, getResp2)
	assert.Equal(t, http.StatusNotFound, getResp2.StatusCode)
	assert.NotEmpty(t, getBody2)
}

func TestIntegration_Route53_DeleteTrafficPolicyInstance_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := route53Send(t, http.MethodDelete, "/2013-04-01/trafficpolicyinstance/nonexistent", "")
	body := readBody(t, resp)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.NotEmpty(t, body)
}
