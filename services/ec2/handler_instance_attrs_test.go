package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestHandler_ModifyAvailabilityZoneGroup(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "ModifyAvailabilityZoneGroup")
	vals.Set("Version", "2016-11-15")
	vals.Set("GroupName", "us-east-1-wl1-bos-wlz-1")
	vals.Set("OptInStatus", "opted-in")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<return>true</return>")
}

func TestHandler_ModifyHosts(t *testing.T) {
	t.Parallel()

	h := newHandler()

	hosts, err := h.Backend.AllocateHosts("us-east-1a", "c5.metal", 1)
	require.NoError(t, err)
	hostID := hosts[0].HostID

	vals := url.Values{}
	vals.Set("Action", "ModifyHosts")
	vals.Set("Version", "2016-11-15")
	vals.Set("HostId.1", hostID)
	vals.Set("AutoPlacement", "on")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ModifyHostsResponse")
	assert.Contains(t, body, hostID)
}

func TestHandler_ModifyInstanceCpuOptions_RequiresStopped(t *testing.T) {
	t.Parallel()

	h := newHandler()

	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	vals := url.Values{}
	vals.Set("Action", "ModifyInstanceCpuOptions")
	vals.Set("Version", "2016-11-15")
	vals.Set("InstanceId", instanceID)
	vals.Set("CoreCount", "2")
	vals.Set("ThreadsPerCore", "1")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>IncorrectInstanceState</Code>")

	_, err = h.Backend.StopInstances([]string{instanceID})
	require.NoError(t, err)
	h.Backend.(*ec2.InMemoryBackend).TickLifecycleForTest()

	rec = postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<coreCount>2</coreCount>")
	assert.Contains(t, body, "<threadsPerCore>1</threadsPerCore>")
}

func TestHandler_GetInstanceTpmEkPubAndUefiData(t *testing.T) {
	t.Parallel()

	h := newHandler()

	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	tpmVals := url.Values{}
	tpmVals.Set("Action", "GetInstanceTpmEkPub")
	tpmVals.Set("Version", "2016-11-15")
	tpmVals.Set("InstanceId", instanceID)
	tpmVals.Set("KeyFormat", "der")
	tpmVals.Set("KeyType", "rsa-2048")

	tpmRec := postForm(t, h, tpmVals.Encode())
	require.Equal(t, http.StatusOK, tpmRec.Code)
	assert.Contains(t, tpmRec.Body.String(), "<GetInstanceTpmEkPubResponse")

	uefiVals := url.Values{}
	uefiVals.Set("Action", "GetInstanceUefiData")
	uefiVals.Set("Version", "2016-11-15")
	uefiVals.Set("InstanceId", instanceID)

	uefiRec := postForm(t, h, uefiVals.Encode())
	require.Equal(t, http.StatusOK, uefiRec.Code)
	assert.Contains(t, uefiRec.Body.String(), "<GetInstanceUefiDataResponse")
}

func TestHandler_AssociateDisassociateInstanceEventWindow(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createVals := url.Values{}
	createVals.Set("Action", "CreateInstanceEventWindow")
	createVals.Set("Version", "2016-11-15")
	createVals.Set("Name", "test-window")
	createVals.Set("CronExpression", "0 4 * * 6,7")

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	windowID := extractXMLValue(t, createRec.Body.String(), "instanceEventWindowId")
	require.NotEmpty(t, windowID)

	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	assocVals := url.Values{}
	assocVals.Set("Action", "AssociateInstanceEventWindow")
	assocVals.Set("Version", "2016-11-15")
	assocVals.Set("InstanceEventWindowId", windowID)
	assocVals.Set("AssociationTarget.InstanceId.1", instanceID)

	assocRec := postForm(t, h, assocVals.Encode())
	require.Equal(t, http.StatusOK, assocRec.Code)
	assert.Contains(t, assocRec.Body.String(), instanceID)

	disassocVals := url.Values{}
	disassocVals.Set("Action", "DisassociateInstanceEventWindow")
	disassocVals.Set("Version", "2016-11-15")
	disassocVals.Set("InstanceEventWindowId", windowID)
	disassocVals.Set("AssociationTarget.InstanceId.1", instanceID)

	disassocRec := postForm(t, h, disassocVals.Encode())
	require.Equal(t, http.StatusOK, disassocRec.Code)
	assert.Contains(t, disassocRec.Body.String(), "<DisassociateInstanceEventWindowResponse")
}

func TestHandler_ModifyPublicIpDnsNameOptions_UnknownNIFails(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "ModifyPublicIpDnsNameOptions")
	vals.Set("Version", "2016-11-15")
	vals.Set("NetworkInterfaceId", "eni-doesnotexist")
	vals.Set("HostnameType", "public-ipv4-dns-name")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>InvalidNetworkInterfaceID.NotFound</Code>")
}
