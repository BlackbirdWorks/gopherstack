package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_MacHosts_DescribeDerivedFromDedicatedHost(t *testing.T) {
	t.Parallel()

	h := newHandler()

	hosts, err := h.Backend.AllocateHosts("us-east-1a", "mac2.metal", 1)
	require.NoError(t, err)
	hostID := hosts[0].HostID

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeMacHosts")
	describeVals.Set("Version", "2016-11-15")

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	body := describeRec.Body.String()
	assert.Contains(t, body, "<DescribeMacHostsResponse")
	assert.Contains(t, body, hostID)
	assert.Contains(t, body, "<macOSLatestSupportedVersionSet>")
}

func TestHandler_MacModificationTask_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	h := newHandler()

	instances, err := h.Backend.RunInstances("ami-test", "mac2.metal", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	createVals := url.Values{}
	createVals.Set("Action", "CreateMacSystemIntegrityProtectionModificationTask")
	createVals.Set("Version", "2016-11-15")
	createVals.Set("InstanceId", instanceID)
	createVals.Set("MacSystemIntegrityProtectionStatus", "enabled")

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<CreateMacSystemIntegrityProtectionModificationTaskResponse")
	assert.Contains(t, createBody, "<taskState>pending</taskState>")

	taskID := extractXMLValue(t, createBody, "macModificationTaskId")
	require.NotEmpty(t, taskID)

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeMacModificationTasks")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("MacModificationTaskId.1", taskID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "<taskState>successful</taskState>")
}

func TestHandler_CreateDelegateMacVolumeOwnershipTask(t *testing.T) {
	t.Parallel()

	h := newHandler()

	instances, err := h.Backend.RunInstances("ami-test", "mac2-m2.metal", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	createVals := url.Values{}
	createVals.Set("Action", "CreateDelegateMacVolumeOwnershipTask")
	createVals.Set("Version", "2016-11-15")
	createVals.Set("InstanceId", instanceID)
	createVals.Set("MacCredentials", `{"internalDiskPassword":""}`)

	rec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<CreateDelegateMacVolumeOwnershipTaskResponse")
	assert.Contains(t, body, "<taskType>volume-ownership-delegation</taskType>")
}

func TestHandler_CreateMacSIPModificationTask_NonMacInstanceFails(t *testing.T) {
	t.Parallel()

	h := newHandler()

	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	vals := url.Values{}
	vals.Set("Action", "CreateMacSystemIntegrityProtectionModificationTask")
	vals.Set("Version", "2016-11-15")
	vals.Set("InstanceId", instanceID)
	vals.Set("MacSystemIntegrityProtectionStatus", "enabled")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>InvalidParameterValue</Code>")
}
