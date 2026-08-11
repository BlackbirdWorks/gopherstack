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

// TestHandler_MacModificationTask_TagDualWritePathVisibility proves a
// create-time tag and one added later via CreateTags are BOTH visible through
// DescribeMacModificationTasks and DescribeTags.
func TestHandler_MacModificationTask_TagDualWritePathVisibility(t *testing.T) {
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
	createVals.Set("TagSpecification.1.ResourceType", "mac-modification-task")
	createVals.Set("TagSpecification.1.Tag.1.Key", "CreateTime")
	createVals.Set("TagSpecification.1.Tag.1.Value", "yes")

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	// The create-time tag must already be visible on the create response.
	assert.Contains(t, createBody, "CreateTime")

	taskID := extractXMLValue(t, createBody, "macModificationTaskId")
	require.NotEmpty(t, taskID)

	tagRec := postForm(t, h,
		"Action=CreateTags&Version=2016-11-15&ResourceId.1="+taskID+
			"&Tag.1.Key=AddedLater&Tag.1.Value=yes")
	require.Equal(t, http.StatusOK, tagRec.Code)

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeMacModificationTasks")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("MacModificationTaskId.1", taskID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	descBody := describeRec.Body.String()
	assert.Contains(t, descBody, "CreateTime")
	assert.Contains(t, descBody, "AddedLater")

	tagsRec := postForm(t, h,
		"Action=DescribeTags&Version=2016-11-15&Filter.1.Name=resource-id&Filter.1.Value.1="+taskID)
	require.Equal(t, http.StatusOK, tagsRec.Code)
	tagsBody := tagsRec.Body.String()
	assert.Contains(t, tagsBody, "CreateTime")
	assert.Contains(t, tagsBody, "AddedLater")
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
