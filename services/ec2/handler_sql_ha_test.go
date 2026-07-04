package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_SQLHa_EnableDisableDescribe(t *testing.T) {
	t.Parallel()

	h := newHandler()

	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	enableVals := url.Values{}
	enableVals.Set("Action", "EnableInstanceSqlHaStandbyDetections")
	enableVals.Set("Version", "2016-11-15")
	enableVals.Set("InstanceId.1", instanceID)

	enableRec := postForm(t, h, enableVals.Encode())
	require.Equal(t, http.StatusOK, enableRec.Code)
	body := enableRec.Body.String()
	assert.Contains(t, body, "<EnableInstanceSqlHaStandbyDetectionsResponse")
	assert.Contains(t, body, "<haStatus>processing</haStatus>")

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeInstanceSqlHaStates")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("InstanceId.1", instanceID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "<haStatus>active</haStatus>")

	disableVals := url.Values{}
	disableVals.Set("Action", "DisableInstanceSqlHaStandbyDetections")
	disableVals.Set("Version", "2016-11-15")
	disableVals.Set("InstanceId.1", instanceID)

	disableRec := postForm(t, h, disableVals.Encode())
	require.Equal(t, http.StatusOK, disableRec.Code)
	assert.Contains(t, disableRec.Body.String(), "<DisableInstanceSqlHaStandbyDetectionsResponse")
}

func TestHandler_DescribeInstanceSqlHaHistoryStates(t *testing.T) {
	t.Parallel()

	h := newHandler()

	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	enableVals := url.Values{}
	enableVals.Set("Action", "EnableInstanceSqlHaStandbyDetections")
	enableVals.Set("Version", "2016-11-15")
	enableVals.Set("InstanceId.1", instanceID)
	postForm(t, h, enableVals.Encode())

	historyVals := url.Values{}
	historyVals.Set("Action", "DescribeInstanceSqlHaHistoryStates")
	historyVals.Set("Version", "2016-11-15")
	historyVals.Set("InstanceId.1", instanceID)

	rec := postForm(t, h, historyVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<DescribeInstanceSqlHaHistoryStatesResponse")
}

func TestHandler_EnableInstanceSqlHaStandbyDetections_UnknownInstanceFails(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "EnableInstanceSqlHaStandbyDetections")
	vals.Set("Version", "2016-11-15")
	vals.Set("InstanceId.1", "i-doesnotexist")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>InvalidInstanceID.NotFound</Code>")
}
