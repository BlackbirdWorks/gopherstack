package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ScheduledInstances_DescribeAvailabilityPurchaseRun(t *testing.T) {
	t.Parallel()

	h := newHandler()

	availVals := url.Values{}
	availVals.Set("Action", "DescribeScheduledInstanceAvailability")
	availVals.Set("Version", "2016-11-15")

	availRec := postForm(t, h, availVals.Encode())
	require.Equal(t, http.StatusOK, availRec.Code)
	availBody := availRec.Body.String()
	assert.Contains(t, availBody, "<DescribeScheduledInstanceAvailabilityResponse")

	token := extractXMLValue(t, availBody, "purchaseToken")
	require.NotEmpty(t, token)

	purchaseVals := url.Values{}
	purchaseVals.Set("Action", "PurchaseScheduledInstances")
	purchaseVals.Set("Version", "2016-11-15")
	purchaseVals.Set("PurchaseRequest.1.PurchaseToken", token)
	purchaseVals.Set("PurchaseRequest.1.InstanceCount", "1")

	purchaseRec := postForm(t, h, purchaseVals.Encode())
	require.Equal(t, http.StatusOK, purchaseRec.Code)
	purchaseBody := purchaseRec.Body.String()
	assert.Contains(t, purchaseBody, "<PurchaseScheduledInstancesResponse")

	sciID := extractXMLValue(t, purchaseBody, "scheduledInstanceId")
	require.NotEmpty(t, sciID)

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeScheduledInstances")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("ScheduledInstanceId.1", sciID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), sciID)

	runVals := url.Values{}
	runVals.Set("Action", "RunScheduledInstances")
	runVals.Set("Version", "2016-11-15")
	runVals.Set("ScheduledInstanceId", sciID)
	runVals.Set("InstanceCount", "1")
	runVals.Set("LaunchSpecification.ImageId", "ami-12345")

	runRec := postForm(t, h, runVals.Encode())
	require.Equal(t, http.StatusOK, runRec.Code)
	runBody := runRec.Body.String()
	assert.Contains(t, runBody, "<RunScheduledInstancesResponse")
	assert.Contains(t, runBody, "<instanceId>i-")
}

func TestHandler_ScheduledInstances_PurchaseInvalidTokenFails(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "PurchaseScheduledInstances")
	vals.Set("Version", "2016-11-15")
	vals.Set("PurchaseRequest.1.PurchaseToken", "not-a-real-token")
	vals.Set("PurchaseRequest.1.InstanceCount", "1")

	rec := postForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Errors>")
}
