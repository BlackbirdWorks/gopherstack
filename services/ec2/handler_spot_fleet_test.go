package ec2_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// newEC2Handler creates a fresh EC2 handler for handler-level tests.
func newEC2SpotFleetHandler() *ec2.Handler {
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.Region = "us-east-1"
	h.AccountID = "123456789012"

	return h
}

// spotFleetForm sends a form request to the EC2 handler.
func spotFleetForm(t *testing.T, h *ec2.Handler, form string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func minimalSpotFleetForm() string {
	vals := url.Values{}
	vals.Set("Version", "2016-11-15")
	vals.Set("Action", "RequestSpotFleet")
	vals.Set("SpotFleetRequestConfig.TargetCapacity", "2")
	vals.Set("SpotFleetRequestConfig.IamFleetRole", "arn:aws:iam::123456789012:role/fleet-role")
	vals.Set("SpotFleetRequestConfig.LaunchSpecifications.1.ImageId", "ami-12345678")
	vals.Set("SpotFleetRequestConfig.LaunchSpecifications.1.InstanceType", "m5.large")

	return vals.Encode()
}

// ---- RequestSpotFleet ----

func TestHandlerRequestSpotFleet(t *testing.T) {
	t.Parallel()
	h := newEC2SpotFleetHandler()
	rec := spotFleetForm(t, h, minimalSpotFleetForm())

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName            xml.Name `xml:"RequestSpotFleetResponse"`
		SpotFleetRequestID string   `xml:"spotFleetRequestId"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.SpotFleetRequestID)
	assert.True(t, strings.HasPrefix(resp.SpotFleetRequestID, "sfr-"))
}

func TestHandlerRequestSpotFleet_NoLaunchSpecs(t *testing.T) {
	t.Parallel()
	h := newEC2SpotFleetHandler()
	vals := url.Values{}
	vals.Set("Version", "2016-11-15")
	vals.Set("Action", "RequestSpotFleet")
	vals.Set("SpotFleetRequestConfig.TargetCapacity", "2")

	rec := spotFleetForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- DescribeSpotFleetRequests ----

func TestHandlerDescribeSpotFleetRequests_Empty(t *testing.T) {
	t.Parallel()
	h := newEC2SpotFleetHandler()
	vals := url.Values{}
	vals.Set("Version", "2016-11-15")
	vals.Set("Action", "DescribeSpotFleetRequests")

	rec := spotFleetForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName                   xml.Name `xml:"DescribeSpotFleetRequestsResponse"`
		SpotFleetRequestConfigSet struct {
			Items []struct{} `xml:"item"`
		} `xml:"spotFleetRequestConfigSet"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.SpotFleetRequestConfigSet.Items)
}

func TestHandlerDescribeSpotFleetRequests_AfterCreate(t *testing.T) {
	t.Parallel()
	h := newEC2SpotFleetHandler()

	// Create a fleet.
	createRec := spotFleetForm(t, h, minimalSpotFleetForm())
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		XMLName            xml.Name `xml:"RequestSpotFleetResponse"`
		SpotFleetRequestID string   `xml:"spotFleetRequestId"`
	}
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	// Describe.
	vals := url.Values{}
	vals.Set("Version", "2016-11-15")
	vals.Set("Action", "DescribeSpotFleetRequests")
	vals.Set("SpotFleetRequestId.1", createResp.SpotFleetRequestID)

	rec := spotFleetForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		SpotFleetRequestConfigSet struct {
			Items []struct {
				SpotFleetRequestID string `xml:"spotFleetRequestId"`
			} `xml:"item"`
		} `xml:"spotFleetRequestConfigSet"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Len(t, descResp.SpotFleetRequestConfigSet.Items, 1)
	assert.Equal(
		t,
		createResp.SpotFleetRequestID,
		descResp.SpotFleetRequestConfigSet.Items[0].SpotFleetRequestID,
	)
}

// ---- CancelSpotFleetRequests ----

func TestHandlerCancelSpotFleetRequests(t *testing.T) {
	t.Parallel()
	h := newEC2SpotFleetHandler()

	createRec := spotFleetForm(t, h, minimalSpotFleetForm())
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		SpotFleetRequestID string `xml:"spotFleetRequestId"`
	}
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	vals := url.Values{}
	vals.Set("Version", "2016-11-15")
	vals.Set("Action", "CancelSpotFleetRequests")
	vals.Set("SpotFleetRequestId.1", createResp.SpotFleetRequestID)
	vals.Set("TerminateInstances", "false")

	rec := spotFleetForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- ModifySpotFleetRequest ----

func TestHandlerModifySpotFleetRequest(t *testing.T) {
	t.Parallel()
	h := newEC2SpotFleetHandler()

	createRec := spotFleetForm(t, h, minimalSpotFleetForm())
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		SpotFleetRequestID string `xml:"spotFleetRequestId"`
	}
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	vals := url.Values{}
	vals.Set("Version", "2016-11-15")
	vals.Set("Action", "ModifySpotFleetRequest")
	vals.Set("SpotFleetRequestId", createResp.SpotFleetRequestID)
	vals.Set("TargetCapacity", "5")

	rec := spotFleetForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Return bool `xml:"return"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Return)
}

// ---- DescribeSpotFleetInstances ----

func TestHandlerDescribeSpotFleetInstances(t *testing.T) {
	t.Parallel()
	h := newEC2SpotFleetHandler()

	createRec := spotFleetForm(t, h, minimalSpotFleetForm())
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		SpotFleetRequestID string `xml:"spotFleetRequestId"`
	}
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	vals := url.Values{}
	vals.Set("Version", "2016-11-15")
	vals.Set("Action", "DescribeSpotFleetInstances")
	vals.Set("SpotFleetRequestId", createResp.SpotFleetRequestID)

	rec := spotFleetForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ActiveInstances struct {
			Items []struct {
				InstanceID string `xml:"instanceId"`
			} `xml:"item"`
		} `xml:"activeInstanceSet"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.ActiveInstances.Items, 2)
}

// ---- DescribeSpotFleetRequestHistory ----

func TestHandlerDescribeSpotFleetRequestHistory(t *testing.T) {
	t.Parallel()
	h := newEC2SpotFleetHandler()

	createRec := spotFleetForm(t, h, minimalSpotFleetForm())
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		SpotFleetRequestID string `xml:"spotFleetRequestId"`
	}
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	vals := url.Values{}
	vals.Set("Version", "2016-11-15")
	vals.Set("Action", "DescribeSpotFleetRequestHistory")
	vals.Set("SpotFleetRequestId", createResp.SpotFleetRequestID)
	vals.Set("StartTime", "2020-01-01T00:00:00Z")

	rec := spotFleetForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		HistoryRecords struct {
			Items []struct {
				EventType string `xml:"eventType"`
			} `xml:"item"`
		} `xml:"historyRecordSet"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.HistoryRecords.Items)
}
