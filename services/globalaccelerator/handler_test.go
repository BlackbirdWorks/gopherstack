package globalaccelerator_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/globalaccelerator"
)

func newRawHandler() (*globalaccelerator.Handler, *echo.Echo) {
	h := globalaccelerator.NewHandler(globalaccelerator.NewInMemoryBackend(testAccount, "us-west-2"))
	e := echo.New()
	e.POST("/", h.Handler())

	return h, e
}

func rawCall(e *echo.Echo, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	return rec
}

func TestHandler_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		target string
		body   string
		code   string
		status int
	}{
		{
			"unknown_operation",
			"GlobalAccelerator_V20180706.Frobnicate",
			`{}`,
			"InvalidArgumentException",
			http.StatusBadRequest,
		},
		{
			"malformed_json",
			"GlobalAccelerator_V20180706.CreateAccelerator",
			`{"Name":`,
			"InvalidArgumentException",
			http.StatusBadRequest,
		},
		{
			"wrong_type",
			"GlobalAccelerator_V20180706.CreateAccelerator",
			`{"Name":5}`,
			"InvalidArgumentException",
			http.StatusBadRequest,
		},
		{
			"missing_accelerator",
			"GlobalAccelerator_V20180706.DescribeAccelerator",
			`{"AcceleratorArn":"x"}`,
			"AcceleratorNotFoundException",
			http.StatusBadRequest,
		},
		{
			"missing_listener",
			"GlobalAccelerator_V20180706.DescribeListener",
			`{}`,
			"ListenerNotFoundException",
			http.StatusBadRequest,
		},
		{
			"missing_group",
			"GlobalAccelerator_V20180706.DescribeEndpointGroup",
			`{}`,
			"EndpointGroupNotFoundException",
			http.StatusBadRequest,
		},
		{
			"missing_attachment",
			"GlobalAccelerator_V20180706.DescribeCrossAccountAttachment",
			`{}`,
			"AttachmentNotFoundException",
			http.StatusBadRequest,
		},
		{
			"bad_token",
			"GlobalAccelerator_V20180706.ListAccelerators",
			`{"NextToken":"@@"}`,
			"InvalidNextTokenException",
			http.StatusBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, e := newRawHandler()
			rec := rawCall(e, tc.target, tc.body)

			assert.Equal(t, tc.status, rec.Code)
			assert.Equal(t, "application/x-amz-json-1.1", rec.Header().Get("Content-Type"))
			assert.NotEmpty(t, rec.Header().Get("X-Amz-Crc32"))

			var got map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			assert.Equal(t, tc.code, got["__type"])
			assert.NotEmpty(t, got["message"])
			assert.Equal(t, got["message"], got["Message"])
		})
	}
}

func TestHandler_EmptyBodyAndResponseShapes(t *testing.T) {
	t.Parallel()

	_, e := newRawHandler()

	rec := rawCall(e, "GlobalAccelerator_V20180706.ListAccelerators", ``)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"Accelerators":[]}`, rec.Body.String())

	rec = rawCall(e, "GlobalAccelerator_V20180706.ListCrossAccountResourceAccounts", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"ResourceOwnerAwsAccountIds":[]}`, rec.Body.String())

	rec = rawCall(e, "GlobalAccelerator_V20180706.CreateAccelerator", `{"Name":"raw","IpAddressType":"IPV4"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Accelerator map[string]any `json:"Accelerator"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.IsType(t, float64(0), out.Accelerator["CreatedTime"], "timestamps are epoch-seconds numbers")
	assert.IsType(t, float64(0), out.Accelerator["LastModifiedTime"])
	assert.Equal(t, true, out.Accelerator["Enabled"])
	assert.NotContains(t, out.Accelerator, "DualStackDnsName")
	assert.NotContains(t, out.Accelerator, "Events")
}

func TestHandler_RoutingAndMetadata(t *testing.T) {
	t.Parallel()

	h, e := newRawHandler()
	assert.Equal(t, "GlobalAccelerator", h.Name())
	assert.Equal(t, "globalaccelerator", h.ChaosServiceName())
	assert.Equal(t, []string{"us-west-2"}, h.ChaosRegions())
	assert.Len(t, h.GetSupportedOperations(), 56)
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("X-Amz-Target", "GlobalAccelerator_V20180706.ListByoipCidrs")

	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, h.RouteMatcher()(c))
	assert.Equal(t, "ListByoipCidrs", h.ExtractOperation(c))
	assert.Equal(t, "ListByoipCidrs", h.ExtractResource(c))

	other := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	other.Header.Set("X-Amz-Target", "AWSShield_20160616.ListProtections")
	assert.False(t, h.RouteMatcher()(e.NewContext(other, httptest.NewRecorder())))
}

func TestErrorTable_CoversEverySentinel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		err  error
		code string
	}{
		{globalaccelerator.ErrAcceleratorNotDisabled, "AcceleratorNotDisabledException"},
		{globalaccelerator.ErrAcceleratorNotFound, "AcceleratorNotFoundException"},
		{globalaccelerator.ErrAssociatedEndpointGroupFound, "AssociatedEndpointGroupFoundException"},
		{globalaccelerator.ErrAssociatedListenerFound, "AssociatedListenerFoundException"},
		{globalaccelerator.ErrAttachmentNotFound, "AttachmentNotFoundException"},
		{globalaccelerator.ErrByoipCidrNotFound, "ByoipCidrNotFoundException"},
		{globalaccelerator.ErrConflict, "ConflictException"},
		{globalaccelerator.ErrEndpointAlreadyExists, "EndpointAlreadyExistsException"},
		{globalaccelerator.ErrEndpointGroupAlreadyExists, "EndpointGroupAlreadyExistsException"},
		{globalaccelerator.ErrEndpointGroupNotFound, "EndpointGroupNotFoundException"},
		{globalaccelerator.ErrEndpointNotFound, "EndpointNotFoundException"},
		{globalaccelerator.ErrIncorrectCidrState, "IncorrectCidrStateException"},
		{globalaccelerator.ErrInternalServiceError, "InternalServiceErrorException"},
		{globalaccelerator.ErrInvalidArgument, "InvalidArgumentException"},
		{globalaccelerator.ErrInvalidNextToken, "InvalidNextTokenException"},
		{globalaccelerator.ErrInvalidPortRange, "InvalidPortRangeException"},
		{globalaccelerator.ErrLimitExceeded, "LimitExceededException"},
		{globalaccelerator.ErrListenerNotFound, "ListenerNotFoundException"},
	}

	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(httptest.NewRequest(http.MethodPost, "/", nil), rec)

			require.NoError(t, globalaccelerator.WriteErrorForTest(c, tc.err))

			var got map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			assert.Equal(t, tc.code, got["__type"])
		})
	}
}
