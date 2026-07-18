package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func newTestHandlerHTTP() *iotwireless.Handler {
	bk := iotwireless.NewInMemoryBackend()
	h := iotwireless.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doIoTWRequest(t *testing.T, h *iotwireless.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	var req *http.Request

	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "unknown_base", path: "/unknown-resource"},
		{name: "root", path: "/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodGet, tt.path, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_Metadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkFunc func(*testing.T, *iotwireless.Handler)
		name      string
	}{
		{
			name: "name_is_IoTWireless",
			checkFunc: func(t *testing.T, h *iotwireless.Handler) {
				t.Helper()
				assert.Equal(t, "IoTWireless", h.Name())
			},
		},
		{
			name: "chaos_service_name_is_iotwireless",
			checkFunc: func(t *testing.T, h *iotwireless.Handler) {
				t.Helper()
				assert.Equal(t, "iotwireless", h.ChaosServiceName())
			},
		},
		{
			name: "supported_operations_not_empty",
			checkFunc: func(t *testing.T, h *iotwireless.Handler) {
				t.Helper()
				assert.NotEmpty(t, h.GetSupportedOperations())
			},
		},
		{
			name: "chaos_operations_matches_supported",
			checkFunc: func(t *testing.T, h *iotwireless.Handler) {
				t.Helper()
				assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
			},
		},
		{
			name: "chaos_regions_contains_default_region",
			checkFunc: func(t *testing.T, h *iotwireless.Handler) {
				t.Helper()
				assert.Equal(t, []string{testRegion}, h.ChaosRegions())
			},
		},
		{
			name: "match_priority_is_86",
			checkFunc: func(t *testing.T, h *iotwireless.Handler) {
				t.Helper()
				assert.Equal(t, 86, h.MatchPriority())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			tt.checkFunc(t, h)
		})
	}
}

func TestHandler_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{
			name:       "get_wireless_gateway_not_found",
			method:     http.MethodGet,
			path:       "/wireless-gateways/no-such-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_wireless_gateway_not_found",
			method:     http.MethodDelete,
			path:       "/wireless-gateways/no-such-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_service_profile_not_found",
			method:     http.MethodDelete,
			path:       "/service-profiles/no-such-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_wireless_device_not_found",
			method:     http.MethodDelete,
			path:       "/wireless-devices/no-such-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_destination_not_found",
			method:     http.MethodDelete,
			path:       "/destinations/no-such-name",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "unsupported_method_on_collection",
			method:     http.MethodPut,
			path:       "/service-profiles",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Populate a device so the snapshot is non-trivial.
	body := `{"Name":"persist-dev","Type":"LoRaWAN","DestinationName":"d","Description":"desc"}`
	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Snapshot via the handler.
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap, "Snapshot must return non-nil data")

	// Fresh handler with the same backend type.
	bk2 := iotwireless.NewInMemoryBackend()
	h2 := iotwireless.NewHandler(bk2)
	h2.AccountID = testAccountID
	h2.DefaultRegion = testRegion

	// Restore into h2.
	require.NoError(t, h2.Restore(t.Context(), snap))

	// The device must be visible through h2.
	devices := bk2.ListWirelessDevices(testAccountID, testRegion)
	assert.Len(t, devices, 1)
	assert.Equal(t, "persist-dev", devices[0].Name)
}

func TestHandler_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()
	ops := h.GetSupportedOperations()

	newOps := []string{
		"AssociateAwsAccountWithPartnerAccount",
		"AssociateMulticastGroupWithFuotaTask",
		"AssociateWirelessDeviceWithFuotaTask",
		"AssociateWirelessDeviceWithMulticastGroup",
		"AssociateWirelessDeviceWithThing",
		"AssociateWirelessGatewayWithCertificate",
		"AssociateWirelessGatewayWithThing",
		"CancelMulticastGroupSession",
		"CreateDeviceProfile",
		"CreateFuotaTask",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op, "GetSupportedOperations should contain %q", op)
	}
}

// TestHandler_NotFoundErrors verifies that not-found errors return 404.
func TestHandler_NotFoundErrors(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	tests := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/multicast-groups/nonexistent", ""},
		{http.MethodDelete, "/multicast-groups/nonexistent", ""},
		{http.MethodPatch, "/multicast-groups/nonexistent", `{"Name":"x"}`},
		{http.MethodGet, "/network-analyzer-configurations/nonexistent", ""},
		{http.MethodDelete, "/network-analyzer-configurations/nonexistent", ""},
		{http.MethodGet, "/fuota-tasks/nonexistent", ""},
		{http.MethodPatch, "/fuota-tasks/nonexistent", `{"Name":"x"}`},
		{http.MethodPut, "/fuota-tasks/nonexistent", `{}`},
		{http.MethodGet, "/wireless-devices/nonexistent", ""},
		{http.MethodDelete, "/wireless-devices/nonexistent", ""},
	}

	for _, tt := range tests {
		t.Run(tt.method+"_"+tt.path, func(t *testing.T) {
			t.Parallel()
			rec := doIoTWRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestHandler_BackendReset verifies that Reset() clears all new backend state.
func TestHandler_BackendReset(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()

	// Add some data
	_, err := bk.CreateMulticastGroup(testAccountID, testRegion, "mg1", "", nil)
	require.NoError(t, err)

	err = bk.PutResourceLogLevel("res1", "DEBUG")
	require.NoError(t, err)

	_, err = bk.CreateWirelessGatewayTaskDefinition("000000000000", "us-east-1", "def1", false)
	require.NoError(t, err)

	// Reset
	bk.Reset()

	// Verify cleared
	groups := bk.ListMulticastGroups(testAccountID, testRegion)
	assert.Empty(t, groups)

	level := bk.GetResourceLogLevel("res1")
	assert.Equal(t, "INFO", level)

	taskDefs := bk.ListWirelessGatewayTaskDefinitions()
	assert.Empty(t, taskDefs)
}

// TestHandler_Reset verifies that Handler.Reset() delegates to the backend.
func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices",
		`{"Name":"dev1","Type":"LoRaWAN","DestinationName":"d"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	h.Reset()

	rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	list, ok := resp["WirelessDeviceList"].([]any)
	require.True(t, ok)
	assert.Empty(t, list)
}

// TestHandler_GetSupportedOperations_AllOps verifies that all expected ops are included.
func TestHandler_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()
	ops := h.GetSupportedOperations()

	expected := []string{
		"GetDeviceProfile",
		"ListDeviceProfiles",
		"DeleteDeviceProfile",
		"GetFuotaTask",
		"ListFuotaTasks",
		"DeleteFuotaTask",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "GetSupportedOperations should contain %q", op)
	}
}

// TestHandler_HandleError_NotFound verifies that handleError maps not-found errors to 404.
func TestHandler_HandleError_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "device_profile_not_found",
			path:       "/device-profiles/no-such-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "fuota_task_not_found",
			path:       "/fuota-tasks/no-such-id",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodGet, tt.path, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListEmpty_NonNil verifies that HTTP list responses return an empty array, not null.
func TestHandler_ListEmpty_NonNil(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		listKey string
	}{
		{name: "device_profiles", path: "/device-profiles", listKey: "DeviceProfileList"},
		{name: "fuota_tasks", path: "/fuota-tasks", listKey: "FuotaTaskList"},
		{name: "wireless_devices", path: "/wireless-devices", listKey: "WirelessDeviceList"},
		{name: "wireless_gateways", path: "/wireless-gateways", listKey: "WirelessGatewayList"},
		{name: "service_profiles", path: "/service-profiles", listKey: "ServiceProfileList"},
		{name: "destinations", path: "/destinations", listKey: "DestinationList"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			list, ok := resp[tt.listKey].([]any)
			require.True(t, ok, "key %q should be a JSON array, got %T", tt.listKey, resp[tt.listKey])
			assert.Empty(t, list)
		})
	}
}

// sigV4AuthHeader carries an AWS SigV4 credential scope naming "iotwireless"
// as the service. RouteMatcher (via httputils.ExtractServiceFromRequest)
// requires this to disambiguate REST paths — like "/tags" — that other
// REST-JSON services could plausibly also expose.
const sigV4AuthHeader = "AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/20240101/us-east-1/iotwireless/aws4_request, " +
	"SignedHeaders=host, Signature=deadbeef"

// routeMatchRequest drives a request through the real RouteMatcher and then
// the Handler, the same path a live aws-sdk-go-v2 client takes. Unlike
// doIoTWRequest (which calls h.Handler()(c) directly), this catches ops that
// the handler's dispatch logic can serve correctly but that RouteMatcher or
// the path parser would never route a real SDK request to in the first
// place — exactly the bug class that made AssociateAwsAccountWithPartnerAccount
// and the /tags family unreachable before this fix.
func routeMatchRequest(
	t *testing.T, h *iotwireless.Handler, method, path, body string,
) (bool, *httptest.ResponseRecorder) {
	t.Helper()

	e := echo.New()

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	req.Header.Set("Authorization", sigV4AuthHeader)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if !h.RouteMatcher()(c) {
		return false, rec
	}

	require.NoError(t, h.Handler()(c))

	return true, rec
}

// Test_RouteMatcher_TagOperations verifies TagResource, ListTagsForResource,
// and UntagResource are reachable at their real wire path+method: bare
// "/tags" with the resource ARN as the "resourceArn" query parameter. A
// prior version of this handler expected the ARN as a path segment
// ("/tags/{arn}"), which no real aws-sdk-go-v2 client ever sends — the ops
// were unreachable despite unit tests (calling h.Handler()(c) directly)
// passing.
func Test_RouteMatcher_TagOperations(t *testing.T) {
	t.Parallel()

	h := iotwireless.NewHandler(iotwireless.NewInMemoryBackend())
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	matched, rec := routeMatchRequest(t, h, http.MethodPost, "/service-profiles", `{"Name":"tagme"}`)
	require.True(t, matched)
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	resourceArn := created["Arn"].(string)
	require.NotEmpty(t, resourceArn)

	encodedArn := strings.ReplaceAll(resourceArn, ":", "%3A")

	matched, rec = routeMatchRequest(t, h, http.MethodPost, "/tags?resourceArn="+encodedArn,
		`{"Tags":[{"Key":"env","Value":"prod"}]}`)
	require.True(t, matched, "POST /tags must be routed by RouteMatcher")
	assert.Equal(t, http.StatusNoContent, rec.Code)

	matched, rec = routeMatchRequest(t, h, http.MethodGet, "/tags?resourceArn="+encodedArn, "")
	require.True(t, matched, "GET /tags must be routed by RouteMatcher")
	require.Equal(t, http.StatusOK, rec.Code)

	var listed struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listed))
	require.Len(t, listed.Tags, 1)
	assert.Equal(t, "env", listed.Tags[0].Key)
	assert.Equal(t, "prod", listed.Tags[0].Value)

	matched, rec = routeMatchRequest(t, h, http.MethodDelete, "/tags?resourceArn="+encodedArn+"&tagKeys=env", "")
	require.True(t, matched, "DELETE /tags must be routed by RouteMatcher")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

// Test_RouteMatcher_AssociateAwsAccountWithPartnerAccount verifies the op is
// reachable at its real wire path+method: POST /partner-accounts (no path
// parameter — the partner account ID is Sidewalk.AmazonId in the body). A
// prior version of this handler only accepted PUT /partner-accounts/{id},
// which no real aws-sdk-go-v2 client ever sends.
func Test_RouteMatcher_AssociateAwsAccountWithPartnerAccount(t *testing.T) {
	t.Parallel()

	h := iotwireless.NewHandler(iotwireless.NewInMemoryBackend())
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	matched, rec := routeMatchRequest(t, h, http.MethodPost, "/partner-accounts",
		`{"Sidewalk":{"AmazonId":"partner-abc"}}`)
	require.True(t, matched, "POST /partner-accounts must be routed by RouteMatcher")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["Arn"])

	sidewalk, ok := resp["Sidewalk"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "partner-abc", sidewalk["AmazonId"])

	// The association must be a real state mutation, not a disguised no-op:
	// GetPartnerAccount for the same ID must now report AccountLinked.
	matched, rec = routeMatchRequest(t, h, http.MethodGet, "/partner-accounts/partner-abc", "")
	require.True(t, matched)
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, true, getResp["AccountLinked"])
}

// Test_ErrorResponse_SetsAmznErrortypeHeader verifies every error response
// carries the X-Amzn-Errortype header (and matching __type body field) the
// aws-sdk-go-v2 REST-JSON error deserializer needs to construct the correct
// typed *types.XxxException. Previously no header or __type field was ever
// set, so every error — 404s included — deserialized client-side into an
// untyped smithy.GenericAPIError{Code: "UnknownError"}, silently breaking
// any errors.As(err, &types.ResourceNotFoundException{}) style handling.
func Test_ErrorResponse_SetsAmznErrortypeHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		method      string
		path        string
		body        string
		wantErrType string
		wantStatus  int
	}{
		{
			name:        "not_found_maps_to_ResourceNotFoundException",
			method:      http.MethodGet,
			path:        "/wireless-devices/no-such-device",
			wantErrType: "ResourceNotFoundException",
			wantStatus:  http.StatusNotFound,
		},
		{
			name:        "bad_body_maps_to_ValidationException",
			method:      http.MethodPost,
			path:        "/tags",
			body:        "",
			wantErrType: "ValidationException",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, tt.method, tt.path, tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantErrType, rec.Header().Get("X-Amzn-Errortype"))

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, tt.wantErrType, body["__type"])
			assert.NotEmpty(t, body["Message"])
		})
	}
}
