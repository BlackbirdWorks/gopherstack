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

// Test_GetWirelessDevice_ReflectsThingAssociation verifies
// AssociateWirelessDeviceWithThing / DisassociateWirelessDeviceFromThing are
// real state mutations reflected by GetWirelessDevice's ThingArn/ThingName —
// not a disguised no-op. Real AWS derives ThingName from the last path
// segment of ThingArn (arn:...:thing/<name>) since the association request
// never carries ThingName directly.
func Test_GetWirelessDevice_ReflectsThingAssociation(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	createRec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices",
		`{"Name":"dev1","Type":"LoRaWAN","DestinationName":"d"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	id := created["Id"].(string)

	// Before association, ThingArn/ThingName must be absent.
	getRec := doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var before map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &before))
	assert.Empty(t, before["ThingArn"])
	assert.Empty(t, before["ThingName"])

	thingArn := "arn:aws:iot:us-east-1:000000000000:thing/my-thing"
	assocRec := doIoTWRequest(t, h, http.MethodPut, "/wireless-devices/"+id+"/thing",
		`{"ThingArn":"`+thingArn+`"}`)
	require.Equal(t, http.StatusNoContent, assocRec.Code)

	getRec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var after map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &after))
	assert.Equal(t, thingArn, after["ThingArn"])
	assert.Equal(t, "my-thing", after["ThingName"])

	disassocRec := doIoTWRequest(t, h, http.MethodDelete, "/wireless-devices/"+id+"/thing", "")
	require.Equal(t, http.StatusNoContent, disassocRec.Code)

	getRec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var afterDisassoc map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &afterDisassoc))
	assert.Empty(t, afterDisassoc["ThingArn"])
	assert.Empty(t, afterDisassoc["ThingName"])
}

// Test_GetWirelessGateway_ReflectsThingAssociation is the WirelessGateway
// analogue of Test_GetWirelessDevice_ReflectsThingAssociation.
func Test_GetWirelessGateway_ReflectsThingAssociation(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	createRec := doIoTWRequest(t, h, http.MethodPost, "/wireless-gateways", `{"Name":"gw1"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	id := created["Id"].(string)

	thingArn := "arn:aws:iot:us-east-1:000000000000:thing/gw-thing"
	assocRec := doIoTWRequest(t, h, http.MethodPut, "/wireless-gateways/"+id+"/thing",
		`{"ThingArn":"`+thingArn+`"}`)
	require.Equal(t, http.StatusNoContent, assocRec.Code)

	getRec := doIoTWRequest(t, h, http.MethodGet, "/wireless-gateways/"+id, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var after map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &after))
	assert.Equal(t, thingArn, after["ThingArn"])
	assert.Equal(t, "gw-thing", after["ThingName"])
}
