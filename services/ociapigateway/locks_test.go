package ociapigateway_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const apiBase = "/20190501"

func idOf(t *testing.T, body string) string {
	t.Helper()

	var out struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal([]byte(body), &out), body)
	require.NotEmpty(t, out.ID, body)

	return out.ID
}

func (e *env) post(t *testing.T, path, body string, want int) string {
	t.Helper()

	status, out, _ := rawDo(t, http.MethodPost, e.base+apiBase+path, body)
	require.Equal(t, want, status, out)

	return out
}

// lockables creates one resource of every lockable kind and returns kind -> resource path.
func (e *env) lockables(t *testing.T) map[string]string {
	t.Helper()

	gw := e.mkGateway(t, "lock-gw")
	dep := e.mkDeployment(t, *gw.Id, "/l", stockSpec(200, "x"))
	certPEM, keyPEM := selfSigned(t, "lock.example.test")

	cert, err := e.api.CreateCertificate(t.Context(), apigateway.CreateCertificateRequest{
		CompartmentId: new(testCompartment), Certificate: &certPEM, PrivateKey: &keyPEM, DisplayName: new("lock-cert"),
	})
	require.NoError(t, err)

	comp := `"compartmentId":"` + testCompartment + `"`
	apiID := idOf(t, e.post(t, "/apis", `{`+comp+`,"displayName":"lock-api"}`, http.StatusAccepted))
	sdkID := idOf(t, e.post(t, "/sdks", `{"apiId":"`+apiID+`","targetLanguage":"JAVA","displayName":"lock-sdk"}`, http.StatusAccepted))
	planID := idOf(t, e.post(t, "/usagePlans",
		`{`+comp+`,"displayName":"lock-plan","entitlements":[{"name":"e","targets":[{"deploymentId":"`+dep+`"}]}]}`,
		http.StatusAccepted))
	subID := idOf(t, e.post(t, "/subscribers",
		`{`+comp+`,"displayName":"lock-sub","clients":[{"name":"c","token":"tok"}],"usagePlans":["`+planID+`"]}`,
		http.StatusAccepted))

	e.settle()

	return map[string]string{
		"gateways": "/gateways/" + *gw.Id, "deployments": "/deployments/" + dep, "certificates": "/certificates/" + *cert.Id,
		"apis": "/apis/" + apiID, "sdks": "/sdks/" + sdkID, "usagePlans": "/usagePlans/" + planID,
		"subscribers": "/subscribers/" + subID,
	}
}

func TestResourceLocksBlockDeletion(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	res := e.lockables(t)

	for kind, path := range res {
		t.Run(kind, func(t *testing.T) {
			url := e.base + apiBase + path

			body := e.post(t, path+"/actions/addLock", `{"type":"DELETE","message":"keep"}`, http.StatusOK)
			assert.Contains(t, body, `"type":"DELETE"`)
			assert.Contains(t, body, `"message":"keep"`)

			status, out, _ := rawDo(t, http.MethodDelete, url, "")
			assert.Equal(t, http.StatusConflict, status, out)

			e.post(t, path+"/actions/removeLock", `{"type":"FULL"}`, http.StatusNotFound)

			body = e.post(t, path+"/actions/removeLock", `{"type":"DELETE"}`, http.StatusOK)
			assert.NotContains(t, body, `"type":"DELETE"`)

			want := http.StatusAccepted
			if kind == "gateways" || kind == "usagePlans" {
				want = http.StatusConflict // still referenced by a deployment or a subscriber
			}

			status, out, _ = rawDo(t, http.MethodDelete, url, "")
			assert.Equal(t, want, status, out)
		})
	}
}

func TestFullLockBlocksUpdateViaSDK(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "full-lock")

	added, err := e.gw.AddGatewayLock(t.Context(), apigateway.AddGatewayLockRequest{
		GatewayId: gw.Id,
		AddResourceLockDetails: apigateway.AddResourceLockDetails{
			Type: apigateway.AddResourceLockDetailsTypeFull, Message: new("frozen"),
		},
	})
	require.NoError(t, err)
	require.Len(t, added.Locks, 1)
	assert.Equal(t, apigateway.ResourceLockTypeFull, added.Locks[0].Type)
	assert.NotEqual(t, *gw.Id+"x", *added.Etag)

	_, err = e.gw.UpdateGateway(t.Context(), apigateway.UpdateGatewayRequest{
		GatewayId: gw.Id, UpdateGatewayDetails: apigateway.UpdateGatewayDetails{DisplayName: new("renamed")},
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.gw.RemoveGatewayLock(t.Context(), apigateway.RemoveGatewayLockRequest{
		GatewayId: gw.Id,
		RemoveResourceLockDetails: apigateway.RemoveResourceLockDetails{
			Type: apigateway.RemoveResourceLockDetailsTypeFull,
		},
	})
	require.NoError(t, err)

	_, err = e.gw.UpdateGateway(t.Context(), apigateway.UpdateGatewayRequest{
		GatewayId: gw.Id, UpdateGatewayDetails: apigateway.UpdateGatewayDetails{DisplayName: new("renamed")},
	})
	require.NoError(t, err)
}

func TestLocksAtCreateAreEnforced(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	created, err := e.gw.CreateGateway(t.Context(), apigateway.CreateGatewayRequest{
		CreateGatewayDetails: apigateway.CreateGatewayDetails{
			CompartmentId: new(testCompartment), EndpointType: apigateway.GatewayEndpointTypePublic,
			SubnetId: new(testSubnet), DisplayName: new("born-locked"),
			Locks: []apigateway.AddResourceLockDetails{{Type: apigateway.AddResourceLockDetailsTypeDelete}},
		},
	})
	require.NoError(t, err)
	require.Len(t, created.Locks, 1)
	assert.NotNil(t, created.Locks[0].TimeCreated)
	e.settle()

	_, err = e.gw.DeleteGateway(t.Context(), apigateway.DeleteGatewayRequest{GatewayId: created.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
}

func TestCancelWorkRequest(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	created := e.createGateway(t, "cancel")

	_, err := e.wr.CancelWorkRequest(t.Context(), apigateway.CancelWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)

	got, err := e.wr.GetWorkRequest(t.Context(), apigateway.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, apigateway.WorkRequestStatusCanceled, got.Status)

	_, err = e.wr.CancelWorkRequest(t.Context(), apigateway.CancelWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "a finished work request cannot be canceled again")

	_, err = e.wr.CancelWorkRequest(t.Context(), apigateway.CancelWorkRequestRequest{WorkRequestId: new("ocid1.none")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	e.settle()

	done := e.createGateway(t, "cancel-done")
	e.settle()

	_, err = e.wr.CancelWorkRequest(t.Context(), apigateway.CancelWorkRequestRequest{WorkRequestId: done.OpcWorkRequestId})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
}
