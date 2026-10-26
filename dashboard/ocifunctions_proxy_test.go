package dashboard_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ocifunctions"
)

const ociFnRoot = "/dashboard/api/v1/oci/functions"

type ociFnSeed struct {
	h     *dashboard.DashboardHandler
	appID string
	fnID  string
}

func ociFnCall(
	t *testing.T,
	h *ocifunctions.Handler,
	method, path, body string,
) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)

	return rec, out
}

func seedOCIFunctions(t *testing.T) ociFnSeed {
	t.Helper()

	fh := ocifunctions.NewHandler(ocifunctions.NewInMemoryBackend())
	fh.Backend.SetTransition(0)

	rec, app := ociFnCall(t, fh, http.MethodPost, "/20181201/applications",
		`{"compartmentId":"`+ociComp+`","displayName":"app-one","subnetIds":["ocid1.subnet.oc1..s"],`+
			`"config":{"LOG_LEVEL":"info","DB_PASSWORD":"hunter2","API_KEY":"k-123"}}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	appID, _ := app["id"].(string)

	rec, fn := ociFnCall(t, fh, http.MethodPost, "/20181201/functions",
		`{"applicationId":"`+appID+`","displayName":"fn-one","memoryInMBs":256,`+
			`"sourceDetails":{"sourceType":"CONTAINER_IMAGE","image":"iad.ocir.io/ns/img:1"},`+
			`"config":{"GREETING":"hello","AUTH_TOKEN":"tok-999"}}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	fnID, _ := fn["id"].(string)

	fh.Backend.Sweep()

	return ociFnSeed{h: dashboard.NewHandler(dashboard.Config{OCIFunctionsOps: fh}), appID: appID, fnID: fnID}
}

func TestOCIFunctionsProxy(t *testing.T) {
	t.Parallel()

	t.Run("browse", func(t *testing.T) {
		t.Parallel()

		s := seedOCIFunctions(t)
		runGCPSteps(t, s.h, []gcpStep{
			{http.MethodGet, ociFnRoot + "/applications", "", `"displayName":"app-one"`, 200},
			{http.MethodGet, ociFnRoot + "/applications/" + s.appID, "", `"LOG_LEVEL":"info"`, 200},
			{http.MethodGet, ociFnRoot + "/applications/" + s.appID + "/functions", "", `"displayName":"fn-one"`, 200},
			{http.MethodGet, ociFnRoot + "/functions/" + s.fnID, "", `"GREETING":"hello"`, 200},
			{http.MethodGet, ociFnRoot + "/functions/" + s.fnID, "", `"invokeEndpoint":"http://example.com:8154"`, 200},
			{http.MethodGet, ociFnRoot + "/functions/" + s.fnID, "", `"memoryInMBs":256`, 200},
			{http.MethodGet, ociFnRoot + "/workrequests", "", `"operationType":"CREATE_FUNCTION"`, 200},
			{http.MethodGet, ociFnRoot + "/workrequests?resourceId=" + s.fnID, "", `"workRequests":[{`, 200},
		})
	})

	t.Run("secret looking config is masked", func(t *testing.T) {
		t.Parallel()

		s := seedOCIFunctions(t)

		for _, p := range []string{"/applications/" + s.appID, "/functions/" + s.fnID} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, ociFnRoot+p, nil)
			require.NoError(t, s.h.Handler()(echo.NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code, p)

			for _, secret := range []string{"hunter2", "k-123", "tok-999"} {
				assert.NotContains(t, rec.Body.String(), secret, p)
			}

			assert.Contains(t, rec.Body.String(), "maskedConfigKeys", p)
		}
	})

	t.Run("invoke", func(t *testing.T) {
		t.Parallel()

		s := seedOCIFunctions(t)
		runGCPSteps(t, s.h, []gcpStep{
			{
				http.MethodPost,
				ociFnRoot + "/functions/" + s.fnID + "/invoke",
				`{"body":"{}","dryRun":true}`,
				`"status":200`,
				200,
			},
			{
				http.MethodPost,
				ociFnRoot + "/functions/" + s.fnID + "/invoke",
				`{"body":"{}"}`,
				"function engine unavailable",
				503,
			},
		})
	})

	t.Run("rejects", func(t *testing.T) {
		t.Parallel()

		s := seedOCIFunctions(t)
		runGCPSteps(t, s.h, []gcpStep{
			{http.MethodGet, ociFnRoot + "/applications/nope", "", "NotAuthorizedOrNotFound", 404},
			{http.MethodGet, ociFnRoot + "/applications/nope/functions", "", "NotAuthorizedOrNotFound", 404},
			{http.MethodGet, ociFnRoot + "/functions/nope", "", "NotAuthorizedOrNotFound", 404},
			{http.MethodPost, ociFnRoot + "/functions/nope/invoke", `{}`, "NotAuthorizedOrNotFound", 404},
			{http.MethodPost, ociFnRoot + "/functions/" + s.fnID + "/invoke", `not json`, "invalid request body", 400},
			{http.MethodGet, ociFnRoot + "/workrequests/nope/logs", "", "NotAuthorizedOrNotFound", 404},
		})
	})

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, ociFnRoot + "/applications", "", "UNAVAILABLE", http.StatusServiceUnavailable},
			{http.MethodPost, ociFnRoot + "/functions/x/invoke", `{}`, "UNAVAILABLE", http.StatusServiceUnavailable},
			{http.MethodGet, ociFnRoot + "/workrequests", "", "UNAVAILABLE", http.StatusServiceUnavailable},
		})
	})
}
