package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dnsapi "google.golang.org/api/dns/v1"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
	"github.com/blackbirdworks/gopherstack/services/gcpdns"
	"github.com/blackbirdworks/gopherstack/services/gcpworkflows"
)

const workflowSource = "main:\n  params: [args]\n  steps:\n    - done:\n        return: ${\"hello \" + args.name}\n"

func TestGCPWorkflowsProxy(t *testing.T) {
	t.Parallel()

	const (
		loc = "/dashboard/api/v1/gcp/workflows/v1/projects/p/locations/us-central1"
		wf  = loc + "/workflows/hello"
	)

	seed := func(t *testing.T) *dashboard.DashboardHandler {
		t.Helper()

		w := gcpworkflows.NewHandler(gcpworkflows.NewInMemoryBackend())
		t.Cleanup(w.Backend.Close)

		_, err := w.Backend.CreateWorkflow(&workflowspb.CreateWorkflowRequest{
			Parent: "projects/p/locations/us-central1", WorkflowId: "hello",
			Workflow: &workflowspb.Workflow{
				SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: workflowSource},
			},
		})
		require.NoError(t, err)

		return dashboard.NewHandler(dashboard.Config{GCPWorkflowsOps: w})
	}

	t.Run("browse", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, seed(t), []gcpStep{
			{http.MethodGet, loc + "/workflows", "", `/workflows/hello"`, 200},
			{http.MethodGet, wf, "", `"sourceContents"`, 200},
			{http.MethodGet, wf + ":listRevisions", "", `"revisionId"`, 200},
			{http.MethodGet, wf + "/executions", "", "{}", 200},
		})
	})

	t.Run("execute and cancel", func(t *testing.T) {
		t.Parallel()

		h := seed(t)
		call := func(method, path, body string) (int, string) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(method, path, strings.NewReader(body))
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

			return rec.Code, rec.Body.String()
		}

		code, out := call(http.MethodPost, wf+"/executions", `{"argument":"{\"name\":\"gopher\"}"}`)
		require.Equal(t, http.StatusOK, code, out)
		assert.Contains(t, out, "/executions/")

		require.Eventually(t, func() bool {
			_, listed := call(http.MethodGet, wf+"/executions", "")

			return strings.Contains(listed, `"state":"SUCCEEDED"`) && strings.Contains(listed, "hello gopher")
		}, 30*time.Second, 10*time.Millisecond)

		var name string

		_, listed := call(http.MethodGet, wf+"/executions", "")
		if _, rest, ok := strings.Cut(listed, `"name":"`); ok {
			name, _, _ = strings.Cut(rest, `"`)
		}

		require.NotEmpty(t, name)

		code, out = call(http.MethodGet, "/dashboard/api/v1/gcp/workflows/v1/"+name, "")
		assert.Equal(t, http.StatusOK, code)
		assert.Contains(t, out, "hello gopher")

		code, out = call(http.MethodPost, "/dashboard/api/v1/gcp/workflows/v1/"+name+":cancel", "")
		assert.Equal(t, http.StatusBadRequest, code, out)
		assert.Contains(t, out, "FAILED_PRECONDITION")
	})

	t.Run("rejects", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, seed(t), []gcpStep{
			{http.MethodGet, loc + "/other", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, loc + "/workflows", "{}", "unsupported", http.StatusNotFound},
			{http.MethodDelete, wf, "", "unsupported", http.StatusNotFound},
			{http.MethodGet, wf + ":bogus", "", "unsupported", http.StatusNotFound},
			{http.MethodGet, wf + "/other", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, wf + "/executions", `{bad`, "invalid request body", http.StatusBadRequest},
			{http.MethodPost, wf + "/executions", `{"argument":"{"}`, "INVALID_ARGUMENT", http.StatusBadRequest},
			{http.MethodGet, loc + "/workflows/nope", "", "NOT_FOUND", http.StatusNotFound},
			{http.MethodGet, wf + "/executions/nope", "", "NOT_FOUND", http.StatusNotFound},
		})
	})

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, loc + "/workflows", "", "ServiceUnavailable", http.StatusServiceUnavailable},
		})
	})
}

func TestGCPDNSProxy(t *testing.T) {
	t.Parallel()

	const root = "/dashboard/api/v1/gcp/dns/v1/projects/p/managedZones"

	seed := func(t *testing.T) *dashboard.DashboardHandler {
		t.Helper()

		d := gcpdns.NewHandler(gcpdns.NewInMemoryBackend())
		_, err := d.Backend.CreateZone(
			"p",
			&dnsapi.ManagedZone{Name: "example", DnsName: "example.com.", Description: "demo"},
		)
		require.NoError(t, err)

		_, err = d.Backend.CreateRRSet("p", "example", &dnsapi.ResourceRecordSet{
			Name: "www.example.com.", Type: "A", Ttl: 300, Rrdatas: []string{"192.0.2.10"},
		})
		require.NoError(t, err)

		return dashboard.NewHandler(dashboard.Config{GCPDNSOps: d})
	}

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"browse", []gcpStep{
			{http.MethodGet, root, "", `"dnsName":"example.com."`, 200},
			{http.MethodGet, root + "/example", "", `"description":"demo"`, 200},
			{http.MethodGet, root + "/example/rrsets", "", "192.0.2.10", 200},
			{http.MethodGet, root + "/example/rrsets?name=www.example.com.&type=A", "", "192.0.2.10", 200},
			{http.MethodGet, root + "/example/changes", "", `"additions"`, 200},
		}},
		{"actions", []gcpStep{
			{
				http.MethodPost, root + "/example/rrsets",
				`{"name":"api.example.com.","type":"A","ttl":60,"rrdatas":["192.0.2.20"]}`, "192.0.2.20", 200,
			},
			{http.MethodGet, root + "/example/rrsets", "", "api.example.com.", 200},
			{http.MethodDelete, root + "/example/rrsets/api.example.com./A", "", "{}", 200},
			{http.MethodGet, root + "/example/rrsets/api.example.com./A", "", "notFound", 404},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, "/dashboard/api/v1/gcp/dns/v1/projects/p/policies", "", "unsupported", 400},
			{http.MethodGet, "/dashboard/api/v1/gcp/dns/v2/projects/p/managedZones", "", "unsupported", 400},
			{http.MethodGet, "/dashboard/api/v1/gcp/dns/v1/projects/p/managedZones/%2e%2e/x", "", "unsupported", 400},
			{http.MethodPost, root + "/example/dnsKeys", "{}", "unsupported", 400},
			{http.MethodGet, root + "/nope", "", "notFound", 404},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runGCPSteps(t, seed(t), tt.steps)
		})
	}

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, root, "", "UNAVAILABLE", http.StatusServiceUnavailable},
		})
	})
}

func TestAzureFileProxy(t *testing.T) {
	t.Parallel()

	const (
		root = "/dashboard/api/v1/azure/file"
		acct = root + "/devstoreaccount1"
	)

	seed := func(t *testing.T) *dashboard.DashboardHandler {
		t.Helper()

		f := azurefile.NewHandler(azurefile.NewInMemoryBackend())
		put := func(path, body string, hdr map[string]string) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/"+path, strings.NewReader(body))

			for k, v := range hdr {
				req.Header.Set(k, v)
			}

			require.NoError(t, f.Handler()(echo.NewContext(req, rec)))
			require.Less(t, rec.Code, 300, "%s: %s", path, rec.Body.String())
		}

		put("share1?restype=share", "", nil)
		put("share1/docs?restype=directory", "", nil)
		put("share1/docs/a.txt", "", map[string]string{"x-ms-type": "file", "x-ms-content-length": "5"})
		put(
			"share1/docs/a.txt?comp=range",
			"hello",
			map[string]string{"x-ms-write": "update", "x-ms-range": "bytes=0-4"},
		)
		put("share1?restype=share&comp=snapshot", "", nil)

		return dashboard.NewHandler(dashboard.Config{AzureFileOps: f})
	}

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"browse", []gcpStep{
			{http.MethodGet, root + "accounts", "", `"devstoreaccount1"`, 200},
			{http.MethodGet, acct + "?comp=list", "", "<Name>share1</Name>", 200},
			{http.MethodGet, acct + "?comp=list&include=snapshots", "", "Snapshot", 200},
			{http.MethodGet, acct + "/share1?restype=directory&comp=list", "", "<Name>docs</Name>", 200},
			{http.MethodGet, acct + "/share1/docs?restype=directory&comp=list", "", "<Name>a.txt</Name>", 200},
			{http.MethodGet, acct + "/share1/docs/a.txt", "", "hello", 200},
		}},
		{"actions", []gcpStep{
			{http.MethodDelete, acct + "/share1/docs/a.txt", "", "", 202},
			{http.MethodGet, acct + "/share1/docs/a.txt", "", "ResourceNotFound", 404},
			{http.MethodDelete, acct + "/share1/docs?restype=directory", "", "", 202},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, root + "/%2e%2e/etc", "", "unsupported", 400},
			{http.MethodGet, root + "/", "", "unsupported", 400},
			{http.MethodGet, acct + "/nope?restype=directory&comp=list", "", "ShareNotFound", 404},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runGCPSteps(t, seed(t), tt.steps)
		})
	}

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, acct + "?comp=list", "", "ServiceUnavailable", http.StatusServiceUnavailable},
			{http.MethodGet, root + "accounts", "", "UNAVAILABLE", http.StatusServiceUnavailable},
		})
	})
}
