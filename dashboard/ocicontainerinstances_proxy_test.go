package dashboard_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/ocicontainerinstances"
	"github.com/blackbirdworks/gopherstack/services/ocicore"
	"github.com/blackbirdworks/gopherstack/services/ociidentity"
)

const (
	ciComp      = "ocid1.compartment.oc1..cicomp"
	ciBase      = "/dashboard/api/v1/oci/containerinstances"
	ciAPI       = ciBase + "/api"
	ciAD        = "Uocm:US-ASHBURN-AD-1"
	ciSubnet    = "ocid1.subnet.oc1..cisubnet"
	ciEnvValue  = "SUPER-SECRET-ENV-VALUE"
	ciPassword  = "SUPER-SECRET-REGISTRY-PASSWORD"
	ciUsername  = "SUPER-SECRET-REGISTRY-USER"
	ciHeaderVal = "SUPER-SECRET-HEADER-VALUE"
	ciConfigTxt = "SUPER-SECRET-CONFIG-CONTENT"
)

type ciFakeRuntime struct{ logs string }

func (r ciFakeRuntime) Pull(context.Context, string, *ocicontainerinstances.Credential) error {
	return nil
}

func (r ciFakeRuntime) Start(context.Context, container.Spec) (string, error) { return "c1", nil }

func (r ciFakeRuntime) Wait(ctx context.Context, _ string) (int, error) {
	<-ctx.Done()

	return 0, ctx.Err()
}

func (r ciFakeRuntime) Logs(context.Context, string) (string, error) { return r.logs, nil }
func (r ciFakeRuntime) Kill(context.Context, string) error            { return nil }
func (r ciFakeRuntime) Remove(context.Context, string) error          { return nil }

type ciFixture struct {
	h   *dashboard.DashboardHandler
	ops *ocicontainerinstances.Handler
}

func newCIDashboard(t *testing.T, mutate func(*ocicontainerinstances.InMemoryBackend)) ciFixture {
	t.Helper()

	be := ocicontainerinstances.NewInMemoryBackend()
	be.SetTransition(0)

	if mutate != nil {
		mutate(be)
	}

	t.Cleanup(be.Close)

	ops := ocicontainerinstances.NewHandler(be)
	core := ocicore.NewHandler(ocicore.NewInMemoryBackend())

	return ciFixture{
		h: dashboard.NewHandler(dashboard.Config{
			OCIContainerInstancesOps: ops, OCICoreOps: core, OCIIdentityOps: ociidentity.NewHandler(ociidentity.NewInMemoryBackend()),
		}),
		ops: ops,
	}
}

func (f ciFixture) direct(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	e.Any("/*", f.ops.Trusted())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	e.ServeHTTP(rec, req)

	return rec
}

func (f ciFixture) proxy(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	return lbDo(t, f.h, method, path, body)
}

// settle runs the due state transitions until the instance reports state.
func (f ciFixture) settle(t *testing.T, id, state string) {
	t.Helper()

	require.Eventually(t, func() bool {
		f.ops.Backend.Sweep()

		rec := f.direct(t, http.MethodGet, "/20210415/containerInstances/"+id, "")

		var got struct {
			State string `json:"lifecycleState"`
		}

		return json.Unmarshal(rec.Body.Bytes(), &got) == nil && got.State == state
	}, 10*time.Second, 5*time.Millisecond)
}

const ciSecretsCreate = `{
 "compartmentId":"` + ciComp + `","availabilityDomain":"` + ciAD + `","shape":"CI.Standard.E4.Flex",
 "displayName":"secrets","shapeConfig":{"ocpus":1,"memoryInGBs":4},
 "vnics":[{"subnetId":"` + ciSubnet + `"}],
 "volumes":[{"name":"cfg","volumeType":"CONFIGFILE","configs":[{"fileName":"app.conf","path":"etc","data":"%s"}]}],
 "imagePullSecrets":[{"secretType":"BASIC","registryEndpoint":"registry.example.com","username":"` + ciUsername +
	`","password":"` + ciPassword + `"}],
 "containers":[{"imageUrl":"alpine:3.20","displayName":"main","environmentVariables":{"TOKEN":"` + ciEnvValue + `"},
  "volumeMounts":[{"volumeName":"cfg","mountPath":"/etc/app"}],
  "healthChecks":[{"healthCheckType":"HTTP","port":8080,"path":"/","headers":[{"name":"X-Auth","value":"` + ciHeaderVal + `"}]}]}]}`

func ciCreateDirect(t *testing.T, f ciFixture) string {
	t.Helper()

	cfg := base64.StdEncoding.EncodeToString([]byte(ciConfigTxt))
	rec := f.direct(t, http.MethodPost, "/20210415/containerInstances", strings.Replace(ciSecretsCreate, "%s", cfg, 1))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	var inst struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &inst))
	f.settle(t, inst.ID, "ACTIVE")

	return inst.ID
}

func TestOCIContainerInstancesProxyScrubbing(t *testing.T) {
	t.Parallel()

	f := newCIDashboard(t, nil)
	id := ciCreateDirect(t, f)

	direct := f.direct(t, http.MethodGet, "/20210415/containerInstances/"+id, "")
	require.Contains(t, direct.Body.String(), ciConfigTxt[:0]+"app.conf")

	var ctrList struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}

	rec := f.proxy(t, http.MethodGet, ciAPI+"/containers?compartmentId="+ciComp+"&containerInstanceId="+id, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ctrList))
	require.Len(t, ctrList.Items, 1)

	rawCtr := f.direct(t, http.MethodGet, "/20210415/containers/"+ctrList.Items[0].ID, "")
	require.Contains(t, rawCtr.Body.String(), ciEnvValue, "fixture must hold the secret for the scrub to prove anything")

	tests := []struct {
		name string
		path string
		want []string
	}{
		{"instance", ciAPI + "/containerInstances/" + id,
			[]string{`"secretType":"BASIC"`, `"registryEndpoint":"registry.example.com"`, `"sizeInBytes":27`, `"fileName":"app.conf"`}},
		{"instance list", ciAPI + "/containerInstances?compartmentId=" + ciComp, []string{`"displayName":"secrets"`}},
		{"container", ciAPI + "/containers/" + ctrList.Items[0].ID,
			[]string{`"environmentVariableNames":["TOKEN"]`, `"name":"X-Auth"`}},
		{"container list", ciAPI + "/containers?compartmentId=" + ciComp, []string{`"displayName":"main"`}},
		{"work requests", ciAPI + "/workRequests?compartmentId=" + ciComp + "&resourceId=" + id, []string{`"status"`}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := f.proxy(t, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			body := rec.Body.String()
			for _, w := range tt.want {
				assert.Contains(t, body, w)
			}

			for _, secret := range []string{
				ciEnvValue, ciPassword, ciUsername, ciHeaderVal, ciConfigTxt,
				base64.StdEncoding.EncodeToString([]byte(ciConfigTxt)), `"environmentVariables"`, `"password"`,
			} {
				assert.NotContains(t, body, secret)
			}
		})
	}
}

func TestOCIContainerInstancesProxyCreateRebuild(t *testing.T) {
	t.Parallel()

	f := newCIDashboard(t, nil)

	body := `{"compartmentId":"` + ciComp + `","availabilityDomain":"` + ciAD + `","shape":"CI.Standard.E4.Flex",
	 "displayName":"web","shapeConfig":{"ocpus":1,"memoryInGBs":4,"extra":1},"containerRestartPolicy":"NEVER",
	 "vnics":[{"subnetId":"` + ciSubnet + `","isPublicIpAssigned":true}],
	 "freeformTags":{"owner":"mallory"},"securityContext":{"securityContextType":"LINUX","runAsUser":0},
	 "volumes":[{"name":"v","volumeType":"EMPTYDIR"}],
	 "imagePullSecrets":[{"secretType":"BASIC","registryEndpoint":"r","username":"u","password":"p"}],
	 "containers":[{"imageUrl":"alpine:3.20","command":["sh"],"arguments":["-c","echo hi"],
	  "environmentVariables":{"A":"1"},"isResourcePrincipalDisabled":true,"freeformTags":{"k":"v"},
	  "workingDirectory":"/","securityContext":{"runAsUser":0}}]}`

	rec := f.proxy(t, http.MethodPost, ciAPI+"/containerInstances", body)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	var inst struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &inst))

	raw := f.direct(t, http.MethodGet, "/20210415/containerInstances/"+inst.ID, "").Body.String()
	for _, dropped := range []string{"mallory", "securityContext", "EMPTYDIR", "registryEndpoint", "runAsUser"} {
		assert.NotContains(t, raw, dropped)
	}

	assert.Contains(t, raw, `"containerRestartPolicy":"NEVER"`)

	ctr := f.direct(t, http.MethodGet, "/20210415/containers?compartmentId="+ciComp, "").Body.String()
	assert.NotContains(t, ctr, "freeformTags")
}

func TestOCIContainerInstancesProxyBadWrites(t *testing.T) {
	t.Parallel()

	f := newCIDashboard(t, nil)
	good := `{"compartmentId":"` + ciComp + `","availabilityDomain":"` + ciAD + `","shape":"CI.Standard.E4.Flex",
	 "shapeConfig":{"ocpus":1},"vnics":[{"subnetId":"` + ciSubnet + `"}],"containers":[{"imageUrl":"alpine"}]`

	tests := []struct {
		name string
		body string
	}{
		{"not json", `nope`},
		{"no container", strings.Replace(good, `"containers":[{"imageUrl":"alpine"}]`, `"containers":[]`, 1) + "}"},
		{"no image", strings.Replace(good, `"imageUrl":"alpine"`, `"imageUrl":""`, 1) + "}"},
		{"two vnics", strings.Replace(good, `"vnics":[{"subnetId":"`+ciSubnet+`"}]`, `"vnics":[{"subnetId":"a"},{"subnetId":"b"}]`, 1) + "}"},
		{"no ocpus", strings.Replace(good, `"shapeConfig":{"ocpus":1}`, `"shapeConfig":{}`, 1) + "}"},
		{"bad restart", good + `,"containerRestartPolicy":"SOMETIMES"}`},
		{"bad env name", strings.Replace(good, `{"imageUrl":"alpine"}`, `{"imageUrl":"alpine","environmentVariables":{"a b":"1"}}`, 1) + "}"},
		{"long env value", strings.Replace(good, `{"imageUrl":"alpine"}`,
			`{"imageUrl":"alpine","environmentVariables":{"A":"`+strings.Repeat("x", 5000)+`"}}`, 1) + "}"},
		{"too many containers", strings.Replace(good, `[{"imageUrl":"alpine"}]`,
			"["+strings.Repeat(`{"imageUrl":"a"},`, 8)+`{"imageUrl":"a"}]`, 1) + "}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := f.proxy(t, http.MethodPost, ciAPI+"/containerInstances", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "invalid Container Instances request")
		})
	}

	empty := f.direct(t, http.MethodGet, "/20210415/containerInstances?compartmentId="+ciComp, "")
	assert.NotContains(t, empty.Body.String(), "alpine")
	assert.JSONEq(t, `{"items":[]}`, empty.Body.String())
}

func TestOCIContainerInstancesProxyAllowList(t *testing.T) {
	t.Parallel()

	f := newCIDashboard(t, nil)
	id := "ocid1.x.oc1..abc"

	tests := []struct {
		method string
		path   string
	}{
		{http.MethodPatch, ciAPI + "/containerInstances/" + id},
		{http.MethodPost, ciAPI + "/containerInstances/" + id + "/actions/changeCompartment"},
		{http.MethodPost, ciAPI + "/containerInstances/" + id + "/actions/explode"},
		{http.MethodGet, ciAPI + "/containerInstances/" + id + "/actions/start"},
		{http.MethodPut, ciAPI + "/containers/" + id},
		{http.MethodDelete, ciAPI + "/containers/" + id},
		{http.MethodGet, ciAPI + "/containers/" + id + "/actions/retrieveLogs"},
		{http.MethodPost, ciAPI + "/workRequests"},
		{http.MethodDelete, ciAPI + "/workRequests/" + id},
		{http.MethodGet, ciAPI + "/workRequests/" + id + "/logs"},
		{http.MethodPost, ciAPI + "/containerInstanceShapes"},
		{http.MethodGet, ciAPI + "/unknown"},
		{http.MethodGet, ciAPI + "/containerInstances/%2e%2e"},
		{http.MethodGet, ciAPI + "/containerInstances/a%2fb"},
		{http.MethodGet, ciAPI + "/containerInstances/bad%20id"},
		{http.MethodGet, ciAPI + "/containerInstances/" + id + "/a/b/c"},
		{http.MethodGet, ciAPI + "/_gopherstack/oci/keys"},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			t.Parallel()

			rec := f.proxy(t, tt.method, tt.path, "{}")
			assert.Contains(t, []int{http.StatusBadRequest, http.StatusMethodNotAllowed}, rec.Code, rec.Body.String())
		})
	}
}

func TestOCIContainerInstancesProxyUnavailable(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	for _, p := range []string{
		"/compartments", "/engine", "/availabilitydomains", "/subnets", "/api/containerInstances?compartmentId=x",
	} {
		rec := lbDo(t, h, http.MethodGet, ciBase+p, "")
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code, p)
	}
}

func TestOCIContainerInstancesProxyLifecycle(t *testing.T) {
	t.Parallel()

	f := newCIDashboard(t, nil)
	id := ciCreateDirect(t, f)
	inst := ciAPI + "/containerInstances/" + id

	rec := f.proxy(t, http.MethodPost, inst+"/actions/stop", "")
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	assert.NotEmpty(t, rec.Header().Get("opc-work-request-id"))
	f.settle(t, id, "INACTIVE")

	rec = f.proxy(t, http.MethodPost, inst+"/actions/start", "")
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	f.settle(t, id, "ACTIVE")

	rec = f.proxy(t, http.MethodPost, inst+"/actions/restart", "")
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	f.settle(t, id, "ACTIVE")

	rec = f.proxy(t, http.MethodPut, inst, `{"displayName":"renamed","freeformTags":{"x":"y"}}`)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	assert.NotContains(t, f.direct(t, http.MethodGet, "/20210415/containerInstances/"+id, "").Body.String(), `"x":"y"`)
	assert.Contains(t, f.proxy(t, http.MethodGet, inst, "").Body.String(), `"displayName":"renamed"`)

	rec = f.proxy(t, http.MethodPut, inst, `{"displayName":""}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = f.proxy(t, http.MethodGet, ciAPI+"/workRequests?compartmentId="+ciComp+"&resourceId="+id, "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "percentComplete")

	rec = f.proxy(t, http.MethodDelete, inst, "")
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	f.settle(t, id, "DELETED")
}

func TestOCIContainerInstancesProxyLogsStateEngine(t *testing.T) {
	t.Parallel()

	f := newCIDashboard(t, nil)
	id := ciCreateDirect(t, f)

	var ctrs struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}

	rec := f.proxy(t, http.MethodGet, ciAPI+"/containers?compartmentId="+ciComp+"&containerInstanceId="+id, "")
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ctrs))
	require.Len(t, ctrs.Items, 1)

	rec = f.proxy(t, http.MethodPost, ciAPI+"/containers/"+ctrs.Items[0].ID+"/actions/retrieveLogs", "")
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "IncorrectState")
	assert.Contains(t, rec.Body.String(), "container logs need the docker engine")

	assert.JSONEq(t, `{"engine":"state"}`, f.proxy(t, http.MethodGet, ciBase+"/engine", "").Body.String())
}

func TestOCIContainerInstancesProxyLogsDockerEngine(t *testing.T) {
	t.Parallel()

	logs := strings.Repeat("0123456789é\n", 40000) + "<script>alert(1)</script>\nlast line"
	f := newCIDashboard(t, func(be *ocicontainerinstances.InMemoryBackend) {
		be.SetEngine(ocicontainerinstances.NewEngine(func() (ocicontainerinstances.Runtime, error) {
			return ciFakeRuntime{logs: logs}, nil
		}))
	})

	assert.JSONEq(t, `{"engine":"docker"}`, f.proxy(t, http.MethodGet, ciBase+"/engine", "").Body.String())

	id := ciCreateDirect(t, f)

	var ctrs struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}

	rec := f.proxy(t, http.MethodGet, ciAPI+"/containers?compartmentId="+ciComp+"&containerInstanceId="+id, "")
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ctrs))
	require.Len(t, ctrs.Items, 1)

	rec = f.proxy(t, http.MethodPost, ciAPI+"/containers/"+ctrs.Items[0].ID+"/actions/retrieveLogs", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	assert.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	assert.Contains(t, rec.Header().Get("Content-Security-Policy"), "sandbox")
	assert.Equal(t, "true", rec.Header().Get("X-Logs-Truncated"))
	assert.LessOrEqual(t, rec.Body.Len(), 256<<10)
	assert.Positive(t, rec.Body.Len())
	assert.Greater(t, len(logs), 256<<10)
	assert.True(t, strings.HasSuffix(rec.Body.String(), "last line"), "the tail is kept")
	assert.True(t, strings.HasPrefix(rec.Body.String(), "0123456789é\n") || strings.HasPrefix(rec.Body.String(), "é\n") ||
		strings.Contains(rec.Body.String()[:20], "\n"))
}

func TestOCIContainerInstancesProxyHelpers(t *testing.T) {
	t.Parallel()

	f := newCIDashboard(t, nil)

	rec := f.proxy(t, http.MethodGet, ciBase+"/compartments", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), ociidentity.TenancyID)

	rec = f.proxy(t, http.MethodGet, ciBase+"/availabilitydomains", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "AD-1")

	rec = f.proxy(t, http.MethodGet, ciAPI+"/containerInstanceShapes?compartmentId="+ciComp, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "CI.Standard.E4.Flex")

	rec = f.proxy(t, http.MethodGet, ciBase+"/subnets?compartmentId="+ciComp, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.JSONEq(t, `{"items":[]}`, rec.Body.String())
}

func TestOCIContainerInstancesProxySubnets(t *testing.T) {
	t.Parallel()

	be := ocicore.NewInMemoryBackend()
	core := ocicore.NewHandler(be)

	vcn, _, aerr := be.CreateVcn(ocicore.CreateVcnIn{CompartmentID: ciComp, DisplayName: "vcn", CidrBlock: "10.0.0.0/16"})
	require.Nil(t, aerr)

	vcnJSON, err := json.Marshal(vcn)
	require.NoError(t, err)

	var v struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(vcnJSON, &v))

	_, _, aerr = be.CreateSubnet(ocicore.CreateSubnetIn{
		CompartmentID: ciComp, VcnID: v.ID, DisplayName: "sub-a", CidrBlock: "10.0.1.0/24",
	})
	require.Nil(t, aerr)

	h := dashboard.NewHandler(dashboard.Config{
		OCIContainerInstancesOps: ocicontainerinstances.NewHandler(ocicontainerinstances.NewInMemoryBackend()),
		OCICoreOps:               core,
	})

	rec := lbDo(t, h, http.MethodGet, ciBase+"/subnets?compartmentId="+ciComp, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"displayName":"sub-a"`)
	assert.Contains(t, rec.Body.String(), `"cidrBlock":"10.0.1.0/24"`)
	assert.NotContains(t, rec.Body.String(), "securityListIds")
}
