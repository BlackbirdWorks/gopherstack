package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ocicore"
)

const (
	ociCoreComp = "ocid1.compartment.oc1..aaaa"
	ociCoreBase = "/dashboard/api/v1/oci/core"
)

func newOCICoreDashboard(t *testing.T) (*dashboard.DashboardHandler, *ocicore.Handler) {
	t.Helper()

	c := ocicore.NewHandler(ocicore.NewInMemoryBackend())
	c.EnforceAuth = true

	_, _, aerr := c.Backend.CreateVcn(ocicore.CreateVcnIn{
		CompartmentID: ociCoreComp, DisplayName: "vcn-one", CidrBlock: "10.0.0.0/16",
	})
	require.Nil(t, aerr)

	return dashboard.NewHandler(dashboard.Config{OCICoreOps: c}), c
}

func TestOCICoreProxy(t *testing.T) {
	t.Parallel()

	api := ociCoreBase + "/api"
	q := "?compartmentId=" + ociCoreComp

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"compartments", []gcpStep{
			{http.MethodGet, ociCoreBase + "/compartments", "", ociCoreComp, 200},
		}},
		{"vcns", []gcpStep{
			{http.MethodGet, api + "/vcns" + q, "", `"displayName":"vcn-one"`, 200},
			{http.MethodGet, api + "/subnets" + q, "", "[]", 200},
		}},
		{"rejects", []gcpStep{
			{http.MethodDelete, api + "/vcns/x", "", "", http.StatusMethodNotAllowed},
			{http.MethodPost, api + "/vcns", `{}`, "unsupported", 400},
			{http.MethodPost, api + "/instances/x?action=TERMINATE", ``, "unsupported", 400},
			{http.MethodGet, api + "/adminthing", "", "unsupported", 400},
			{http.MethodGet, api + "/vcns/%2e%2e", "", "unsupported", 400},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newOCICoreDashboard(t)
			runGCPSteps(t, h, tt.steps)
		})
	}

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, api+"/vcns"+q, nil)
		require.NoError(t, dashboard.NewHandler(dashboard.Config{}).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}

func TestOCICoreProxyInstanceActions(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	b := ocicore.NewInMemoryBackend()
	b.SetClock(func() time.Time { return now })

	vcn, _, aerr := b.CreateVcn(ocicore.CreateVcnIn{
		CompartmentID: ociCoreComp, DisplayName: "v", CidrBlock: "10.0.0.0/16",
	})
	require.Nil(t, aerr)

	subnet, _, aerr := b.CreateSubnet(ocicore.CreateSubnetIn{
		CompartmentID: ociCoreComp, VcnID: vcn.(*ocicore.Vcn).ID, CidrBlock: "10.0.1.0/24", DisplayName: "s",
	})
	require.Nil(t, aerr)

	imgs, aerr := b.ListImages(ocicore.ImageListOpts{
		Compartment: ociCoreComp, OS: "Oracle Linux", OSVersion: "9", Shape: "VM.Standard.E4.Flex",
	})
	require.Nil(t, aerr)
	require.NotEmpty(t, imgs.Data)

	inst, _, aerr := b.LaunchInstance(ocicore.LaunchInstanceIn{
		CompartmentID: ociCoreComp, AvailabilityDomain: "Abcd:US-ASHBURN-AD-1", Shape: "VM.Standard.E4.Flex",
		DisplayName: "web", Metadata: map[string]string{"user_data": "c2VjcmV0LXRva2Vu"},
		ShapeConfig:       &ocicore.ShapeConfigIn{Ocpus: new(2.0)},
		SourceDetails:     &ocicore.SourceDetails{SourceType: "image", ImageID: imgs.Data[0].(*ocicore.Image).ID},
		CreateVnicDetails: &ocicore.CreateVnicIn{SubnetID: subnet.(*ocicore.Subnet).ID},
	})
	require.Nil(t, aerr)

	now = now.Add(time.Minute)

	h := dashboard.NewHandler(dashboard.Config{OCICoreOps: ocicore.NewHandler(b)})
	path := ociCoreBase + "/api/instances/" + inst.(*ocicore.Instance).ID

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "c2VjcmV0LXRva2Vu")
	assert.Contains(t, rec.Body.String(), "user_data")

	runGCPSteps(t, h, []gcpStep{
		{http.MethodGet, ociCoreBase + "/api/instances?compartmentId=" + ociCoreComp, "", `"displayName":"web"`, 200},
		{http.MethodPost, path + "?action=STOP", "", `"lifecycleState":"STOPPING"`, 200},
		{http.MethodPost, path + "?action=START", "", "IncorrectState", http.StatusConflict},
		{http.MethodPost, path + "?action=TERMINATE", "", "unsupported", http.StatusBadRequest},
	})
}
