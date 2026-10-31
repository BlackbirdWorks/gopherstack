package dashboard_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
	"github.com/blackbirdworks/gopherstack/services/gcpresourcemanager"
)

const (
	rmBase = "/dashboard/api/v1/gcp/resourcemanager/v1"
	rmOrg  = "organizations/" + gcpresourcemanager.DefaultOrganizationID
)

func newRMDashboard(t *testing.T, enforce gcpenforce.Settings) *dashboard.DashboardHandler {
	t.Helper()

	b := gcpresourcemanager.NewInMemoryBackend()
	h := gcpresourcemanager.NewHandler(b)
	h.SetEnforcement(enforce)

	return dashboard.NewHandler(dashboard.Config{GCPResourceManagerOps: h})
}

func TestGCPResourceManagerProxy(t *testing.T) {
	t.Parallel()

	t.Run("hierarchy lifecycle", func(t *testing.T) {
		t.Parallel()

		h := newRMDashboard(t, gcpenforce.Settings{})

		runGCPSteps(t, h, []gcpStep{
			{http.MethodGet, rmBase + "/organizations", "", rmOrg, 200},
			{http.MethodPost, rmBase + "/folders", `{"parent":"` + rmOrg + `","displayName":"Team Alpha","name":"folders/9"}`,
				"operations/", 200},
			{http.MethodGet, rmBase + "/folders?parent=" + rmOrg, "", "Team Alpha", 200},
			{http.MethodPost, rmBase + "/projects", `{"projectId":"alpha-proj","displayName":"Alpha","parent":"` + rmOrg + `",` +
				`"labels":{"env":"dev"},"state":"DELETE_REQUESTED"}`, "operations/", 200},
			{http.MethodGet, rmBase + "/projects?parent=" + rmOrg, "", `"env":"dev"`, 200},
			{http.MethodGet, rmBase + "/projects/alpha-proj", "", `"state":"ACTIVE"`, 200},
			{http.MethodDelete, rmBase + "/projects/alpha-proj", "", "operations/", 200},
			{http.MethodGet, rmBase + "/projects/alpha-proj", "", "DELETE_REQUESTED", 200},
			{http.MethodPost, rmBase + "/projects/alpha-proj:undelete", "", "operations/", 200},
			{http.MethodGet, rmBase + "/enforcement", "", `"projects":false`, 200},
			{http.MethodGet, rmBase + "/projects", "", "gopherstack-test", 200},
		})
	})

	t.Run("services", func(t *testing.T) {
		t.Parallel()

		h := newRMDashboard(t, gcpenforce.Settings{})
		svc := rmBase + "/projects/gopherstack-test/services"

		runGCPSteps(t, h, []gcpStep{
			{http.MethodGet, svc + "?state=DISABLED", "", "pubsub.googleapis.com", 200},
			{http.MethodPost, svc + "/pubsub.googleapis.com:enable", "", "operations/", 200},
			{http.MethodGet, svc + "/pubsub.googleapis.com", "", `"state":"ENABLED"`, 200},
			{http.MethodGet, svc + "?state=bogus", "", "state must be", 400},
			{http.MethodPost, svc + "/pubsub.googleapis.com:disable", `{"disableDependentServices":true,"name":"x"}`,
				"operations/", 200},
			{http.MethodPost, svc + "/pubsub.googleapis.com:purge", "", "unsupported", 404},
		})
	})

	t.Run("dependents block disable", func(t *testing.T) {
		t.Parallel()

		h := newRMDashboard(t, gcpenforce.Settings{})
		svc := rmBase + "/projects/gopherstack-test/services"

		runGCPSteps(t, h, []gcpStep{
			{http.MethodPost, svc + "/container.googleapis.com:enable", "", "operations/", 200},
			{http.MethodPost, svc + "/compute.googleapis.com:disable", `{}`, "", 400},
		})
	})

	t.Run("iam etag", func(t *testing.T) {
		t.Parallel()

		h := newRMDashboard(t, gcpenforce.Settings{})
		res := rmBase + "/projects/gopherstack-test"

		rec := agwDo(t, h, http.MethodPost, res+":getIamPolicy", "")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var pol struct {
			Etag string `json:"etag"`
		}

		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &pol))

		add := `{"role":"roles/viewer","member":"user:a@example.com","etag":"` + pol.Etag + `"}`

		first := agwDo(t, h, http.MethodPost, res+":addMember", add)
		require.Equal(t, http.StatusOK, first.Code, first.Body.String())
		assert.Contains(t, first.Body.String(), "user:a@example.com")

		stale := agwDo(t, h, http.MethodPost, res+":addMember", add)
		assert.Equal(t, http.StatusConflict, stale.Code, stale.Body.String())
		assert.Contains(t, stale.Body.String(), "ABORTED")

		var cur struct {
			Etag string `json:"etag"`
		}

		require.NoError(t, json.Unmarshal(first.Body.Bytes(), &cur))

		del := `{"role":"roles/viewer","member":"user:a@example.com","etag":"` + cur.Etag + `"}`
		runGCPSteps(t, h, []gcpStep{
			{http.MethodPost, res + ":removeMember", del, `"etag"`, 200},
			{http.MethodPost, res + ":removeMember", `{"role":"roles/viewer","member":"user:a@example.com","etag":"eA=="}`,
				"changed by another", 409},
		})
	})

	t.Run("rebuilds bodies", func(t *testing.T) {
		t.Parallel()

		h := newRMDashboard(t, gcpenforce.Settings{})
		rec := agwDo(t, h, http.MethodPost, rmBase+"/folders",
			`{"parent":"`+rmOrg+`","displayName":"Rebuilt","name":"folders/1","state":"DELETE_REQUESTED","etag":"zz"}`)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		got := agwDo(t, h, http.MethodGet, rmBase+"/folders?parent="+rmOrg, "")
		assert.Contains(t, got.Body.String(), `"state":"ACTIVE"`)
		assert.NotContains(t, got.Body.String(), "zz")
	})

	t.Run("rejects", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, newRMDashboard(t, gcpenforce.Settings{}), []gcpStep{
			{http.MethodPost, rmBase + "/folders", `{"parent":"projects/x","displayName":"abc"}`, "parent must be", 400},
			{http.MethodPost, rmBase + "/projects", `{"projectId":"abcdef","parent":"bad"}`, "parent must be", 400},
			{http.MethodPost, rmBase + "/projects", `nope`, "invalid", 400},
			{http.MethodGet, rmBase + "/projects?parent=projects/x", "", "unsupported", 404},
			{http.MethodGet, rmBase + "/folders?parent=folders/1/../2", "", "unsupported", 404},
			{http.MethodDelete, rmBase + "/organizations/1", "", "unsupported", 404},
			{http.MethodPost, rmBase + "/organizations/1:move", `{}`, "unsupported", 404},
			{http.MethodPatch, rmBase + "/projects/gopherstack-test", `{}`, "unsupported", 404},
			{http.MethodPut, rmBase + "/folders", `{}`, "unsupported", 404},
			{http.MethodGet, rmBase + "/tagKeys?parent=folders/1", "", "unsupported", 404},
			{http.MethodGet, rmBase + "/operations/x", "", "unsupported", 404},
			{http.MethodGet, rmBase + "/projects/gopherstack-test/services/a%2Fb", "", "unsupported", 404},
			{http.MethodGet, rmBase + "/projects/gopherstack-test/billingInfo", "", "unsupported", 404},
			{http.MethodPost, rmBase + "/projects/gopherstack-test:setIamPolicy", `{}`, "unsupported", 404},
			{http.MethodPost, rmBase + "/projects/gopherstack-test:addMember", `{"role":"admin","member":"user:a","etag":"e"}`,
				"role must be", 400},
			{http.MethodPost, rmBase + "/projects/gopherstack-test:addMember", `{"role":"roles/viewer","member":"user:a"}`,
				"etag is required", 400},
			{http.MethodPost, rmBase + "/projects/gopherstack-test:move", `{"destinationParent":"projects/x"}`,
				"destinationParent", 400},
		})
	})

	t.Run("move and tags", func(t *testing.T) {
		t.Parallel()

		h := newRMDashboard(t, gcpenforce.Settings{})

		runGCPSteps(t, h, []gcpStep{
			{http.MethodPost, rmBase + "/folders", `{"parent":"` + rmOrg + `","displayName":"Dest"}`, "operations/", 200},
			{http.MethodGet, rmBase + "/tagKeys?parent=" + rmOrg, "", "", 200},
			{http.MethodGet, rmBase + "/tagBindings?parent=//cloudresourcemanager.googleapis.com/projects/gopherstack-test",
				"", "", 200},
			{http.MethodPost, rmBase + "/projects/gopherstack-test:move", `{"destinationParent":"` + rmOrg + `"}`,
				"operations/", 200},
		})
	})

	t.Run("enforcement banner", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, newRMDashboard(t, gcpenforce.Settings{Projects: true, ServiceEnablement: true}), []gcpStep{
			{http.MethodGet, rmBase + "/enforcement", "", `"serviceEnablement":true`, 200},
		})
	})

	t.Run("unavailable without the service", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, rmBase + "/organizations", "", "", http.StatusServiceUnavailable},
		})
	})
}
