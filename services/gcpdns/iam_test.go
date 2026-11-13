package gcpdns_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dnsapi "google.golang.org/api/dns/v1"
)

func TestManagedZoneIAM(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "z", "z.com.")

	res := "projects/" + project + "/managedZones/z"

	empty, err := e.svc.ManagedZones.GetIamPolicy(res, &dnsapi.GoogleIamV1GetIamPolicyRequest{}).
		Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Empty(t, empty.Bindings)
	assert.NotEmpty(t, empty.Etag)

	set, err := e.svc.ManagedZones.SetIamPolicy(res, &dnsapi.GoogleIamV1SetIamPolicyRequest{
		Policy: &dnsapi.GoogleIamV1Policy{
			Bindings: []*dnsapi.GoogleIamV1Binding{{Role: "roles/dns.reader", Members: []string{"user:a@example.com"}}},
		},
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	require.Len(t, set.Bindings, 1)
	assert.NotEqual(t, empty.Etag, set.Etag)

	got, err := e.svc.ManagedZones.GetIamPolicy(res, &dnsapi.GoogleIamV1GetIamPolicyRequest{}).
		Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, set.Etag, got.Etag)
	assert.Equal(t, "roles/dns.reader", got.Bindings[0].Role)

	_, err = e.svc.ManagedZones.SetIamPolicy(res, &dnsapi.GoogleIamV1SetIamPolicyRequest{
		Policy: &dnsapi.GoogleIamV1Policy{Etag: empty.Etag},
	}).Context(t.Context()).Do()
	assertAPIError(t, err, http.StatusConflict, "")

	perms, err := e.svc.ManagedZones.TestIamPermissions(res, &dnsapi.GoogleIamV1TestIamPermissionsRequest{
		Permissions: []string{"dns.managedZones.get", "storage.buckets.get"},
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, []string{"dns.managedZones.get"}, perms.Permissions)

	_, err = e.svc.ManagedZones.GetIamPolicy("projects/"+project+"/managedZones/ghost",
		&dnsapi.GoogleIamV1GetIamPolicyRequest{}).Context(t.Context()).Do()
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	require.NoError(t, e.svc.ManagedZones.Delete(project, "z").Context(t.Context()).Do())
	e.zone(t, "z", "z.com.")

	again, err := e.svc.ManagedZones.GetIamPolicy(res, &dnsapi.GoogleIamV1GetIamPolicyRequest{}).
		Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Empty(t, again.Bindings, "a recreated zone starts without a policy")
}
