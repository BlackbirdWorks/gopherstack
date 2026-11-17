package azuregraph_test

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	devicesPath = "/v1.0/devices"
	deviceGUID  = "4c299165-6e8f-4b45-a5ba-c5d250a707ff"
)

func deviceBody() map[string]any {
	return map[string]any{
		"accountEnabled": false, "deviceId": deviceGUID, "displayName": "Test device",
		"operatingSystem": "linux", "operatingSystemVersion": "1",
		"alternativeSecurityIds": []any{map[string]any{"type": 2, "key": "base64Y3YxN2E1MWFlYw=="}},
	}
}

func TestDeviceCreateValidation(t *testing.T) {
	t.Parallel()

	with := func(k string, v any) map[string]any {
		b := deviceBody()
		if v == nil {
			delete(b, k)
		} else {
			b[k] = v
		}

		return b
	}

	tests := []struct {
		body map[string]any
		name string
	}{
		{with("displayName", nil), "missing name"},
		{with("deviceId", nil), "missing deviceId"},
		{with("operatingSystem", nil), "missing os"},
		{with("operatingSystemVersion", nil), "missing os version"},
		{with("deviceId", "nope"), "bad deviceId"},
		{with("displayName", string(make([]byte, 257))), "name too long"},
		{with("profileType", "Bogus"), "bad profile type"},
		{with("trustType", "Bogus"), "bad trust type"},
		{with("deviceOwnership", "Bogus"), "bad ownership"},
		{with("id", newID0()), "id read-only"},
		{with("registrationDateTime", "2020-01-01T00:00:00Z"), "registration read-only"},
		{with("bogus", 1), "unknown property"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			assertErr(t, e.req(t, http.MethodPost, devicesPath, tt.body), http.StatusBadRequest, "Request_BadRequest")
			assert.Empty(t, values2(e.req(t, http.MethodGet, devicesPath, nil), "id"))
		})
	}

	t.Run("malformed json", func(t *testing.T) {
		t.Parallel()

		assertErr(t, newEnv(t).req(t, http.MethodPost, devicesPath, "["), http.StatusBadRequest, "Request_BadRequest")
	})
}

func TestDeviceLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	created := e.req(t, http.MethodPost, devicesPath, deviceBody())
	require.Equal(t, http.StatusCreated, created.status, created.raw)

	id := str(created.body, "id")
	assert.Equal(t, false, created.body["accountEnabled"])
	assert.NotEmpty(t, str(created.body, "registrationDateTime"))

	assertErr(t, e.req(t, http.MethodPost, devicesPath, deviceBody()), http.StatusBadRequest, "Request_BadRequest")

	upper := deviceBody()
	upper["deviceId"] = "4C299165-6E8F-4B45-A5BA-C5D250A707FF"
	assertErr(t, e.req(t, http.MethodPost, devicesPath, upper), http.StatusBadRequest, "Request_BadRequest")

	byKey := e.req(t, http.MethodGet, "/v1.0/devices(deviceId='"+deviceGUID+"')", nil)
	require.Equal(t, http.StatusOK, byKey.status, byKey.raw)
	assert.Equal(t, id, str(byKey.body, "id"))
	assert.Equal(
		t,
		[]string{id},
		values2(e.req(t, http.MethodGet, devicesPath+"?$filter=operatingSystem%20eq%20'linux'", nil), "id"),
	)

	patch := e.req(
		t,
		http.MethodPatch,
		devicesPath+"/"+id,
		map[string]any{"displayName": "Renamed", "isCompliant": true},
	)
	require.Equal(t, http.StatusNoContent, patch.status, patch.raw)

	got := e.req(t, http.MethodGet, devicesPath+"/"+id, nil)
	assert.Equal(t, "Renamed", str(got.body, "displayName"))
	assert.Equal(t, true, got.body["isCompliant"])

	for _, body := range []map[string]any{{"deviceId": newID0()}, {"id": newID0()}, {"manufacturer": "x"}, {"bogus": 1}} {
		assertErr(t, e.req(t, http.MethodPatch, devicesPath+"/"+id, body), http.StatusBadRequest, "Request_BadRequest")
	}

	assertErr(t, e.req(t, http.MethodPatch, devicesPath+"/"+newID0(), map[string]any{"displayName": "x"}),
		http.StatusNotFound, "Request_ResourceNotFound")

	require.Equal(
		t,
		http.StatusNoContent,
		e.req(t, http.MethodDelete, "/v1.0/devices(deviceId='"+deviceGUID+"')", nil).status,
	)
	assertErr(t, e.req(t, http.MethodGet, devicesPath+"/"+id, nil), http.StatusNotFound, "Request_ResourceNotFound")
	assertErr(
		t,
		e.req(t, http.MethodGet, "/v1.0/directory/deletedItems/"+id, nil),
		http.StatusNotFound,
		"Request_ResourceNotFound",
	)

	again := e.req(t, http.MethodPost, devicesPath, deviceBody())
	assert.Equal(t, http.StatusCreated, again.status, again.raw)
}

func TestDeviceRegisteredLinks(t *testing.T) {
	t.Parallel()

	for _, rel := range []string{"registeredOwners", "registeredUsers"} {
		t.Run(rel, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			id := str(e.mustCreate(t, devicesPath, deviceBody()), "id")
			user := newUser(t, e, "dev"+upnDomain)
			uid := str(user, "id")
			base := devicesPath + "/" + id + "/" + rel
			ref := map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/directoryObjects/" + uid}

			require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPost, base+"/$ref", ref).status)
			assertErr(t, e.req(t, http.MethodPost, base+"/$ref", ref), http.StatusBadRequest, "Request_BadRequest")
			assert.Equal(t, []string{uid}, values2(e.req(t, http.MethodGet, base, nil), "id"))
			assert.Len(t, values2(e.req(t, http.MethodGet, base+"/$ref", nil), "@odata.id"), 1)
			assert.Equal(t, []string{uid}, values2(e.req(t, http.MethodGet, base+"/microsoft.graph.user", nil), "id"))

			grp := newGroup(t, e, "g"+rel)
			assertErr(t, e.req(t, http.MethodPost, base+"/$ref",
				map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/directoryObjects/" + str(grp, "id")}),
				http.StatusBadRequest, "Request_BadRequest")
			assertErr(t, e.req(t, http.MethodPost, base+"/$ref",
				map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/directoryObjects/" + newID0()}),
				http.StatusNotFound, "Request_ResourceNotFound")
			assertErr(
				t,
				e.req(t, http.MethodDelete, base+"/"+newID0()+"/$ref", nil),
				http.StatusNotFound,
				"Request_ResourceNotFound",
			)

			fresh := newEnv(t)
			require.NoError(t, fresh.h.Restore(t.Context(), e.h.Snapshot(t.Context())))
			assert.Equal(t, []string{uid}, values2(fresh.req(t, http.MethodGet, base, nil), "id"))

			require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, base+"/"+uid+"/$ref", nil).status)
			assert.Empty(t, values2(e.req(t, http.MethodGet, base, nil), "id"))

			require.Equal(t, http.StatusNoContent, e.req(t, http.MethodPost, base+"/$ref", ref).status)
			require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, "/v1.0/users/"+uid, nil).status)
			assert.Empty(t, values2(e.req(t, http.MethodGet, base, nil), "id"))
		})
	}
}

func TestDeviceDeleteDropsLinksAndPersists(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	id := str(e.mustCreate(t, devicesPath, deviceBody()), "id")
	uid := str(newUser(t, e, "link"+upnDomain), "id")
	ref := map[string]any{"@odata.id": "https://graph.microsoft.com/v1.0/users/" + uid}
	require.Equal(
		t,
		http.StatusNoContent,
		e.req(t, http.MethodPost, devicesPath+"/"+id+"/registeredOwners/$ref", ref).status,
	)

	fresh := newEnv(t)
	require.NoError(t, fresh.h.Restore(t.Context(), e.h.Snapshot(t.Context())))
	assert.Equal(t, "Test device", str(fresh.req(t, http.MethodGet, devicesPath+"/"+id, nil).body, "displayName"))

	require.Equal(t, http.StatusNoContent, e.req(t, http.MethodDelete, devicesPath+"/"+id, nil).status)

	assert.NotContains(t, string(e.h.Snapshot(t.Context())), id)

	back := e.mustCreate(t, devicesPath, deviceBody())
	assert.Empty(t, values2(e.req(t, http.MethodGet, devicesPath+"/"+str(back, "id")+"/registeredOwners", nil), "id"))
}

func TestDeviceLimitsUnderConcurrency(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	var wg sync.WaitGroup

	// the same deviceId raced by many creators lands exactly once
	var won atomic.Int64

	for range 16 {
		wg.Go(func() {
			r := e.req(t, http.MethodPost, devicesPath, map[string]any{
				"deviceId": "11111111-2222-3333-4444-555555555555", "displayName": "d", "operatingSystem": "o",
				"operatingSystemVersion": "1",
			})
			if r.status == http.StatusCreated {
				won.Add(1)
			}
		})
	}

	wg.Wait()
	assert.EqualValues(t, 1, won.Load())
}
