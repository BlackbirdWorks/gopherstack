package ocifilestorage

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEveryOperationIsRoutable(t *testing.T) {
	t.Parallel()

	ops := buildOps()
	routed := map[string]bool{}

	for _, r := range routeTable() {
		routed[r.Op] = true

		assert.Contains(t, ops, r.Op, "route %s %s has a handler", r.Method, r.Path)
	}

	for op := range ops {
		assert.True(t, routed[op], "operation %s has a route", op)
	}

	assert.Len(t, routeTable(), len(ops))
}

func TestParseRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method string
		path   string
		op     string
	}{
		{http.MethodPost, "/20171215/fileSystems", opCreateFileSystem},
		{http.MethodGet, "/20171215/mountTargets/ocid1.x", opGetMountTarget},
		{http.MethodPost, "/20171215/mountTargets/actions/validateKeyTabs", opValidateKeyTabs},
		{http.MethodPost, "/20171215/mountTargets/m1/actions/addLock", opAddMountTargetLock},
		{http.MethodDelete, "/20171215/fileSystems/f1/quotaRules/q1", opDeleteQuotaRule},
		{http.MethodGet, "/20171215/nope", opUnknown},
		{http.MethodPatch, "/20171215/fileSystems", opUnknown},
		{http.MethodGet, "/_gopherstack/oci/keys", opAdminKeys},
	}

	for _, tc := range tests {
		t.Run(tc.op+"-"+tc.method, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequestWithContext(t.Context(), tc.method, tc.path, http.NoBody)
			assert.Equal(t, tc.op, parseRoute(r).op)
		})
	}
}
