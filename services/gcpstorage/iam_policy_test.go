package gcpstorage_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketIAMConditions(t *testing.T) {
	t.Parallel()

	const (
		iamPath = "/storage/v1/b/cond-bkt/iam"
		condBnd = `{"role":"roles/storage.objectViewer","members":["allUsers"],` +
			`"condition":{"expression":"request.time < timestamp('2099-01-01T00:00:00Z')","title":"expires"}}`
	)

	r := newRaw(t)
	r.do(t, http.MethodPost, "/storage/v1/b?project=p", `{"name":"cond-bkt"}`, nil)

	resp, _ := r.do(t, http.MethodPut, iamPath, `{"version":1,"bindings":[`+condBnd+`]}`, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "conditions need policy version 3")

	resp, body := r.do(t, http.MethodPut, iamPath, `{"version":3,"bindings":[`+condBnd+`]}`, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)

	var set struct {
		Etag    string `json:"etag"`
		Version int    `json:"version"`
	}

	require.NoError(t, json.Unmarshal([]byte(body), &set))
	assert.Equal(t, 3, set.Version)

	tests := []struct {
		name   string
		path   string
		want   int
		method string
		body   string
	}{
		{"get v1 rejected", iamPath, http.StatusBadRequest, http.MethodGet, ""},
		{"get v3 ok", iamPath + "?optionsRequestedPolicyVersion=3", http.StatusOK, http.MethodGet, ""},
		{"stale etag", iamPath, http.StatusPreconditionFailed, http.MethodPut, `{"etag":"c3RhbGU=","version":3}`},
		{"current etag", iamPath, http.StatusOK, http.MethodPut, `{"etag":"` + set.Etag + `","version":3}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, body := r.do(t, tt.method, tt.path, tt.body, nil)
			assert.Equal(t, tt.want, resp.StatusCode, body)
		})
	}

	_, body = r.do(t, http.MethodGet, iamPath+"/testPermissions?permissions=storage.objects.get&permissions=pubsub.topics.get", "", nil)
	assert.JSONEq(t, `{"kind":"storage#testIamPermissionsResponse","permissions":["storage.objects.get"]}`, body)
}
