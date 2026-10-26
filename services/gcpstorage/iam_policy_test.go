package gcpstorage_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	condIAMPath = "/storage/v1/b/cond-bkt/iam"
	condBinding = `{"role":"roles/storage.objectViewer","members":["allUsers"],` +
		`"condition":{"expression":"request.time < timestamp('2099-01-01T00:00:00Z')","title":"expires"}}`
)

func TestBucketIAMConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		seed       string
		want       int
		freshEtag  bool
		wantPolVer int
	}{
		{
			name: "set v1 with condition", method: http.MethodPut, path: condIAMPath, want: http.StatusBadRequest,
			body: `{"version":1,"bindings":[` + condBinding + `]}`,
		},
		{
			name: "set v3 with condition", method: http.MethodPut, path: condIAMPath, want: http.StatusOK,
			body: `{"version":3,"bindings":[` + condBinding + `]}`, wantPolVer: 3,
		},
		{
			name: "get v1 of conditional", method: http.MethodGet, path: condIAMPath, want: http.StatusBadRequest,
			seed: `{"version":3,"bindings":[` + condBinding + `]}`,
		},
		{
			name: "get v3 of conditional", method: http.MethodGet, path: condIAMPath + "?optionsRequestedPolicyVersion=3",
			want: http.StatusOK, seed: `{"version":3,"bindings":[` + condBinding + `]}`, wantPolVer: 3,
		},
		{
			name: "stale etag", method: http.MethodPut, path: condIAMPath, want: http.StatusPreconditionFailed,
			body: `{"etag":"c3RhbGU=","version":3}`, seed: `{"version":3,"bindings":[` + condBinding + `]}`,
		},
		{
			name: "current etag", method: http.MethodPut, path: condIAMPath, want: http.StatusOK,
			body: `{"version":3}`, seed: `{"version":3,"bindings":[` + condBinding + `]}`, freshEtag: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := newRaw(t)
			r.do(t, http.MethodPost, "/storage/v1/b?project=p", `{"name":"cond-bkt"}`, nil)

			body := tt.body

			if tt.seed != "" {
				resp, seeded := r.do(t, http.MethodPut, condIAMPath, tt.seed, nil)
				require.Equal(t, http.StatusOK, resp.StatusCode, seeded)

				var cur struct {
					Etag string `json:"etag"`
				}

				require.NoError(t, json.Unmarshal([]byte(seeded), &cur))

				if tt.freshEtag {
					body = `{"etag":"` + cur.Etag + `","version":3}`
				}
			}

			resp, out := r.do(t, tt.method, tt.path, body, nil)
			require.Equal(t, tt.want, resp.StatusCode, out)

			if tt.wantPolVer != 0 {
				var got struct {
					Version int `json:"version"`
				}

				require.NoError(t, json.Unmarshal([]byte(out), &got))
				assert.Equal(t, tt.wantPolVer, got.Version)
			}
		})
	}
}

func TestBucketIAMTestPermissionsFilters(t *testing.T) {
	t.Parallel()

	r := newRaw(t)
	r.do(t, http.MethodPost, "/storage/v1/b?project=p", `{"name":"cond-bkt"}`, nil)

	_, body := r.do(t, http.MethodGet,
		condIAMPath+"/testPermissions?permissions=storage.objects.get&permissions=pubsub.topics.get", "", nil)
	assert.JSONEq(t, `{"kind":"storage#testIamPermissionsResponse","permissions":["storage.objects.get"]}`, body)
}
