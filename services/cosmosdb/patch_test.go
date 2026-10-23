package cosmosdb_test

import (
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const patchSeed = `{"id":"p1","pk":"a","n":1,"name":"x","tags":["a","b"],"addr":{"city":"c"}}`

func TestSDK_PatchItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build      func(*azcosmos.PatchOperations)
		name       string
		want       string
		wantStatus int
	}{
		{
			name: "add set replace",
			build: func(p *azcosmos.PatchOperations) {
				p.AppendAdd("/added", "v")
				p.AppendSet("/name", "y")
				p.AppendReplace("/addr/city", "d")
			},
			want: `{"id":"p1","pk":"a","n":1,"name":"y","tags":["a","b"],"addr":{"city":"d"},"added":"v"}`,
		},
		{
			name:  "array insert and append",
			build: func(p *azcosmos.PatchOperations) { p.AppendAdd("/tags/1", "m"); p.AppendAdd("/tags/-", "z") },
			want:  `{"id":"p1","pk":"a","n":1,"name":"x","tags":["a","m","b","z"],"addr":{"city":"c"}}`,
		},
		{
			name:  "remove",
			build: func(p *azcosmos.PatchOperations) { p.AppendRemove("/tags/0"); p.AppendRemove("/addr") },
			want:  `{"id":"p1","pk":"a","n":1,"name":"x","tags":["b"]}`,
		},
		{
			name:  "incr existing and new",
			build: func(p *azcosmos.PatchOperations) { p.AppendIncrement("/n", 41); p.AppendIncrement("/fresh", 5) },
			want:  `{"id":"p1","pk":"a","n":42,"name":"x","tags":["a","b"],"addr":{"city":"c"},"fresh":5}`,
		},
		{
			name: "condition holds",
			build: func(p *azcosmos.PatchOperations) {
				p.SetCondition("from c where c.n = 1")
				p.AppendSet("/name", "ok")
			},
			want: `{"id":"p1","pk":"a","n":1,"name":"ok","tags":["a","b"],"addr":{"city":"c"}}`,
		},
		{
			name: "condition fails",
			build: func(p *azcosmos.PatchOperations) {
				p.SetCondition("from c where c.n = 99")
				p.AppendSet("/name", "no")
			},
			wantStatus: http.StatusPreconditionFailed,
		},
		{
			name:       "replace missing target",
			build:      func(p *azcosmos.PatchOperations) { p.AppendReplace("/nope", 1) },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "remove missing target",
			build:      func(p *azcosmos.PatchOperations) { p.AppendRemove("/nope") },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "incr on string",
			build:      func(p *azcosmos.PatchOperations) { p.AppendIncrement("/name", 1) },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "id is immutable",
			build:      func(p *azcosmos.PatchOperations) { p.AppendSet("/id", "other") },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "partition key is immutable",
			build:      func(p *azcosmos.PatchOperations) { p.AppendSet("/pk", "b") },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "failed op leaves the document untouched",
			build:      func(p *azcosmos.PatchOperations) { p.AppendSet("/name", "z"); p.AppendRemove("/nope") },
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			c := s.container()
			pk := azcosmos.NewPartitionKeyString("a")

			_, err := c.CreateItem(t.Context(), pk, []byte(patchSeed), nil)
			require.NoError(t, err)

			var ops azcosmos.PatchOperations
			tt.build(&ops)

			resp, err := c.PatchItem(t.Context(), pk, "p1", ops, nil)
			if tt.wantStatus != 0 {
				assert.Equal(t, tt.wantStatus, responseStatus(t, err))

				read, readErr := c.ReadItem(t.Context(), pk, "p1", nil)
				require.NoError(t, readErr)
				assert.JSONEq(t, patchSeed, stripSystem(t, read.Value))

				return
			}

			require.NoError(t, err)
			assert.JSONEq(t, tt.want, stripSystem(t, resp.Value))

			read, readErr := c.ReadItem(t.Context(), pk, "p1", nil)
			require.NoError(t, readErr)
			assert.JSONEq(t, tt.want, stripSystem(t, read.Value))
			assert.Equal(t, resp.ETag, read.ETag)
		})
	}
}

func TestSDK_PatchItem_ETagAndMissing(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	c := s.container()
	pk := azcosmos.NewPartitionKeyString("a")

	created, err := c.CreateItem(t.Context(), pk, []byte(patchSeed), nil)
	require.NoError(t, err)

	var ops azcosmos.PatchOperations
	ops.AppendSet("/name", "y")

	stale := azcore.ETag(`"stale"`)
	_, err = c.PatchItem(t.Context(), pk, "p1", ops, &azcosmos.ItemOptions{IfMatchEtag: &stale})
	assert.Equal(t, http.StatusPreconditionFailed, responseStatus(t, err))

	_, err = c.PatchItem(t.Context(), pk, "p1", ops, &azcosmos.ItemOptions{
		IfMatchEtag: &created.ETag, EnableContentResponseOnWrite: true,
	})
	require.NoError(t, err)

	_, err = c.PatchItem(t.Context(), pk, "missing", ops, nil)
	assert.Equal(t, http.StatusNotFound, responseStatus(t, err))
}

func TestRaw_PatchMoveAndLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		want       string
		wantStatus int
	}{
		{
			name: "move property",
			body: `{"operations":[{"op":"move","from":"/name","path":"/addr/label"}]}`,
			want: `{"id":"p1","pk":"a","n":1,"tags":["a","b"],"addr":{"city":"c","label":"x"}}`,
		},
		{
			name:       "move from missing",
			body:       `{"operations":[{"op":"move","from":"/nope","path":"/x"}]}`,
			wantStatus: http.StatusBadRequest,
		},
		{name: "no operations", body: `{"operations":[]}`, wantStatus: http.StatusBadRequest},
		{
			name:       "unknown op",
			body:       `{"operations":[{"op":"copy","path":"/x","value":1}]}`,
			wantStatus: http.StatusBadRequest,
		},
		{name: "not json", body: `nope`, wantStatus: http.StatusBadRequest},
		{
			name: "eleven operations",
			body: `{"operations":[` + repeatOp(11) + `]}`, wantStatus: http.StatusBadRequest,
		},
		{name: "ten operations", body: `{"operations":[` + repeatOp(10) + `]}`, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			require.Equal(t, http.StatusCreated, s.create(patchSeed).status)

			resp := s.do(http.MethodPatch, "/dbs/d/colls/c/docs/p1", map[string]string{
				"X-Ms-Documentdb-Partitionkey": `["a"]`,
			}, tt.body)
			require.Equal(t, max(tt.wantStatus, http.StatusOK), resp.status, string(resp.body))

			if tt.want != "" {
				assert.JSONEq(t, tt.want, stripSystem(t, resp.body))
			}
		})
	}
}

func repeatOp(n int) string {
	ops := make([]string, n)
	for i := range ops {
		ops[i] = `{"op":"incr","path":"/n","value":1}`
	}

	return joinComma(ops)
}
