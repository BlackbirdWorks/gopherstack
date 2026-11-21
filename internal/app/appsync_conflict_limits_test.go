package app

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestAppSyncLambdaConflictHandlerLimits(t *testing.T) {
	t.Parallel()

	const (
		itemLimit    = 400 * 1024
		payloadLimit = 6 << 20
	)

	tests := []struct {
		reply func([]byte) ([]byte, error)
		name  string
	}{
		{
			name: "resolve item over the dynamodb item limit",
			reply: func([]byte) ([]byte, error) {
				return []byte(`{"action":"RESOLVE","item":{"title":"` + strings.Repeat("x", itemLimit+1) + `"}}`), nil
			},
		},
		{
			name: "response over the lambda payload limit",
			reply: func([]byte) ([]byte, error) {
				return []byte(`{"action":"REJECT","pad":"` + strings.Repeat("x", payloadLimit) + `"}`), nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newASCFFixture(t, asCFSync("LAMBDA", asCFArn), &asCFLambda{fn: tt.reply})
			f.seed(t)

			res := f.do(t, `mutation { putItem(id:"i1", title:"x", v:1) `+asCFSel+` }`)
			require.NotEmpty(t, res.Errors)
			assert.Equal(t, "ConflictError", res.Errors[0].ErrorType)

			got := f.do(t, `query { getItem(id:"i1") `+asCFSel+` }`)
			item, _ := got.Data["getItem"].(map[string]any)
			assert.Equal(t, "Nadia", item["title"], "the stored item is untouched")
		})
	}
}

func rawUpdate(expr, names, vals string) string {
	return `rawUpdate(id:"i1", expr:` + strconv.Quote(expr) + `, names:` + strconv.Quote(names) +
		`, vals:` + strconv.Quote(vals) + `, v:2)`
}

func TestAppSyncClientCannotWriteSyncMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutation string
	}{
		{"put _deleted", `rawPut(id:"i2", attrs:"{\"_deleted\":{\"BOOL\":true}}")`},
		{"put _version", `rawPut(id:"i2", attrs:"{\"_version\":{\"N\":\"50\"}}")`},
		{"put _lastChangedAt", `rawPut(id:"i2", attrs:"{\"_lastChangedAt\":{\"N\":\"1\"}}")`},
		{"put _ttl", `rawPut(id:"i2", attrs:"{\"_ttl\":{\"N\":\"1\"}}")`},
		{"update _deleted", rawUpdate("SET _deleted = :t", "{}", `{":t":{"BOOL":true}}`)},
		{"update _ttl", rawUpdate("SET _ttl = :n", "{}", `{":n":{"N":"5"}}`)},
		{"update through a name", rawUpdate("SET #v = :n", `{"#v":"_version"}`, `{":n":{"N":"5"}}`)},
		{"add to _version", rawUpdate("ADD #v :n", `{"#v":"_version"}`, `{":n":{"N":"5"}}`)},
		{"remove _version", rawUpdate("REMOVE _version", "{}", "{}")},
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i)+" "+tt.name, func(t *testing.T) {
			t.Parallel()

			f := newASCFFixture(t, asCFSync("OPTIMISTIC_CONCURRENCY", ""), nil)
			f.seed(t)

			res := f.do(t, `mutation { `+tt.mutation+` `+asCFSel+` }`)
			require.NotEmpty(t, res.Errors, "%s", tt.mutation)
			assert.Equal(t, "BadRequest", res.Errors[0].ErrorType, res.Errors[0].Message)
		})
	}
}

func TestAppSyncVersionedWritesUseTheInjectedClock(t *testing.T) {
	t.Parallel()

	f := newASCFFixture(t, asCFSync("OPTIMISTIC_CONCURRENCY", ""), nil)

	h, ok := serviceByName(f.fx.services)["AppSync"].(*appsyncbackend.Handler)
	require.True(t, ok)

	bk, ok := h.Backend.(*appsyncbackend.InMemoryBackend)
	require.True(t, ok)

	at := time.Date(2031, 2, 3, 4, 5, 6, 0, time.UTC)
	bk.SetClock(func() time.Time { return at })

	res := f.do(t, `mutation { putItem(id:"c1", title:"x", labels:["l"]) { id _lastChangedAt } }`)
	require.Empty(t, res.Errors)

	got, _ := res.Data["putItem"].(map[string]any)
	assert.InDelta(t, float64(at.UnixMilli()), got["_lastChangedAt"], 0)
}
