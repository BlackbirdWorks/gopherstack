package ociqueue_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociqueue"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "persist", func(d *queue.CreateQueueDetails) { d.DeadLetterQueueDeliveryCount = new(1) })
	c := e.data(t, q)

	put(t, c, q, "ch", "keep", "poison")
	in := get(t, c, q, func(r *queue.GetMessagesRequest) { r.Limit = new(1); r.VisibilityInSeconds = new(0) })
	require.Len(t, in, 1)

	snap := e.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := oci.NewInMemoryBackend()
	restored.SetClock(e.clock.Now)
	require.NoError(t, restored.Restore(t.Context(), snap))

	st, err := restored.GetStats(*q.Id, "")
	require.NoError(t, err)
	assert.EqualValues(t, 1, st.DLQ.VisibleMessages, "the dead lettered message survives")
	assert.EqualValues(t, 1, st.Queue.VisibleMessages)

	got, _, err := restored.GetQueue(*q.Id, "http://x")
	require.NoError(t, err)
	assert.Equal(t, "persist", got.Name)

	e.backend.Reset()

	_, _, err = e.backend.GetQueue(*q.Id, "http://x")
	require.Error(t, err)
}

func TestRestoreRejectsNullEntries(t *testing.T) {
	t.Parallel()

	b := oci.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte(`{"version":1,"queues":{"a":null}}`))
	require.Error(t, err)
}

func TestRestoreDiscardsOldVersion(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := e.mkQueue(t, "stale", nil)

	require.NoError(t, e.backend.Restore(t.Context(), []byte(`{"version":99}`)))

	_, _, err := e.backend.GetQueue(*q.Id, "http://x")
	require.Error(t, err)
}

func TestErrorEnvelope(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		code   string
		status int
	}{
		{"missing queue", http.MethodGet, "/20210201/queues/ocid1.queue.oc1..nope", "", "NotAuthorizedOrNotFound", 404},
		{"bad json", http.MethodPost, "/20210201/queues", "{", "CannotParseRequest", 400},
		{"no body", http.MethodPost, "/20210201/queues", "", "MissingParameter", 400},
		{"no compartment", http.MethodPost, "/20210201/queues", `{"displayName":"x"}`, "MissingParameter", 400},
		{
			"bad purge",
			http.MethodPost,
			"/20210201/queues/ocid1.queue.oc1..nope/actions/purge",
			`{"purgeType":"WHAT"}`,
			"InvalidParameter",
			400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := rawDo(t, tt.method, e.base+tt.path, tt.body)
			assert.Equal(t, tt.status, status)
			assert.Contains(t, body, `"code":"`+tt.code+`"`)
			assert.Contains(t, body, `"message"`)
		})
	}
}

func TestAdminKeyEndpoint(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	status, _ := rawDo(t, http.MethodPost, e.base+"/_gopherstack/oci/keys",
		`{"tenancy":"`+testTenancy+`","user":"`+testUser+`","publicKey":`+quote(e.kp.pubPEM)+`}`)
	assert.Equal(t, http.StatusCreated, status)

	status, body := rawDo(t, http.MethodGet, e.base+"/_gopherstack/oci/keys", "")
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, testTenancy)
}
