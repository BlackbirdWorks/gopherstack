package ocistreaming_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlainCursorExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		idle    time.Duration
		expired bool
	}{
		{name: "just inside the lifetime", idle: 4*time.Minute + 59*time.Second},
		{name: "past the lifetime", idle: 5*time.Minute + 5*time.Second, expired: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			s := e.mkStream(t, "expiry", 1)
			c := e.msgs(t, s)
			putOne(t, c, s, "k", "v0")

			cur := plainCursor(t, c, s, "0", streaming.CreateCursorDetailsTypeTrimHorizon, nil)
			e.clock.Advance(tt.idle)

			cursor := cur
			_, err := c.GetMessages(t.Context(), streaming.GetMessagesRequest{StreamId: s.Id, Cursor: &cursor})

			if !tt.expired {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
			assert.Equal(t, "InvalidParameter", se.GetCode())
		})
	}
}

func TestNextCursorRenewsLifetime(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "renew", 1)
	c := e.msgs(t, s)
	putOne(t, c, s, "k", "v0")

	cur := plainCursor(t, c, s, "0", streaming.CreateCursorDetailsTypeTrimHorizon, nil)

	for range 3 {
		e.clock.Advance(4 * time.Minute)

		resp := getMsgs(t, c, s, cur, 0)
		require.NotNil(t, resp.OpcNextCursor)
		cur = *resp.OpcNextCursor
	}
}

func TestGroupCursorUnaffectedByPlainLifetime(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	s := e.mkStream(t, "grp-expiry", 1)
	c := e.msgs(t, s)
	putOne(t, c, s, "k", "v0")

	cur := groupCursor(t, c, s, "g", "i1", nil)
	e.clock.Advance(10 * time.Minute)

	resp := getMsgs(t, c, s, cur, 0)
	assert.Len(t, resp.Items, 1)
}
