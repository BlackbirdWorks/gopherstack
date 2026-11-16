package gcplogging_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQuotaStateIsSwept(t *testing.T) {
	t.Parallel()

	const parents = 1100

	t.Run("list calls", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, true)
		b := e.handler.Backend

		for i := range parents {
			require.NoError(t, b.AdmitList(fmt.Sprintf("projects/p%d", i)))
		}

		e.clk.advance(2 * time.Minute)
		require.NoError(t, b.AdmitList("projects/fresh"))

		list, _ := b.QuotaParents()
		assert.Less(t, list, 10)
	})

	t.Run("tail rates", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, true)
		b := e.handler.Backend
		now := e.clk.now()

		for i := range parents {
			b.TailAllowed(fmt.Sprintf("projects/p%d", i), 1, now)
		}

		b.TailAllowed("projects/fresh", 1, now.Add(2*time.Minute))

		_, tail := b.QuotaParents()
		assert.Less(t, tail, 10)
	})
}

func TestTailSessionsAreBoundedGlobally(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	b := e.handler.Backend

	for i := range 1000 {
		require.NoError(t, b.OpenTail(fmt.Sprintf("projects/p%d", i)))
	}

	assert.Equal(t, codes.ResourceExhausted, status.Code(b.OpenTail("projects/extra")))
}
