package glue_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestStopCrawler_TransitionsOutOfStopping verifies that a stopped crawler does
// not hang in STOPPING forever — the reconciler must advance it to READY.
func TestStopCrawler_TransitionsOutOfStopping(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	defer b.Close()

	const name = "stop-transition-crawler"

	_, err := b.CreateCrawler(name, "arn:aws:iam::000000000000:role/glue", "", glue.CrawlerTarget{}, nil)
	require.NoError(t, err)

	require.NoError(t, b.StartCrawler(name))

	// Wait for RUNNING→READY so the crawler can be stopped.
	require.Eventually(t, func() bool {
		c, gErr := b.GetCrawler(name)
		require.NoError(t, gErr)

		return c.State == "READY"
	}, 2*time.Second, 10*time.Millisecond, "crawler never reached READY after start")

	require.NoError(t, b.StartCrawler(name))
	require.NoError(t, b.StopCrawler(name))

	// Immediately after StopCrawler the crawler is STOPPING.
	c, err := b.GetCrawler(name)
	require.NoError(t, err)
	assert.Equal(t, "STOPPING", c.State)

	// The reconciler must move it out of STOPPING (to READY) rather than
	// leaving it stuck.
	require.Eventually(t, func() bool {
		got, gErr := b.GetCrawler(name)
		require.NoError(t, gErr)

		return got.State == "READY"
	}, 2*time.Second, 10*time.Millisecond, "crawler stuck in STOPPING")
}
