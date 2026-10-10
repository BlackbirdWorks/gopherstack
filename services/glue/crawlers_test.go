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

func TestBatchGetCrawlers_FoundAndMissing(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)
	_, err = b.CreateCrawler("c1", "role", "db", glue.CrawlerTarget{}, nil)
	require.NoError(t, err)

	found, missing := b.BatchGetCrawlers([]string{"c1", "c2"})

	assert.Len(t, found, 1)
	assert.Equal(t, "c1", found[0].Name)
	assert.Len(t, missing, 1)
	assert.Contains(t, missing, "c2")
}

func TestSortedGetCrawlers(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db"}, nil)
	require.NoError(t, err)

	for _, name := range []string{"zeta", "alpha", "mu"} {
		_, crawlerErr := b.CreateCrawler(name, "role", "db", glue.CrawlerTarget{}, nil)
		require.NoError(t, crawlerErr)
	}

	crawlers := b.GetCrawlers()
	require.Len(t, crawlers, 3)

	assert.Equal(t, "alpha", crawlers[0].Name)
	assert.Equal(t, "mu", crawlers[1].Name)
	assert.Equal(t, "zeta", crawlers[2].Name)
}

func TestGetCrawler_LastCrawlAfterRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		stop       bool
	}{
		{name: "completed_run", stop: false, wantStatus: "SUCCEEDED"},
		{name: "stopped_run", stop: true, wantStatus: "CANCELLED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend("000000000000", "us-east-1")
			defer b.Close()

			_, err := b.CreateCrawler("lc", "arn:aws:iam::000000000000:role/glue", "", glue.CrawlerTarget{}, nil)
			require.NoError(t, err)

			c, err := b.GetCrawler("lc")
			require.NoError(t, err)
			assert.Nil(t, c.LastCrawl, "no crawl has run yet")

			require.NoError(t, b.StartCrawler("lc"))

			if tt.stop {
				require.NoError(t, b.StopCrawler("lc"))
			}

			require.Eventually(t, func() bool {
				got, gErr := b.GetCrawler("lc")
				require.NoError(t, gErr)

				return got.State == "READY" && got.LastCrawl != nil
			}, 2*time.Second, 10*time.Millisecond)

			got, err := b.GetCrawler("lc")
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.LastCrawl.Status)
			assert.Equal(t, "/aws-glue/crawlers", got.LastCrawl.LogGroup)
			assert.Positive(t, got.LastCrawl.StartTime)
		})
	}
}
