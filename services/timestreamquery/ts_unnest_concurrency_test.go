package timestreamquery_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

func TestQuery_UnnestConcurrentDistinctShapes(t *testing.T) {
	t.Parallel()

	const workers = 24

	b := fnBackend(t)

	var wg sync.WaitGroup

	errs := make([]error, workers)
	counts := make([]int, workers)

	for i := range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			q := fmt.Sprintf(
				`WITH s AS (SELECT host, create_time_series(time, measure_value::double) AS r FROM db.t GROUP BY host) `+
					`SELECT t.v%d FROM s CROSS JOIN UNNEST(r) AS t (ts%d, v%d)`, i, i, i)

			page, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: q})
			errs[i] = err

			if err == nil {
				counts[i] = len(page.Rows)
			}
		}()
	}

	wg.Wait()

	for i := range workers {
		require.NoError(t, errs[i])
		assert.Equal(t, 6, counts[i])
	}
}
