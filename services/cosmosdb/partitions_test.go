package cosmosdb_test

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

const padKB = 100

func quoted(pk string) string { return strconv.Quote(pk) }

// pkInRange finds a partition key value owned by range idx of n.
func pkInRange(t *testing.T, idx int) string {
	t.Helper()

	const n = 2

	for i := range 1000 {
		pk := fmt.Sprintf("k%d", i)
		if cosmosdb.RangeIndex(quoted(pk), n) == idx {
			return pk
		}
	}

	require.FailNow(t, "no partition key found", "range %d of %d", idx, n)

	return ""
}

func (s *throughputServer) putPK(coll, pk string, padKB int) rawResponse {
	s.t.Helper()

	s.seq++
	body := fmt.Sprintf(`{"id":"i%d","pk":%s,"pad":"%s"}`, s.seq, quoted(pk), strings.Repeat("x", padKB*1024))

	return s.do(http.MethodPost, "/dbs/d/colls/"+coll+"/docs",
		map[string]string{"X-Ms-Documentdb-Partitionkey": "[" + quoted(pk) + "]"}, body)
}

// spike writes padded documents until a 429, returning how many were admitted.
func (s *throughputServer) spike(coll, pk string, limit int) int {
	s.t.Helper()

	for i := range limit {
		if s.putPK(coll, pk, padKB).status == http.StatusTooManyRequests {
			return i
		}
	}

	return limit
}

func (s *throughputServer) query(pk string) rawResponse {
	s.t.Helper()

	h := map[string]string{"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json"}
	if pk != "" {
		h["X-Ms-Documentdb-Partitionkey"] = "[" + quoted(pk) + "]"
	}

	return s.do(http.MethodPost, "/dbs/d/colls/c/docs", h, `{"query":"SELECT * FROM c"}`)
}

func TestPartitions_HotKeyThrottlesWhileTotalUnderBudget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(s *throughputServer, hot, cold string)
		name  string
	}{
		{name: "cold partition admitted", check: func(s *throughputServer, _, cold string) {
			assert.Equal(s.t, http.StatusCreated, s.putPK("c", cold, padKB).status)
		}},
		{name: "hot partition retried after wait", check: func(s *throughputServer, hot, _ string) {
			r := s.putPK("c", hot, padKB)
			require.Equal(s.t, http.StatusTooManyRequests, r.status)
			assert.Equal(s.t, "3200", r.header.Get("X-Ms-Substatus"))

			wait, err := time.ParseDuration(r.header.Get("X-Ms-Retry-After-Ms") + "ms")
			require.NoError(s.t, err)

			s.clock.advance(wait)
			assert.Equal(s.t, http.StatusCreated, s.putPK("c", hot, padKB).status)
		}},
		{name: "cross partition query touches hot", check: func(s *throughputServer, _, _ string) {
			assert.Equal(s.t, http.StatusTooManyRequests, s.query("").status)
		}},
		{name: "scoped query on cold passes", check: func(s *throughputServer, _, cold string) {
			assert.Equal(s.t, http.StatusOK, s.query(cold).status)
		}},
		{name: "scoped query on hot throttles", check: func(s *throughputServer, hot, _ string) {
			assert.Equal(s.t, http.StatusTooManyRequests, s.query(hot).status)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newThroughputServer(t, true, false)
			s.mustCreate("/dbs", `{"id":"d"}`, nil)
			s.container("c", manualHeader(10100))

			hot, cold := pkInRange(t, 0), pkInRange(t, 1)
			admitted := s.spike("c", hot, 500)
			require.Less(t, admitted, 500, "hot partition must throttle")
			require.Positive(t, admitted)

			// Each admitted write costs ~56 RU, so the hot partition spent about half the 10100 RU/s offer.
			assert.Less(t, float64(admitted)*56, 10100.0)

			tt.check(s, hot, cold)
		})
	}
}

func TestPartitions_BurstCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		burst   bool
		idle    time.Duration
		minimum int
		maximum int
	}{
		{name: "burst off", burst: false, idle: 100 * time.Second, minimum: 5, maximum: 20},
		{name: "burst on without idle time", burst: true, minimum: 5, maximum: 20},
		{name: "burst on after idle", burst: true, idle: 100 * time.Second, minimum: 40, maximum: 70},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newBurstServer(t, true, false, tt.burst)
			s.mustCreate("/dbs", `{"id":"d"}`, nil)
			s.container("c", manualHeader(1000))
			require.Equal(t, http.StatusCreated, s.putPK("c", "k", 1).status)

			s.clock.advance(tt.idle)

			got := s.spike("c", "k", 200)
			assert.GreaterOrEqual(t, got, tt.minimum)
			assert.LessOrEqual(t, got, tt.maximum)
		})
	}
}

func TestPartitions_PKRangesFollowThroughput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		offer map[string]string
		name  string
		want  int
	}{
		{name: "small", offer: manualHeader(400), want: 1},
		{name: "exactly one", offer: manualHeader(10000), want: 1},
		{name: "two", offer: manualHeader(10100), want: 2},
		{name: "three", offer: manualHeader(30000), want: 3},
		{name: "autoscale max", offer: autoHeader(20000), want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newThroughputServer(t, false, false)
			s.mustCreate("/dbs", `{"id":"d"}`, nil)
			s.container("c", tt.offer)

			assertRangesTile(t, s, tt.want)
		})
	}
}

func assertRangesTile(t *testing.T, s *throughputServer, want int) {
	t.Helper()

	resp := s.do(http.MethodGet, "/dbs/d/colls/c/pkranges", nil, "")
	require.Equal(t, http.StatusOK, resp.status)

	ranges, _ := resp.object(t)["PartitionKeyRanges"].([]any)
	require.Len(t, ranges, want)

	prevMax := ""

	for _, r := range ranges {
		m := rowObj(t, r)
		assert.Equal(t, prevMax, m["minInclusive"])

		prevMax, _ = m["maxExclusive"].(string)
	}

	assert.Equal(t, "FF", prevMax)
}

func TestPartitions_ReplaceThroughputSplitsRanges(t *testing.T) {
	t.Parallel()

	s := newThroughputServer(t, false, false)
	s.mustCreate("/dbs", `{"id":"d"}`, nil)
	s.container("c", manualHeader(10000))
	assertRangesTile(t, s, 1)

	first := s.do(http.MethodGet, "/dbs/d/colls/c/pkranges", nil, "")
	info, err := s.backend.GetThroughput("d", "c")
	require.NoError(t, err)

	put := s.do(http.MethodPut, "/offers/"+info.OfferRID, nil, `{"content":{"offerThroughput":30000}}`)
	require.Equal(t, http.StatusOK, put.status, string(put.body))

	assertRangesTile(t, s, 3)

	stale := s.do(http.MethodGet, "/dbs/d/colls/c/pkranges",
		map[string]string{"If-None-Match": first.header.Get("ETag")}, "")
	assert.Equal(t, http.StatusOK, stale.status, "a changed layout invalidates the cached ranges")
}

func TestPartitions_RangeBoundsContainKeys(t *testing.T) {
	t.Parallel()

	for _, n := range []int{1, 2, 3, 7, 10, 100} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			t.Parallel()

			for k := range 300 {
				pk := quoted(fmt.Sprintf("key-%d", k))
				idx := cosmosdb.RangeIndex(pk, n)
				require.GreaterOrEqual(t, idx, 0)
				require.Less(t, idx, n)

				lo, hi := cosmosdb.RangeBounds(idx, n)
				epk := cosmosdb.EPKHex(pk)
				assert.True(t, lo == "" || epk >= lo, "%s below %s", epk, lo)
				assert.True(t, hi == "FF" || epk < hi, "%s not below %s", epk, hi)
			}
		})
	}
}

func TestPartitions_ChangeFeedPerRange(t *testing.T) {
	t.Parallel()

	s := newThroughputServer(t, false, false)
	s.mustCreate("/dbs", `{"id":"d"}`, nil)
	s.container("c", manualHeader(30000))

	want := map[string]int{}

	for i := range 30 {
		pk := fmt.Sprintf("key-%d", i)
		r := s.putPK("c", pk, 0)
		require.Equal(t, http.StatusCreated, r.status)

		want[fmt.Sprintf("i%d", s.seq)] = cosmosdb.RangeIndex(quoted(pk), 3)
	}

	seen := map[string]int{}

	for rng := range 3 {
		feed := s.do(http.MethodGet, "/dbs/d/colls/c/docs",
			map[string]string{"A-Im": "Incremental feed", "X-Ms-Documentdb-Partitionkeyrangeid": strconv.Itoa(rng)}, "")

		if feed.status == http.StatusNotModified {
			continue
		}

		require.Equal(t, http.StatusOK, feed.status)

		for _, id := range feedIDs(t, feed) {
			_, dup := seen[id]
			assert.False(t, dup, "%s served by two ranges", id)

			seen[id] = rng
		}
	}

	assert.Equal(t, want, seen, "every document is served by exactly the range owning its key")

	bad := s.do(http.MethodGet, "/dbs/d/colls/c/docs",
		map[string]string{"A-Im": "Incremental feed", "X-Ms-Documentdb-Partitionkeyrangeid": "3"}, "")
	assert.Equal(t, http.StatusBadRequest, bad.status)

	all := s.do(http.MethodGet, "/dbs/d/colls/c/docs", map[string]string{"A-Im": "Incremental feed"}, "")
	assert.Len(t, feedIDs(t, all), 30)
}
