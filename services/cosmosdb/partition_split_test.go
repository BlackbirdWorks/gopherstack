package cosmosdb_test

import (
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func (s *throughputServer) setOffer(ru int) rawResponse {
	s.t.Helper()

	info, err := s.backend.GetThroughput("d", "c")
	require.NoError(s.t, err)

	return s.do(http.MethodPut, "/offers/"+info.OfferRID, nil, fmt.Sprintf(`{"content":{"offerThroughput":%d}}`, ru))
}

type rangeDoc struct {
	id      string
	min     string
	max     string
	parents []string
}

func (s *throughputServer) ranges() []rangeDoc {
	s.t.Helper()

	resp := s.do(http.MethodGet, "/dbs/d/colls/c/pkranges", nil, "")
	require.Equal(s.t, http.StatusOK, resp.status)

	raw, _ := resp.object(s.t)["PartitionKeyRanges"].([]any)
	out := make([]rangeDoc, 0, len(raw))

	for _, r := range raw {
		m := rowObj(s.t, r)
		d := rangeDoc{id: m["id"].(string), min: m["minInclusive"].(string), max: m["maxExclusive"].(string)}

		for _, p := range m["parents"].([]any) {
			d.parents = append(d.parents, p.(string))
		}

		out = append(out, d)
	}

	return out
}

func ids(rs []rangeDoc) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.id
	}

	return out
}

func TestPartitionSplit_ThroughputRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, s *throughputServer)
		name  string
	}{
		{name: "increase splits widest range with parents", check: func(t *testing.T, s *throughputServer) {
			t.Helper()

			s.container("c", manualHeader(20000))
			require.Equal(t, []string{"0", "1"}, ids(s.ranges()))
			require.Equal(t, http.StatusOK, s.setOffer(30000).status)

			rs := s.ranges()
			require.Len(t, rs, 3, "ROUNDUP(30000/10000) partitions")
			assert.Contains(t, ids(rs), "1", "the unsplit partition keeps its id")

			children := 0

			for _, r := range rs {
				if len(r.parents) > 0 {
					children++

					assert.Equal(t, []string{"0"}, r.parents)
				}
			}

			assert.Equal(t, 2, children)
		}},
		{name: "ranges still tile the hash space after a split", check: func(t *testing.T, s *throughputServer) {
			t.Helper()

			s.container("c", manualHeader(10000))
			require.Equal(t, http.StatusOK, s.setOffer(50000).status)

			rs := s.ranges()
			require.Len(t, rs, 5)

			prev := ""
			for _, r := range rs {
				assert.Equal(t, prev, r.min)
				prev = r.max
			}

			assert.Equal(t, "FF", prev)
		}},
		{name: "lowering throughput never merges", check: func(t *testing.T, s *throughputServer) {
			t.Helper()

			s.container("c", manualHeader(30000))
			require.Equal(t, http.StatusOK, s.setOffer(5000).status)

			assert.Len(t, s.ranges(), 3)
		}},
		{name: "a retired range id answers 410 with substatus 1002", check: func(t *testing.T, s *throughputServer) {
			t.Helper()

			s.container("c", manualHeader(10000))
			require.Equal(t, http.StatusOK, s.setOffer(20000).status)

			feed := func(id string) rawResponse {
				return s.do(http.MethodGet, "/dbs/d/colls/c/docs",
					map[string]string{"A-Im": "Incremental feed", "X-Ms-Documentdb-Partitionkeyrangeid": id}, "")
			}

			gone := feed("0")
			assert.Equal(t, http.StatusGone, gone.status)
			assert.Equal(t, "1002", gone.header.Get("X-Ms-Substatus"))
			assert.Equal(t, http.StatusBadRequest, feed("99").status, "an id never issued is not gone")
			assert.Equal(t, http.StatusNotModified, feed("1").status)
		}},
		{name: "scale-up is asynchronous on the clock", check: func(t *testing.T, s *throughputServer) {
			t.Helper()

			cosmosdb.SetNowFunc(s.backend, s.clock.now)
			s.backend.SetPartitionLimits(0, 5*time.Hour)
			s.container("c", manualHeader(10000))

			put := s.setOffer(30000)
			require.Equal(t, http.StatusOK, put.status)
			assert.Equal(t, "true", put.header.Get("X-Ms-Offer-Replace-Pending"))
			assert.Len(t, s.ranges(), 1)

			s.clock.advance(4 * time.Hour)
			assert.Len(t, s.ranges(), 1)

			s.clock.advance(time.Hour)
			assert.Len(t, s.ranges(), 3)

			info, err := s.backend.GetThroughput("d", "c")
			require.NoError(t, err)
			assert.False(t, info.Pending)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newThroughputServer(t, false, false)
			s.mustCreate("/dbs", `{"id":"d"}`, nil)
			tt.check(t, s)
		})
	}
}

func TestPartitionSplit_StorageRule(t *testing.T) {
	t.Parallel()

	const limit = 8 * 1024

	tests := []struct {
		name      string
		docs      int
		sameKey   bool
		wantSplit bool
	}{
		{name: "under the limit stays whole", docs: 4},
		{name: "over the limit splits at the median key", docs: 40, wantSplit: true},
		{name: "one logical partition cannot split", docs: 40, sameKey: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newThroughputServer(t, false, false)
			s.backend.SetPartitionLimits(limit, 0)
			s.mustCreate("/dbs", `{"id":"d"}`, nil)
			s.container("c", manualHeader(10000))

			for i := range tt.docs {
				pk := "same"
				if !tt.sameKey {
					pk = "key-" + strconv.Itoa(i)
				}

				require.Equal(t, http.StatusCreated, s.putPK("c", pk, 1).status)
			}

			got := len(s.ranges())
			if tt.wantSplit {
				assert.GreaterOrEqual(t, got, 2)
			} else {
				assert.Equal(t, 1, got)
			}
		})
	}
}

func TestPartitionSplit_StorageChildrenFit(t *testing.T) {
	t.Parallel()

	s := newThroughputServer(t, false, false)
	s.backend.SetPartitionLimits(8*1024, 0)
	s.mustCreate("/dbs", `{"id":"d"}`, nil)
	s.container("c", manualHeader(10000))

	for i := range 40 {
		require.Equal(t, http.StatusCreated, s.putPK("c", "key-"+strconv.Itoa(i), 1).status)
	}

	rs := s.ranges()
	require.Greater(t, len(rs), 1)

	for _, r := range rs[len(rs)-2:] {
		assert.NotEmpty(t, r.parents)
	}

	assert.True(t, slices.IsSorted(func() []string {
		mins := make([]string, len(rs))
		for i, r := range rs {
			mins[i] = r.min
		}

		return mins
	}()), "ranges are listed in hash order")
}

func TestPartitionCharge_CrossPartitionFollowsWork(t *testing.T) {
	t.Parallel()

	s := newThroughputServer(t, true, false)
	s.mustCreate("/dbs", `{"id":"d"}`, nil)
	s.container("c", manualHeader(20000))

	hot, cold := pkInRange(t, 0), pkInRange(t, 1)
	for range 30 {
		require.Equal(t, http.StatusCreated, s.putPK("c", hot, padKB).status)
	}

	s.clock.advance(2 * time.Second)

	throttled := false

	for range 100 {
		count := s.do(http.MethodPost, "/dbs/d/colls/c/docs",
			map[string]string{"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json"},
			`{"query":"SELECT VALUE COUNT(1) FROM c"}`)
		if count.status == http.StatusTooManyRequests {
			throttled = true

			break
		}
	}

	require.True(t, throttled, "the scanned partition runs out of budget")
	assert.Equal(t, http.StatusCreated, s.putPK("c", cold, padKB).status,
		"the partition that held no documents was not charged for the scan")
}

// The expected hashes are the V2 baselines from azcosmos v1.5.0 (internal/epk/testdata, shared with the .NET SDK)
// with the two top bits cleared, which is how the service keeps every hash below its FF sentinel.
func TestPartitionHash_MatchesSDKBaselines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "empty string", key: `""`, want: "32E9366E637A71B4E710384B2F4970A0"},
		{name: "short string", key: `"asdf"`, want: "208BF1D598F5B63AD5520F04FE58C5E8"},
		{name: "99 byte string", key: `"` + strings.Repeat("a", 99) + `"`, want: "3C02F609961C32AAC2B912B940D1209A"},
		{name: "100 byte string", key: `"` + strings.Repeat("a", 100) + `"`, want: "0DBD7D229437BB665DE6417747994D57"},
		{
			name: "hierarchical routes by first component", key: `["asdf","other"]`,
			want: "208BF1D598F5B63AD5520F04FE58C5E8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, cosmosdb.EPKHex(tt.key))
		})
	}
}

func TestPartitionSplit_SnapshotKeepsLayout(t *testing.T) {
	t.Parallel()

	s := newThroughputServer(t, false, false)
	s.mustCreate("/dbs", `{"id":"d"}`, nil)
	s.container("c", manualHeader(10000))
	require.Equal(t, http.StatusOK, s.setOffer(30000).status)
	require.Equal(t, http.StatusOK, s.setOffer(10000).status)

	want := s.ranges()
	require.Len(t, want, 3)

	s2 := newThroughputServer(t, false, false)
	require.NoError(t, s2.backend.Restore(t.Context(), s.backend.Snapshot(t.Context())))
	assert.Equal(t, want, s2.ranges())
}
