package amp_test

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/amp"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

func nSamples(name string, n int) amp.WriteSeries {
	s := make([]promql.Sample, 0, n)
	for i := range n {
		s = append(s, sample(int64(i), 1))
	}

	return amp.NewSeries(map[string]string{"__name__": name}, s...)
}

func TestIngestionRateQuota(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		writes  []int
		want    []int
		quotas  amp.IngestionQuotas
		advance time.Duration
		last    int
		wantEnd int
	}{
		{
			name:    "bucket drains then refills on the backend clock",
			quotas:  amp.IngestionQuotas{Rate: 10, Burst: 10, ActiveSeries: 100, Enforce: true},
			writes:  []int{10, 1},
			want:    []int{200, 429},
			advance: time.Second,
			last:    10,
			wantEnd: 200,
		},
		{
			name:    "whole request rejected not partially accepted",
			quotas:  amp.IngestionQuotas{Rate: 10, Burst: 10, ActiveSeries: 100, Enforce: true},
			writes:  []int{11, 10},
			want:    []int{429, 200},
			advance: 0,
			last:    1,
			wantEnd: 429,
		},
		{
			name:    "disabled never throttles",
			quotas:  amp.IngestionQuotas{Rate: 1, Burst: 1, ActiveSeries: 1},
			writes:  []int{50, 50},
			want:    []int{200, 200},
			last:    50,
			wantEnd: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetIngestionQuotas(tt.quotas)
			ws := f.workspace(t)

			for i, n := range tt.writes {
				r := f.write(t, ws, nSamples("m"+strconv.Itoa(i), n))
				assert.Equal(t, tt.want[i], r.status, string(r.body))

				if r.status == http.StatusTooManyRequests {
					assert.Contains(t, string(r.body), "ingestion rate limit (")
					assert.Contains(t, string(r.body), "exceeded while adding")
				}
			}

			f.clock.advance(tt.advance)
			r := f.write(t, ws, nSamples("last", tt.last))
			assert.Equal(t, tt.wantEnd, r.status, string(r.body))
		})
	}
}

func TestActiveSeriesQuota(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetIngestionQuotas(amp.IngestionQuotas{Rate: 1000, ActiveSeries: 2, Enforce: true})
	ws := f.workspace(t)

	r := f.write(t, ws, nSamples("a", 1), nSamples("b", 1), nSamples("c", 1))
	assert.Equal(t, http.StatusBadRequest, r.status, string(r.body))
	assert.Contains(t, string(r.body), "per-user series limit of 2 exceeded")
	assert.Equal(t, 2, f.backend.SampleCount(ws))

	r = f.write(t, ws, nSamples("a", 1))
	assert.Equal(t, http.StatusOK, r.status, "existing series still accepted")

	f.clock.advance(3 * time.Hour)

	r = f.write(t, ws, amp.NewSeries(map[string]string{"__name__": "c"}, sample(f.clock.now().UnixMilli(), 1)))
	assert.Equal(t, http.StatusOK, r.status, "series idle for two hours stop counting: "+string(r.body))
}

func TestLabelLimitsAlwaysOn(t *testing.T) {
	t.Parallel()

	many := map[string]string{}
	for i := range 151 {
		many["l"+strconv.Itoa(i)] = "v"
	}

	tests := []struct {
		labels map[string]string
		name   string
		want   int
	}{
		{name: "150 labels", labels: withLabels(150), want: http.StatusOK},
		{name: "151 labels", labels: many, want: http.StatusBadRequest},
		{
			name:   "combined size",
			labels: map[string]string{"a": strings.Repeat("x", 8*1024)},
			want:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)
			r := f.write(t, ws, amp.NewSeries(tt.labels, sample(0, 1)))
			assert.Equal(t, tt.want, r.status, string(r.body))
		})
	}
}

func withLabels(n int) map[string]string {
	out := map[string]string{}
	for i := range n {
		out["l"+strconv.Itoa(i)] = "v"
	}

	return out
}

func TestIngestionRateQuotaConcurrent(t *testing.T) {
	t.Parallel()

	const capacity = 100

	f := newFixture(t)
	f.backend.SetIngestionQuotas(
		amp.IngestionQuotas{Rate: capacity, Burst: capacity, ActiveSeries: 1000, Enforce: true},
	)
	ws := f.workspace(t)

	var accepted atomic.Int64

	var wg sync.WaitGroup

	for g := range 8 {
		wg.Go(func() {
			for i := range 30 {
				_, err := f.backend.Ingest(t.Context(), ws, []amp.WriteSeries{nSamples("c"+strconv.Itoa(g*100+i), 5)})
				if err == nil {
					accepted.Add(5)
				}
			}
		})
	}

	wg.Wait()
	assert.Equal(t, int64(capacity), accepted.Load(), "a frozen clock admits exactly one bucket of samples")
}

type fakeConfig struct{ s amp.Settings }

func (c fakeConfig) GetAMPSettings() amp.Settings { return c.s }

func TestProviderAppliesIngestionSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  amp.Settings
		want int
	}{
		{
			name: "enforced",
			cfg:  amp.Settings{IngestionRate: 5, ActiveSeries: 10, EnforceIngestionQuotas: true},
			want: 429,
		},
		{name: "default off", cfg: amp.Settings{IngestionRate: 5, ActiveSeries: 10}, want: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reg, err := (&amp.Provider{}).Init(&service.AppContext{Config: fakeConfig{s: tt.cfg}})
			require.NoError(t, err)

			h, ok := reg.(*amp.Handler)
			require.True(t, ok)

			f := newFixtureFromBackend(t, h.Backend, &fakeClock{t: time.Unix(0, 0).UTC()})
			ws := f.workspace(t)
			assert.Equal(t, tt.want, f.write(t, ws, nSamples("m", 6)).status)
		})
	}
}
