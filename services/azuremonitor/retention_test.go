package azuremonitor_test

import (
	"encoding/json/v2"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/azuremonitor"
)

func steppedClock(start time.Time) (func() time.Time, func(time.Duration)) {
	var ns atomic.Int64

	ns.Store(start.UnixNano())

	return func() time.Time { return time.Unix(0, ns.Load()).UTC() },
		func(d time.Duration) { ns.Add(int64(d)) }
}

func recordAt(ts time.Time) string {
	return fmt.Sprintf(`[{"Time":%q,"Computer":"web1","Level":"Info","Count":1,"Value":1,"Ok":true}]`,
		ts.UTC().Format(time.RFC3339))
}

func TestTimeGeneratedWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want   time.Time
		name   string
		offset time.Duration
	}{
		{name: "recent kept", offset: -47 * time.Hour, want: testNow.Add(-47 * time.Hour)},
		{name: "too old replaced", offset: -49 * time.Hour, want: testNow},
		{name: "near future kept", offset: 23 * time.Hour, want: testNow.Add(23 * time.Hour)},
		{name: "far future replaced", offset: 25 * time.Hour, want: testNow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, azuremonitor.DefaultSettings())
			now, _ := steppedClock(testNow)
			e.handler.Backend.SetClock(now)

			s := e.seed(t, "source | extend TimeGenerated = Time")
			require.NoError(t, e.upload(t, s, recordAt(testNow.Add(tt.offset))))

			res := e.run(t, s, "AppEvents_CL | project TimeGenerated")
			raw, err := json.Marshal(res.Tables[0].Rows, jsonx.V1())
			require.NoError(t, err)
			assert.JSONEq(t, fmt.Sprintf(`[[%q]]`, tt.want.Format(time.RFC3339)), string(raw))
		})
	}
}

func TestRetentionHidesExpiredRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		advance time.Duration
	}{
		{name: "within default retention", advance: 29 * 24 * time.Hour, want: `[[1]]`},
		{name: "past default retention", advance: 31 * 24 * time.Hour, want: `[[0]]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, azuremonitor.DefaultSettings())
			now, advance := steppedClock(testNow)
			e.handler.Backend.SetClock(now)

			s := e.seed(t, "source | extend TimeGenerated = Time")
			require.NoError(t, e.upload(t, s, recordAt(testNow.Add(-time.Hour))))

			advance(tt.advance)

			res := e.run(t, s, "AppEvents_CL | count")
			raw, err := json.Marshal(res.Tables[0].Rows, jsonx.V1())
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(raw))
		})
	}
}
