package amp_test

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/aws/aws-sdk-go-v2/service/amp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amp"
)

func newDatedFixture(t *testing.T) *fixture {
	t.Helper()

	return newFixtureFromBackend(t, amp.NewInMemoryBackend(testAccount, "us-east-1"),
		&fakeClock{t: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)})
}

func (f *fixture) configure(t *testing.T, in *ampsdk.UpdateWorkspaceConfigurationInput) {
	t.Helper()

	_, err := f.client.UpdateWorkspaceConfiguration(t.Context(), in)
	require.NoError(t, err)
	f.settle()
}

func (f *fixture) ago(d time.Duration) int64 { return f.clock.now().Add(-d).UnixMilli() }

func TestRetentionEnforcement(t *testing.T) {
	t.Parallel()

	const day = 24 * time.Hour

	lbl := map[string]string{"__name__": "m"}

	tests := []struct {
		run  func(t *testing.T, f *fixture, ws string)
		name string
	}{
		{name: "default 150 days on ingest", run: func(t *testing.T, f *fixture, ws string) {
			t.Helper()

			r := f.write(t, ws, amp.NewSeries(lbl,
				sample(f.ago(200*day), 1), sample(f.ago(100*day), 2), sample(f.ago(time.Hour), 3)))
			require.Equal(t, http.StatusOK, r.status)
			assert.Equal(t, 2, f.backend.SampleCount(ws))
		}},
		{name: "configured retention on ingest", run: func(t *testing.T, f *fixture, ws string) {
			t.Helper()

			f.configure(t, &ampsdk.UpdateWorkspaceConfigurationInput{
				WorkspaceId: aws.String(ws), RetentionPeriodInDays: aws.Int32(2),
			})

			r := f.write(t, ws, amp.NewSeries(lbl, sample(f.ago(3*day), 1), sample(f.ago(day), 2)))
			require.Equal(t, http.StatusOK, r.status)
			assert.Equal(t, 1, f.backend.SampleCount(ws))
		}},
		{name: "query hides expired before the janitor runs", run: func(t *testing.T, f *fixture, ws string) {
			t.Helper()

			f.configure(t, &ampsdk.UpdateWorkspaceConfigurationInput{
				WorkspaceId: aws.String(ws), RetentionPeriodInDays: aws.Int32(1),
			})

			at := f.ago(time.Minute)
			require.Equal(t, http.StatusOK, f.write(t, ws, amp.NewSeries(lbl, sample(at, 7))).status)

			q := url.Values{"query": {"m"}, "time": {strconv.FormatInt(at/1000, 10)}}

			_, pr := f.query(t, ws, "query", q)
			assert.Contains(t, string(pr.Data.Result), `"7"`)

			f.clock.advance(day)

			_, pr = f.query(t, ws, "query", q)
			assert.JSONEq(t, `[]`, string(pr.Data.Result))
			assert.Equal(t, 1, f.backend.SampleCount(ws), "not yet pruned")
		}},
		{name: "janitor prunes expired samples and empty series", run: func(t *testing.T, f *fixture, ws string) {
			t.Helper()

			f.configure(t, &ampsdk.UpdateWorkspaceConfigurationInput{
				WorkspaceId: aws.String(ws), RetentionPeriodInDays: aws.Int32(1),
			})

			keep := map[string]string{"__name__": "keep"}
			require.Equal(t, http.StatusOK, f.write(t, ws,
				amp.NewSeries(lbl, sample(f.ago(time.Hour), 1)),
				amp.NewSeries(keep, sample(f.ago(time.Hour), 1)),
			).status)

			f.clock.advance(23 * time.Hour)
			require.Equal(t, http.StatusOK, f.write(t, ws, amp.NewSeries(keep, sample(f.ago(0), 2))).status)
			require.Equal(t, 3, f.backend.SampleCount(ws))

			f.clock.advance(2 * time.Hour)
			f.backend.Tick(t.Context())

			assert.Equal(t, 1, f.backend.SampleCount(ws))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDatedFixture(t)
			tt.run(t, f, f.workspace(t))
		})
	}
}

func TestLimitsPerLabelSetEnforcement(t *testing.T) {
	t.Parallel()

	series := func(team, id string) amp.WriteSeries {
		return amp.NewSeries(map[string]string{"__name__": "m", "team": team, "id": id}, sample(1000, 1))
	}

	tests := []struct {
		run  func(t *testing.T, f *fixture, ws string)
		name string
	}{
		{name: "new series over the cap are refused", run: func(t *testing.T, f *fixture, ws string) {
			t.Helper()

			r := f.write(t, ws, series("a", "1"), series("a", "2"), series("a", "3"), series("b", "1"))
			assert.Equal(t, http.StatusBadRequest, r.status, string(r.body))
			assert.Equal(t, 3, f.backend.SampleCount(ws), "two team=a series and the team=b series are kept")
		}},
		{name: "existing series keep ingesting", run: func(t *testing.T, f *fixture, ws string) {
			t.Helper()

			require.Equal(t, http.StatusOK, f.write(t, ws, series("a", "1"), series("a", "2")).status)

			again := amp.NewSeries(map[string]string{"__name__": "m", "team": "a", "id": "1"}, sample(2000, 2))
			require.Equal(t, http.StatusOK, f.write(t, ws, again).status)
			assert.Equal(t, 3, f.backend.SampleCount(ws))
		}},
		{name: "other label sets are unaffected", run: func(t *testing.T, f *fixture, ws string) {
			t.Helper()

			r := f.write(t, ws, series("b", "1"), series("b", "2"), series("b", "3"))
			assert.Equal(t, http.StatusOK, r.status, string(r.body))
			assert.Equal(t, 3, f.backend.SampleCount(ws))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)
			f.configure(t, &ampsdk.UpdateWorkspaceConfigurationInput{
				WorkspaceId: aws.String(ws),
				LimitsPerLabelSet: []types.LimitsPerLabelSet{{
					LabelSet: map[string]string{"team": "a"},
					Limits:   &types.LimitsPerLabelSetEntry{MaxSeries: aws.Int64(2)},
				}},
			})
			tt.run(t, f, ws)
		})
	}
}
