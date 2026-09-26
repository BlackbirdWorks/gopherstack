package cloudwatch_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// normalizePutMetricDataGolden blanks the random RequestId and the
// now-relative datapoint Timestamp.
func normalizePutMetricDataGolden(b []byte) []byte {
	re := regexp.MustCompile(`<RequestId>[^<]*</RequestId>`)
	b = re.ReplaceAll(b, []byte(`<RequestId>NORMALIZED</RequestId>`))

	// The datapoint window is relative to now, so its bucket start is too.
	ts := regexp.MustCompile(`<Timestamp>[^<]*</Timestamp>`)

	return ts.ReplaceAll(b, []byte(`<Timestamp>NORMALIZED</Timestamp>`))
}

// TestPutMetricData_DatapointsGolden writes CwMaxMetricDataPointsForTest+50
// points to a single metric -- crossing the retention cap by 50, so the
// oldest 50 must be evicted -- then reads them back via GetMetricStatistics
// bucketed into one window covering the whole run. The aggregated
// Sum/SampleCount/Minimum/Maximum pin down exactly which 1000 points
// survived the cap-and-evict, so a byte comparison of this response against
// the checked-in golden (captured from the pre-optimization code) catches
// any change to eviction order from the storeDatum rewrite (append+copy vs.
// in-place shift), not just whether the cap is respected.
func TestPutMetricData_DatapointsGolden(t *testing.T) {
	t.Parallel()

	bk := cloudwatch.NewInMemoryBackend()
	h := cloudwatch.NewHandler(bk)

	const (
		namespace  = "Golden/Namespace"
		metricName = "Requests"
		writes     = cloudwatch.CwMaxMetricDataPointsForTest + 50
	)

	// Buckets are epoch-aligned; start on a period boundary so every write lands in one bucket.
	period := int64(writes + 7200)
	base := time.Unix(time.Now().Add(-13*24*time.Hour).Unix()/period*period, 0).UTC()
	for i := range writes {
		v := float64(i)
		err := bk.PutMetricData(namespace, []cloudwatch.MetricDatum{{
			MetricName: metricName,
			Timestamp:  base.Add(time.Duration(i) * time.Second),
			Value:      v,
			HasValue:   true,
			Count:      1,
			Sum:        v,
			Min:        v,
			Max:        v,
		}})
		require.NoError(t, err)
	}

	e := echo.New()
	start := base.Add(-1 * time.Hour).Format(time.RFC3339)
	end := base.Add(time.Duration(writes)*time.Second + time.Hour).Format(time.RFC3339)

	body := "Action=GetMetricStatistics" +
		"&Namespace=" + namespace +
		"&MetricName=" + metricName +
		"&StartTime=" + start +
		"&EndTime=" + end +
		"&Period=" + strconv.FormatInt(period, 10) +
		"&Statistics.member.1=Sum" +
		"&Statistics.member.2=SampleCount" +
		"&Statistics.member.3=Minimum" +
		"&Statistics.member.4=Maximum" +
		"&Statistics.member.5=Average"

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	got := normalizePutMetricDataGolden(rec.Body.Bytes())

	want, err := os.ReadFile("testdata/put_metric_data_golden.xml")
	require.NoError(t, err)

	require.Equal(t, string(want), string(got))
}
