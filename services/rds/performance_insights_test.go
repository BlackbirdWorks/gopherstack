package rds_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPerformanceInsightsEnabledPersisted verifies PerformanceInsightsEnabled is stored and returned.
func TestPerformanceInsightsEnabledPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                    {"CreateDBInstance"},
		"Version":                   {"2014-10-31"},
		"DBInstanceIdentifier":      {"pi-inst"},
		"DBInstanceClass":           {"db.t3.micro"},
		"Engine":                    {"postgres"},
		"MasterUsername":            {"admin"},
		"AllocatedStorage":          {"20"},
		"EnablePerformanceInsights": {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				PerformanceInsightsEnabled bool `xml:"PerformanceInsightsEnabled"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result.DBInstance.PerformanceInsightsEnabled)
}

func TestPerformanceInsights_ReturnsDataPoints(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	now := time.Now().UTC()
	b.SetPerformanceInsightsData(
		"db-instance-1",
		"db.load.avg",
		[]rds.PIDataPoint{{Timestamp: now.Format(time.RFC3339), Value: 1.0}},
	)
	b.CreateDBInstance(
		"db-instance-1",
		"mysql",
		"db.t3.micro",
		"",
		"admin",
		"",
		20,
		rds.DBInstanceOptions{PerformanceInsightsEnabled: true},
	)
	start := now.Add(-time.Hour)

	points, _ := b.GetPerformanceInsightsData("db-instance-1", "db.load.avg", start, now, 60)

	assert.NotEmpty(t, points, "should return data points")
}

func TestPerformanceInsights_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()
	h.Backend.SetPerformanceInsightsData(
		"my-db-instance",
		"db.load.avg",
		[]rds.PIDataPoint{{Timestamp: time.Now().Format(time.RFC3339), Value: 1.0}},
	)
	h.Backend.CreateDBInstance(
		"my-db-instance",
		"mysql",
		"db.t3.micro",
		"",
		"admin",
		"",
		20,
		rds.DBInstanceOptions{PerformanceInsightsEnabled: true},
	)
	start := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 15, 10, 10, 0, 0, time.UTC)

	rec := postRDSForm(t, h,
		"Action=GetPerformanceInsightsMetrics&Version=2014-10-31"+
			"&DBResourceIdentifier=my-db-instance"+
			"&MetricQueries.member.1.Metric=db.load.avg"+
			"&StartTime="+start.Format(time.RFC3339)+
			"&EndTime="+end.Format(time.RFC3339)+
			"&PeriodInSeconds=60")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "GetPerformanceInsightsMetricsResponse")
	assert.Contains(t, respStr, "db.load.avg")
	assert.Contains(t, respStr, "AlignedStartTime")
	assert.Contains(t, respStr, "AlignedEndTime")
	assert.Contains(t, respStr, "DataPoint")
}
