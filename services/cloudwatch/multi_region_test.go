package cloudwatch_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

const mrHome = "us-east-1"

func newRegionHandler() *cloudwatch.Handler {
	h := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend())
	h.EnableRegions()

	return h
}

func regionForm(t *testing.T, h *cloudwatch.Handler, region string, form url.Values) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return rec.Body.String()
}

func putAlarm(t *testing.T, h *cloudwatch.Handler, region, name string) {
	t.Helper()

	regionForm(t, h, region, url.Values{
		"Action": {"PutMetricAlarm"}, "AlarmName": {name}, "MetricName": {"m"}, "Namespace": {"n"},
		"Statistic": {"Average"}, "Period": {"60"}, "EvaluationPeriods": {"1"}, "Threshold": {"1"},
		"ComparisonOperator": {"GreaterThanThreshold"},
	})
}

type describeAlarmsResp struct {
	Alarms []struct {
		Name string `xml:"AlarmName"`
		ARN  string `xml:"AlarmArn"`
	} `xml:"DescribeAlarmsResult>MetricAlarms>member"`
}

func describeAlarms(t *testing.T, h *cloudwatch.Handler, region string) describeAlarmsResp {
	t.Helper()

	var out describeAlarmsResp

	body := regionForm(t, h, region, url.Values{"Action": {"DescribeAlarms"}})
	require.NoError(t, xml.Unmarshal([]byte(body), &out), body)

	return out
}

func alarmNames(t *testing.T, h *cloudwatch.Handler, region string) []string {
	t.Helper()

	alarms := describeAlarms(t, h, region).Alarms
	names := make([]string, 0, len(alarms))

	for _, a := range alarms {
		names = append(names, a.Name)
	}

	return names
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{"us-east-1", "eu-west-1"}},
		{name: "three-regions", regions: []string{"us-east-1", "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()

			for _, r := range tc.regions {
				putAlarm(t, h, r, "shared")
				putAlarm(t, h, r, "only-"+r)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, alarmNames(t, h, r))

				for _, a := range describeAlarms(t, h, r).Alarms {
					assert.Contains(t, a.ARN, ":"+r+":")
				}
			}
		})
	}
}

func TestHandler_SubscribeAlarmStateChangeRoutesByARNRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home", region: mrHome},
		{name: "peer", region: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()
			putAlarm(t, h, tc.region, "a")

			alarms := describeAlarms(t, h, tc.region).Alarms
			require.Len(t, alarms, 1)

			got := make(chan string, 1)
			unsub := h.SubscribeAlarmStateChange(alarms[0].ARN, func(s string) { got <- s })

			defer unsub()

			regionForm(t, h, tc.region, url.Values{
				"Action": {"SetAlarmState"}, "AlarmName": {"a"}, "StateValue": {"ALARM"}, "StateReason": {"t"},
			})

			assert.Equal(t, "ALARM", <-got)
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		remote bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "peer-round-trips", remote: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler()
			putAlarm(t, src, mrHome, "home")

			if tc.remote {
				putAlarm(t, src, "eu-west-1", "eu")
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)
			assert.Equal(t, tc.remote, strings.Contains(string(snap), `"regions"`))

			dst := newRegionHandler()
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, alarmNames(t, dst, mrHome))
			assert.Equal(t, tc.remote, len(alarmNames(t, dst, "eu-west-1")) == 1)

			plain := cloudwatch.NewHandler(cloudwatch.NewInMemoryBackend())
			require.NoError(t, plain.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, alarmNames(t, plain, mrHome))
		})
	}
}
