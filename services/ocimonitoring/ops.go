package ocimonitoring

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateAlarm: (*Handler).createAlarm, opGetAlarm: (*Handler).getAlarm, opListAlarms: (*Handler).listAlarms,
		opListAlarmsStatus: (*Handler).listAlarmsStatus, opUpdateAlarm: (*Handler).updateAlarm,
		opDeleteAlarm: (*Handler).deleteAlarm, opChangeAlarmComp: (*Handler).changeAlarmCompartment,
		opGetAlarmHistory: (*Handler).getAlarmHistory, opPostMetricData: (*Handler).postMetricData,
		opListMetrics: (*Handler).listMetrics, opSummarizeMetrics: (*Handler).summarizeMetricsData,
	}
}

func reply[T any](q *req, w T, etag string, err error) error {
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, etag)
}

// listReply writes a page as a bare JSON array, the shape the Monitoring list operations declare.
func listReply[T any](q *req, pg page.Page[T], err error) error {
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	data := pg.Data
	if data == nil {
		data = []T{}
	}

	return q.json(http.StatusOK, data, "")
}
