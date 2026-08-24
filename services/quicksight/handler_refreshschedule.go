package quicksight

import (
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// JSON response/body keys used only by dataset refresh schedule/properties
// operations.
const (
	keyScheduleField            = "Schedule"
	keyScheduleIDField          = "ScheduleId"
	keyStartAfterDateTime       = "StartAfterDateTime"
	keyDataSetRefreshProperties = "DataSetRefreshProperties"
	keyRefreshConfiguration     = "RefreshConfiguration"
	keyFailureConfiguration     = "FailureConfiguration"
)

func isRefreshScheduleOp(op string) bool {
	switch op {
	case opCreateRefreshSchedule, opDescribeRefreshSchedule, opUpdateRefreshSchedule,
		opDeleteRefreshSchedule, opListRefreshSchedules,
		opPutDataSetRefreshProperties, opDescribeDataSetRefreshProps, opDeleteDataSetRefreshProps:
		return true
	}

	return false
}

func (h *Handler) dispatchRefreshSchedule(c *echo.Context, op string) error {
	switch op {
	case opCreateRefreshSchedule:
		return h.handleCreateDataSetRefreshSchedule(c)
	case opDescribeRefreshSchedule:
		return h.handleDescribeDataSetRefreshSchedule(c)
	case opUpdateRefreshSchedule:
		return h.handleUpdateDataSetRefreshSchedule(c)
	case opDeleteRefreshSchedule:
		return h.handleDeleteDataSetRefreshSchedule(c)
	case opListRefreshSchedules:
		return h.handleListDataSetRefreshSchedules(c)
	case opPutDataSetRefreshProperties:
		return h.handlePutDataSetRefreshProperties(c)
	case opDescribeDataSetRefreshProps:
		return h.handleDescribeDataSetRefreshProperties(c)
	case opDeleteDataSetRefreshProps:
		return h.handleDeleteDataSetRefreshProperties(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func dataSetScheduleFieldsFromBody(body map[string]any) (string, string, time.Time, map[string]any) {
	sched, _ := body[keyScheduleField].(map[string]any)
	if sched == nil {
		sched = body
	}

	return strField(sched, keyScheduleIDField),
		strField(sched, keyRefreshType),
		epochField(sched, keyStartAfterDateTime),
		mapField(sched, "ScheduleFrequency")
}

func dataSetRefreshScheduleToMap(s *RefreshSchedule) map[string]any {
	m := map[string]any{
		keyScheduleIDField: s.ScheduleID,
		keyArn:             s.Arn,
		keyRefreshType:     s.RefreshType,
	}
	if !s.StartAfterDateTime.IsZero() {
		m[keyStartAfterDateTime] = awstime.Epoch(s.StartAfterDateTime)
	}
	if s.ScheduleFrequency != nil {
		m["ScheduleFrequency"] = s.ScheduleFrequency
	}

	return m
}

func (h *Handler) handleCreateDataSetRefreshSchedule(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	datasetID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	scheduleID, refreshType, startAfter, freq := dataSetScheduleFieldsFromBody(body)

	s, err := h.Backend.CreateRefreshSchedule(accountID, datasetID, scheduleID, refreshType, startAfter, freq)
	if err != nil {
		if errors.Is(err, ErrRefreshScheduleAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:             s.Arn,
		keyScheduleIDField: s.ScheduleID,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleDescribeDataSetRefreshSchedule(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	datasetID := seg(segs, segResID)
	scheduleID := seg(segs, segSubResID)

	s, err := h.Backend.DescribeRefreshSchedule(accountID, datasetID, scheduleID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:             s.Arn,
		keyRefreshSchedule: dataSetRefreshScheduleToMap(s),
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleUpdateDataSetRefreshSchedule(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	datasetID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	scheduleID, refreshType, startAfter, freq := dataSetScheduleFieldsFromBody(body)

	s, err := h.Backend.UpdateRefreshSchedule(accountID, datasetID, scheduleID, refreshType, startAfter, freq)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:             s.Arn,
		keyScheduleIDField: s.ScheduleID,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleDeleteDataSetRefreshSchedule(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	datasetID := seg(segs, segResID)
	scheduleID := seg(segs, segSubResID)

	s, err := h.Backend.DeleteRefreshSchedule(accountID, datasetID, scheduleID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:             s.Arn,
		keyScheduleIDField: s.ScheduleID,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleListDataSetRefreshSchedules(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	datasetID := seg(segs, segResID)

	schedules, err := h.Backend.ListRefreshSchedules(accountID, datasetID)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(schedules))
	for _, s := range schedules {
		items = append(items, dataSetRefreshScheduleToMap(s))
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRefreshSchedules: items,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	})
}

// ---- DataSet refresh properties ----

func (h *Handler) handlePutDataSetRefreshProperties(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	datasetID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	props, _ := body[keyDataSetRefreshProperties].(map[string]any)
	if props == nil {
		props = body
	}

	_, err = h.Backend.PutDataSetRefreshProperties(
		accountID, datasetID, mapField(props, keyRefreshConfiguration), mapField(props, keyFailureConfiguration),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeDataSetRefreshProperties(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	datasetID := seg(segs, segResID)

	props, err := h.Backend.DescribeDataSetRefreshProperties(accountID, datasetID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDataSetRefreshProperties: map[string]any{
			keyRefreshConfiguration: props.RefreshConfiguration,
			keyFailureConfiguration: props.FailureConfiguration,
		},
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteDataSetRefreshProperties(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	datasetID := seg(segs, segResID)

	if err := h.Backend.DeleteDataSetRefreshProperties(accountID, datasetID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}
