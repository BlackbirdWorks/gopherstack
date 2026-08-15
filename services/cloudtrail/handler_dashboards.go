package cloudtrail

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultDashboardsPageSize is the ListDashboards page size used when the
// caller omits MaxResults.
const defaultDashboardsPageSize = 1000

// widgetBody mirrors the real RequestWidget (input) / Widget (output) shape.
type widgetBody struct {
	ViewProperties  map[string]string `json:"ViewProperties"`
	QueryAlias      string            `json:"QueryAlias"`
	QueryStatement  string            `json:"QueryStatement"`
	QueryParameters []string          `json:"QueryParameters"`
}

// refreshScheduleBody mirrors the real RefreshSchedule shape.
type refreshScheduleBody struct {
	Frequency *struct {
		Unit  string `json:"Unit"`
		Value int32  `json:"Value"`
	} `json:"Frequency"`
	Status    string `json:"Status"`
	TimeOfDay string `json:"TimeOfDay"`
}

func toBackendWidgets(in []widgetBody) []Widget {
	if in == nil {
		return nil
	}

	out := make([]Widget, 0, len(in))
	for _, w := range in {
		out = append(out, Widget(w))
	}

	return out
}

func toBackendRefreshSchedule(in *refreshScheduleBody) *RefreshSchedule {
	if in == nil {
		return nil
	}

	rs := &RefreshSchedule{Status: in.Status, TimeOfDay: in.TimeOfDay}
	if in.Frequency != nil {
		rs.Frequency = &RefreshScheduleFrequency{Unit: in.Frequency.Unit, Value: in.Frequency.Value}
	}

	return rs
}

// --- CreateDashboard ---

type createDashboardBody struct {
	RefreshSchedule *refreshScheduleBody `json:"RefreshSchedule"`
	Name            string               `json:"Name"`
	Type            string               `json:"Type"`
	Tags            []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
	Widgets                      []widgetBody `json:"Widgets"`
	TerminationProtectionEnabled bool         `json:"TerminationProtectionEnabled"`
}

func (h *Handler) handleCreateDashboard(c *echo.Context, body []byte) error {
	var in createDashboardBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	kv := make(map[string]string, len(in.Tags))
	for _, tag := range in.Tags {
		kv[tag.Key] = tag.Value
	}

	d, err := h.Backend.CreateDashboard(
		in.Name, in.Type, kv,
		toBackendWidgets(in.Widgets),
		toBackendRefreshSchedule(in.RefreshSchedule),
		in.TerminationProtectionEnabled,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, dashCreateToMap(d))
}

// --- DeleteDashboard ---

type deleteDashboardBody struct {
	DashboardID string `json:"DashboardId"`
}

func (h *Handler) handleDeleteDashboard(c *echo.Context, body []byte) error {
	var in deleteDashboardBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.DeleteDashboard(in.DashboardID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- GetDashboard ---

type getDashboardBody struct {
	DashboardID string `json:"DashboardId"`
}

func (h *Handler) handleGetDashboard(c *echo.Context, body []byte) error {
	var in getDashboardBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	d, err := h.Backend.GetDashboard(in.DashboardID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, dashGetToMap(d))
}

// --- UpdateDashboard ---

// updateDashboardBody matches the real UpdateDashboardInput exactly:
// DashboardId, RefreshSchedule, TerminationProtectionEnabled, Widgets.
// There is no Name field on the real input -- dashboards cannot be renamed
// (a previous version of this backend accepted one; see UpdateDashboard's
// doc comment in dashboards.go).
type updateDashboardBody struct {
	DashboardID                  string               `json:"DashboardId"`
	RefreshSchedule              *refreshScheduleBody `json:"RefreshSchedule"`
	TerminationProtectionEnabled *bool                `json:"TerminationProtectionEnabled"`
	Widgets                      []widgetBody         `json:"Widgets"`
}

func (h *Handler) handleUpdateDashboard(c *echo.Context, body []byte) error {
	var in updateDashboardBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	d, err := h.Backend.UpdateDashboard(
		in.DashboardID,
		toBackendWidgets(in.Widgets),
		toBackendRefreshSchedule(in.RefreshSchedule),
		in.TerminationProtectionEnabled,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, dashUpdateToMap(d))
}

// --- ListDashboards ---

type listDashboardsBody struct {
	NamePrefix string `json:"NamePrefix"`
	Type       string `json:"Type"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

func (h *Handler) handleListDashboards(c *echo.Context, body []byte) error {
	var in listDashboardsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterCombinationException", "invalid request body"),
			)
		}
	}

	list := h.Backend.ListDashboards()
	filtered := make([]*Dashboard, 0, len(list))

	for _, d := range list {
		if in.Type != "" && d.Type != in.Type {
			continue
		}
		if in.NamePrefix != "" && !strings.HasPrefix(d.Name, in.NamePrefix) {
			continue
		}
		filtered = append(filtered, d)
	}

	p := page.New(filtered, in.NextToken, in.MaxResults, defaultDashboardsPageSize)
	items := make([]map[string]any, 0, len(p.Data))
	for _, d := range p.Data {
		items = append(items, dashDetailToMap(d))
	}

	resp := map[string]any{"Dashboards": items}
	if p.Next != "" {
		resp["NextToken"] = p.Next
	}

	return c.JSON(http.StatusOK, resp)
}

// dashDetailToMap renders the DashboardDetail shape used by ListDashboards
// (real API: only DashboardArn and Type per entry).
func dashDetailToMap(d *Dashboard) map[string]any {
	return map[string]any{
		keyDashboardArn: d.DashboardARN,
		keyType:         d.Type,
	}
}

// --- StartDashboardRefresh ---

type startDashboardRefreshBody struct {
	DashboardID string `json:"DashboardId"`
}

func (h *Handler) handleStartDashboardRefresh(c *echo.Context, body []byte) error {
	var in startDashboardRefreshBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	d, err := h.Backend.StartDashboardRefresh(in.DashboardID)
	if err != nil {
		return h.handleError(c, err)
	}

	// StartDashboardRefreshOutput has exactly one field, RefreshId -- no
	// DashboardArn/Status (a previous version of this handler returned
	// those instead, neither of which exists on the real output).
	return c.JSON(http.StatusOK, map[string]any{
		"RefreshId": d.LastRefreshID,
	})
}

// widgetsToMaps/refreshScheduleToMap render the backend's Widget/
// RefreshSchedule types into their real wire shapes.
func widgetsToMaps(widgets []Widget) []map[string]any {
	if widgets == nil {
		return nil
	}

	out := make([]map[string]any, 0, len(widgets))
	for _, w := range widgets {
		m := map[string]any{"QueryStatement": w.QueryStatement}
		if w.QueryAlias != "" {
			m["QueryAlias"] = w.QueryAlias
		}
		if w.QueryParameters != nil {
			m["QueryParameters"] = w.QueryParameters
		}
		if w.ViewProperties != nil {
			m["ViewProperties"] = w.ViewProperties
		}
		out = append(out, m)
	}

	return out
}

func refreshScheduleToMap(rs *RefreshSchedule) map[string]any {
	if rs == nil {
		return nil
	}

	m := map[string]any{}
	if rs.Status != "" {
		m["Status"] = rs.Status
	}
	if rs.TimeOfDay != "" {
		m["TimeOfDay"] = rs.TimeOfDay
	}
	if rs.Frequency != nil {
		m["Frequency"] = map[string]any{
			"Unit":   rs.Frequency.Unit,
			keyValue: rs.Frequency.Value,
		}
	}

	return m
}

// dashTagsList renders a Dashboard's tags as the TagsList shape
// (CreateDashboardOutput's only tag field; []types.Tag{Key,Value}).
func dashTagsList(d *Dashboard) []map[string]string {
	if d.Tags == nil || d.Tags.Len() == 0 {
		return nil
	}

	kv := d.Tags.Clone()
	out := make([]map[string]string, 0, len(kv))
	for k, v := range kv {
		out = append(out, map[string]string{keyKey: k, keyValue: v})
	}

	return out
}

// dashCreateToMap renders CreateDashboardOutput: DashboardArn, Name,
// RefreshSchedule, TagsList, TerminationProtectionEnabled, Type, Widgets
// (cloudtrail@v1.58.4 api_op_CreateDashboard.go). No Status, no
// Created/UpdatedTimestamp, no LastRefreshId/LastRefreshFailureReason --
// none of those exist on the real output.
func dashCreateToMap(d *Dashboard) map[string]any {
	m := map[string]any{
		keyDashboardArn:                 d.DashboardARN,
		keyName:                         d.Name,
		keyType:                         d.Type,
		keyTerminationProtectionEnabled: d.TerminationProtectionEnabled,
	}
	if widgets := widgetsToMaps(d.Widgets); widgets != nil {
		m["Widgets"] = widgets
	}
	if rs := refreshScheduleToMap(d.RefreshSchedule); rs != nil {
		m["RefreshSchedule"] = rs
	}
	if tl := dashTagsList(d); tl != nil {
		m["TagsList"] = tl
	}

	return m
}

// dashGetToMap renders GetDashboardOutput: CreatedTimestamp, DashboardArn,
// LastRefreshFailureReason, LastRefreshId, RefreshSchedule, Status,
// TerminationProtectionEnabled, Type, UpdatedTimestamp, Widgets
// (cloudtrail@v1.58.4 api_op_GetDashboard.go). No Name, no TagsList --
// neither exists on the real output.
func dashGetToMap(d *Dashboard) map[string]any {
	m := map[string]any{
		keyDashboardArn:                 d.DashboardARN,
		keyType:                         d.Type,
		keyStatus:                       d.Status,
		keyTerminationProtectionEnabled: d.TerminationProtectionEnabled,
		keyCreatedTimestamp:             float64(d.CreatedTimestamp.Unix()),
		keyUpdatedTimestamp:             float64(d.UpdatedTimestamp.Unix()),
	}
	if widgets := widgetsToMaps(d.Widgets); widgets != nil {
		m["Widgets"] = widgets
	}
	if rs := refreshScheduleToMap(d.RefreshSchedule); rs != nil {
		m["RefreshSchedule"] = rs
	}
	if d.LastRefreshID != "" {
		m["LastRefreshId"] = d.LastRefreshID
	}
	if d.LastRefreshFailureReason != "" {
		m["LastRefreshFailureReason"] = d.LastRefreshFailureReason
	}

	return m
}

// dashUpdateToMap renders UpdateDashboardOutput: CreatedTimestamp,
// DashboardArn, Name, RefreshSchedule, TerminationProtectionEnabled, Type,
// UpdatedTimestamp, Widgets (cloudtrail@v1.58.4 api_op_UpdateDashboard.go).
// No Status, no TagsList, no LastRefreshId/LastRefreshFailureReason -- none
// of those exist on the real output.
func dashUpdateToMap(d *Dashboard) map[string]any {
	m := map[string]any{
		keyDashboardArn:                 d.DashboardARN,
		keyName:                         d.Name,
		keyType:                         d.Type,
		keyTerminationProtectionEnabled: d.TerminationProtectionEnabled,
		keyCreatedTimestamp:             float64(d.CreatedTimestamp.Unix()),
		keyUpdatedTimestamp:             float64(d.UpdatedTimestamp.Unix()),
	}
	if widgets := widgetsToMaps(d.Widgets); widgets != nil {
		m["Widgets"] = widgets
	}
	if rs := refreshScheduleToMap(d.RefreshSchedule); rs != nil {
		m["RefreshSchedule"] = rs
	}

	return m
}
