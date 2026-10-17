package route53recoverycluster

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	cc "github.com/blackbirdworks/gopherstack/services/route53recoverycontrolconfig"
)

type wireRoutingControl struct {
	ControlPanelArn     string `json:"ControlPanelArn"`
	ControlPanelName    string `json:"ControlPanelName"`
	Owner               string `json:"Owner"`
	RoutingControlArn   string `json:"RoutingControlArn"`
	RoutingControlName  string `json:"RoutingControlName"`
	RoutingControlState string `json:"RoutingControlState"`
}

type stateEntry struct {
	RoutingControlArn   string `json:"RoutingControlArn"`
	RoutingControlState string `json:"RoutingControlState"`
}

func (h *Handler) dispatch(_ context.Context, action, clusterID string, body []byte) ([]byte, error) {
	bk, err := h.controlConfig()
	if err != nil {
		return nil, err
	}

	var out any

	switch action {
	case "GetRoutingControlState":
		out, err = getState(bk, clusterID, body)
	case "ListRoutingControls":
		out, err = listControls(bk, clusterID, body)
	case "UpdateRoutingControlState":
		out, err = updateOne(bk, clusterID, body)
	case "UpdateRoutingControlStates":
		out, err = updateMany(bk, clusterID, body)
	default:
		return nil, restroute.NewError(http.StatusBadRequest, "ValidationException", "Unknown operation %s", action)
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(out)
}

func decode(body []byte, v any) error {
	if len(body) == 0 {
		return nil
	}

	if err := json.Unmarshal(body, v); err != nil {
		return restroute.Validation("request body is not valid JSON for this operation")
	}

	return nil
}

func getState(bk *cc.InMemoryBackend, clusterID string, body []byte) (any, error) {
	var in struct{ RoutingControlArn string }
	if err := decode(body, &in); err != nil {
		return nil, err
	}

	if in.RoutingControlArn == "" {
		return nil, restroute.Validation("RoutingControlArn is required")
	}

	v, err := bk.RoutingControlState(in.RoutingControlArn, clusterID)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"RoutingControlArn": v.ARN, "RoutingControlName": v.Name, "RoutingControlState": v.State,
	}, nil
}

func listControls(bk *cc.InMemoryBackend, clusterID string, body []byte) (any, error) {
	var in struct {
		ControlPanelArn string
		NextToken       string
		MaxResults      int
	}
	if err := decode(body, &in); err != nil {
		return nil, err
	}

	if in.MaxResults < 0 || in.MaxResults > listMax {
		return nil, restroute.Validation("MaxResults must be between 1 and %d", listMax)
	}

	views, next, err := bk.ListRoutingControlStates(in.ControlPanelArn, clusterID, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]wireRoutingControl, 0, len(views))
	for _, v := range views {
		items = append(items, wireRoutingControl{
			ControlPanelArn: v.PanelARN, ControlPanelName: v.PanelName, Owner: v.Owner,
			RoutingControlArn: v.ARN, RoutingControlName: v.Name, RoutingControlState: v.State,
		})
	}

	out := map[string]any{"RoutingControls": items}
	if next != "" {
		out["NextToken"] = next
	}

	return out, nil
}

func updateOne(bk *cc.InMemoryBackend, clusterID string, body []byte) (any, error) {
	var in struct {
		RoutingControlArn     string
		RoutingControlState   string
		SafetyRulesToOverride []string
	}
	if err := decode(body, &in); err != nil {
		return nil, err
	}

	if in.RoutingControlArn == "" || in.RoutingControlState == "" {
		return nil, restroute.Validation("RoutingControlArn and RoutingControlState are required")
	}

	err := bk.UpdateRoutingControlStates(
		[]cc.StateUpdate{
			{ARN: in.RoutingControlArn, State: in.RoutingControlState},
		},
		in.SafetyRulesToOverride,
		clusterID,
	)

	return struct{}{}, err
}

func updateMany(bk *cc.InMemoryBackend, clusterID string, body []byte) (any, error) {
	var in struct {
		UpdateRoutingControlStateEntries []stateEntry
		SafetyRulesToOverride            []string
	}
	if err := decode(body, &in); err != nil {
		return nil, err
	}

	updates := make([]cc.StateUpdate, 0, len(in.UpdateRoutingControlStateEntries))
	for _, e := range in.UpdateRoutingControlStateEntries {
		updates = append(updates, cc.StateUpdate{ARN: e.RoutingControlArn, State: e.RoutingControlState})
	}

	return struct{}{}, bk.UpdateRoutingControlStates(updates, in.SafetyRulesToOverride, clusterID)
}
