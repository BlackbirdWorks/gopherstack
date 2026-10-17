package arczonalshift

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func pageParams(req *restroute.Request) (string, int, error) {
	m, err := restroute.MaxResultsQuery(req.Query, "maxResults")
	if err != nil {
		return "", 0, invalidReason("Other", "maxResults must be an integer")
	}

	return req.Query.Get("nextToken"), m, nil
}

func items[T any](list []T, next string) map[string]any {
	if list == nil {
		list = []T{}
	}

	out := map[string]any{"items": list}
	if next != "" {
		out["nextToken"] = next
	}

	return out
}

func (h *Handler) startZonalShift(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		AwayFrom, Comment, ExpiresIn, ResourceIdentifier string
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.StartZonalShift(in.ResourceIdentifier, in.AwayFrom, in.Comment, in.ExpiresIn)

	return v, http.StatusCreated, err
}

func (h *Handler) listZonalShifts(_ context.Context, req *restroute.Request) (any, int, error) {
	token, m, err := pageParams(req)
	if err != nil {
		return nil, 0, err
	}

	list, next, err := h.Backend.ListZonalShifts(req.Query.Get("status"), req.Query.Get("resourceIdentifier"), token, m)

	return items(list, next), 0, err
}

func (h *Handler) updateZonalShift(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ Comment, ExpiresIn string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.UpdateZonalShift(req.Params["zonalShiftId"], in.Comment, in.ExpiresIn)

	return v, 0, err
}

func (h *Handler) cancelZonalShift(_ context.Context, req *restroute.Request) (any, int, error) {
	v, err := h.Backend.CancelZonalShift(req.Params["zonalShiftId"])

	return v, 0, err
}

func (h *Handler) startPracticeRun(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ AwayFrom, Comment, ResourceIdentifier string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.StartPracticeRun(in.ResourceIdentifier, in.AwayFrom, in.Comment)

	return v, 0, err
}

func (h *Handler) cancelPracticeRun(_ context.Context, req *restroute.Request) (any, int, error) {
	v, err := h.Backend.CancelPracticeRun(req.Params["zonalShiftId"])

	return v, 0, err
}

type practiceBody struct {
	ResourceIdentifier string   `json:"resourceIdentifier"`
	OutcomeAlarms      []Alarm  `json:"outcomeAlarms"`
	BlockingAlarms     []Alarm  `json:"blockingAlarms"`
	AllowedWindows     []string `json:"allowedWindows"`
	BlockedWindows     []string `json:"blockedWindows"`
	BlockedDates       []string `json:"blockedDates"`
}

func (p practiceBody) config(resource string) PracticeConfig {
	return PracticeConfig{
		Resource: resource, OutcomeAlarms: p.OutcomeAlarms, BlockingAlarms: p.BlockingAlarms,
		AllowedWindows: p.AllowedWindows, BlockedWindows: p.BlockedWindows, BlockedDates: p.BlockedDates,
	}
}

func (h *Handler) createPractice(_ context.Context, req *restroute.Request) (any, int, error) {
	var in practiceBody
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.CreatePracticeRunConfiguration(in.config(in.ResourceIdentifier))

	return v, http.StatusCreated, err
}

func (h *Handler) updatePractice(_ context.Context, req *restroute.Request) (any, int, error) {
	var in practiceBody
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	var present map[string]json.RawMessage
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &present)
	}

	set := map[string]bool{}
	for k := range present {
		set[k] = true
	}

	v, err := h.Backend.UpdatePracticeRunConfiguration(
		req.Params["resourceIdentifier"],
		in.config(req.Params["resourceIdentifier"]),
		set,
	)

	return v, 0, err
}

func (h *Handler) deletePractice(_ context.Context, req *restroute.Request) (any, int, error) {
	v, err := h.Backend.DeletePracticeRunConfiguration(req.Params["resourceIdentifier"])

	return v, 0, err
}

func (h *Handler) listManagedResources(ctx context.Context, req *restroute.Request) (any, int, error) {
	token, m, err := pageParams(req)
	if err != nil {
		return nil, 0, err
	}

	region, _ := ctx.Value(regionKey{}).(string)
	list, next, err := h.Backend.ListManagedResources(region, token, m)

	return items(list, next), 0, err
}

func (h *Handler) getManagedResource(_ context.Context, req *restroute.Request) (any, int, error) {
	v, err := h.Backend.GetManagedResource(req.Params["resourceIdentifier"])

	return v, 0, err
}

func (h *Handler) updateAutoshift(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ ZonalAutoshiftStatus string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	res := req.Params["resourceIdentifier"]
	status, err := h.Backend.UpdateZonalAutoshiftConfiguration(res, in.ZonalAutoshiftStatus)

	return map[string]string{"resourceIdentifier": res, "zonalAutoshiftStatus": status}, 0, err
}

func (h *Handler) listAutoshifts(_ context.Context, req *restroute.Request) (any, int, error) {
	token, m, err := pageParams(req)
	if err != nil {
		return nil, 0, err
	}

	list, next, err := h.Backend.ListAutoshifts(req.Query.Get("status"), token, m)

	return items(list, next), 0, err
}

func (h *Handler) getObserver(_ context.Context, _ *restroute.Request) (any, int, error) {
	return map[string]string{"status": h.Backend.ObserverNotificationStatus()}, 0, nil
}

func (h *Handler) updateObserver(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ Status string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	s, err := h.Backend.SetObserverNotificationStatus(in.Status)

	return map[string]string{"status": s}, 0, err
}
