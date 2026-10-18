package rum

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (h *Handler) dataOps(ops map[string]restroute.OpFunc) {
	ops["PutRumEvents"] = h.putRumEvents
	ops["GetAppMonitorData"] = h.getAppMonitorData
}

func (h *Handler) putRumEvents(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in PutEventsInput
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	in.AppID = req.Params[paramID]

	return nil, 0, h.Backend.PutRumEvents(ctx, &in)
}

func (h *Handler) getAppMonitorData(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		TimeRange  *TimeRange    `json:"TimeRange"`
		NextToken  string        `json:"NextToken"`
		Filters    []QueryFilter `json:"Filters"`
		MaxResults int           `json:"MaxResults"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.GetAppMonitorData(
		ctx,
		req.Params[paramName],
		in.TimeRange,
		in.Filters,
		in.NextToken,
		in.MaxResults,
	)
	if err != nil {
		return nil, 0, err
	}

	return pageOut("Events", pg.Data, pg.Next), 0, nil
}
