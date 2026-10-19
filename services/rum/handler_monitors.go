package rum

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (h *Handler) monitorOps(ops map[string]restroute.OpFunc) {
	ops["CreateAppMonitor"] = h.createAppMonitor
	ops["GetAppMonitor"] = h.getAppMonitor
	ops["UpdateAppMonitor"] = h.updateAppMonitor
	ops["DeleteAppMonitor"] = h.deleteAppMonitor
	ops["ListAppMonitors"] = h.listAppMonitors
}

type sourceMapsDoc = wireSourceMaps

func (h *Handler) createAppMonitor(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		AppMonitorConfiguration    *MonitorConfig    `json:"AppMonitorConfiguration"`
		CustomEvents               *wireCustomEvents `json:"CustomEvents"`
		CwLogEnabled               *bool             `json:"CwLogEnabled"`
		DeobfuscationConfiguration *sourceMapsDoc    `json:"DeobfuscationConfiguration"`
		Tags                       map[string]string `json:"Tags"`
		Domain                     string            `json:"Domain"`
		Name                       string            `json:"Name"`
		Platform                   string            `json:"Platform"`
		DomainList                 []string          `json:"DomainList"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	id, err := h.Backend.CreateAppMonitor(ctx, CreateMonitorInput{
		Config: in.AppMonitorConfiguration, CustomEvents: in.CustomEvents, SourceMaps: in.DeobfuscationConfiguration,
		Tags: in.Tags, CwLogEnabled: in.CwLogEnabled, Name: in.Name, Platform: in.Platform, Domain: in.Domain,
		DomainList: in.DomainList,
	})
	if err != nil {
		return nil, 0, err
	}

	return map[string]string{paramID: id}, 0, nil
}

func (h *Handler) getAppMonitor(ctx context.Context, req *restroute.Request) (any, int, error) {
	m, err := h.Backend.getAppMonitor(ctx, req.Params[paramName])
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{"AppMonitor": m}, 0, nil
}

func (h *Handler) updateAppMonitor(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		AppMonitorConfiguration    *MonitorConfig    `json:"AppMonitorConfiguration"`
		CustomEvents               *wireCustomEvents `json:"CustomEvents"`
		CwLogEnabled               *bool             `json:"CwLogEnabled"`
		DeobfuscationConfiguration *sourceMapsDoc    `json:"DeobfuscationConfiguration"`
		Domain                     string            `json:"Domain"`
		DomainList                 []string          `json:"DomainList"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	err := h.Backend.UpdateAppMonitor(ctx, UpdateMonitorInput{
		Config: in.AppMonitorConfiguration, CustomEvents: in.CustomEvents, SourceMaps: in.DeobfuscationConfiguration,
		CwLogEnabled: in.CwLogEnabled, Name: req.Params[paramName], Domain: in.Domain, DomainList: in.DomainList,
	})

	return nil, 0, err
}

func (h *Handler) deleteAppMonitor(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.DeleteAppMonitor(ctx, req.Params[paramName])
}

func (h *Handler) listAppMonitors(ctx context.Context, req *restroute.Request) (any, int, error) {
	maxResults, err := restroute.MaxResultsQuery(req.Query, "maxResults")
	if err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListAppMonitors(ctx, req.Query.Get("nextToken"), maxResults)
	if err != nil {
		return nil, 0, err
	}

	return pageOut("AppMonitorSummaries", pg.Data, pg.Next), 0, nil
}

func pageOut[T any](key string, items []T, next string) map[string]any {
	out := map[string]any{key: items}
	if next != "" {
		out["NextToken"] = next
	}

	return out
}
