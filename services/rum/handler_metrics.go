package rum

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (h *Handler) metricOps(ops map[string]restroute.OpFunc) {
	ops["PutRumMetricsDestination"] = h.putDestination
	ops["DeleteRumMetricsDestination"] = h.deleteDestination
	ops["ListRumMetricsDestinations"] = h.listDestinations
	ops["BatchCreateRumMetricDefinitions"] = h.batchCreate
	ops["BatchDeleteRumMetricDefinitions"] = h.batchDelete
	ops["BatchGetRumMetricDefinitions"] = h.batchGet
	ops["UpdateRumMetricDefinition"] = h.updateDefinition
}

func (h *Handler) putDestination(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		Destination    string `json:"Destination"`
		DestinationARN string `json:"DestinationArn"`
		IAMRoleARN     string `json:"IamRoleArn"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	return nil, 0, h.Backend.PutRumMetricsDestination(
		ctx, req.Params[paramAppMonitorName], in.Destination, in.DestinationARN, in.IAMRoleARN)
}

func (h *Handler) deleteDestination(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.DeleteRumMetricsDestination(
		ctx, req.Params[paramAppMonitorName], req.Query.Get("destination"), req.Query.Get("destinationArn"))
}

func (h *Handler) listDestinations(ctx context.Context, req *restroute.Request) (any, int, error) {
	maxResults, err := restroute.MaxResultsQuery(req.Query, "maxResults")
	if err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListRumMetricsDestinations(
		ctx, req.Params[paramAppMonitorName], req.Query.Get("nextToken"), maxResults)
	if err != nil {
		return nil, 0, err
	}

	return pageOut("Destinations", pg.Data, pg.Next), 0, nil
}

func (h *Handler) batchCreate(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		Destination       string              `json:"Destination"`
		DestinationARN    string              `json:"DestinationArn"`
		MetricDefinitions []WireMetricRequest `json:"MetricDefinitions"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	defs, errs, err := h.Backend.BatchCreateRumMetricDefinitions(
		ctx, req.Params[paramAppMonitorName], in.Destination, in.DestinationARN, in.MetricDefinitions)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{"MetricDefinitions": defs, "Errors": errs}, 0, nil
}

func (h *Handler) batchDelete(ctx context.Context, req *restroute.Request) (any, int, error) {
	ids, errs, err := h.Backend.BatchDeleteRumMetricDefinitions(
		ctx, req.Params[paramAppMonitorName], req.Query.Get("destination"), req.Query.Get("destinationArn"),
		req.Query["metricDefinitionIds"])
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{"MetricDefinitionIds": ids, "Errors": errs}, 0, nil
}

func (h *Handler) batchGet(ctx context.Context, req *restroute.Request) (any, int, error) {
	maxResults, err := restroute.MaxResultsQuery(req.Query, "maxResults")
	if err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.BatchGetRumMetricDefinitions(
		ctx, req.Params[paramAppMonitorName], req.Query.Get("destination"), req.Query.Get("destinationArn"),
		req.Query.Get("nextToken"), maxResults)
	if err != nil {
		return nil, 0, err
	}

	return pageOut("MetricDefinitions", pg.Data, pg.Next), 0, nil
}

func (h *Handler) updateDefinition(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		MetricDefinition   WireMetricRequest `json:"MetricDefinition"`
		Destination        string            `json:"Destination"`
		DestinationARN     string            `json:"DestinationArn"`
		MetricDefinitionID string            `json:"MetricDefinitionId"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	return nil, 0, h.Backend.UpdateRumMetricDefinition(
		ctx,
		req.Params[paramAppMonitorName],
		in.Destination,
		in.DestinationARN,
		in.MetricDefinitionID,
		in.MetricDefinition,
	)
}
