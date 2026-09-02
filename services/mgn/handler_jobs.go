package mgn

import (
	"context"
	"net/http"
)

//nolint:dupl // structurally parallel to handleListApplications; both decode Filters, list, paginate
func (h *Handler) handleDescribeJobs(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req describeJobsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	f := DescribeJobsFilters{}
	if req.Filters != nil {
		f = DescribeJobsFilters{
			JobIDs:   req.Filters.JobIDs,
			FromDate: req.Filters.FromDate,
			ToDate:   req.Filters.ToDate,
		}
	}

	pg, err := h.Backend.DescribeJobs(f, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	items := make([]jobWire, len(pg.Data))
	for i, j := range pg.Data {
		items[i] = toJobWire(j)
	}

	return marshalResponse(describeJobsResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleDescribeJobLogItems(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req describeJobLogItemsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if req.JobID == "" {
		return nil, validationError("jobID is required")
	}

	pg, err := h.Backend.DescribeJobLogItems(req.JobID, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	items := make([]jobLogWire, len(pg.Data))
	for i, l := range pg.Data {
		items[i] = toJobLogWire(l)
	}

	return marshalResponse(describeJobLogItemsResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleDeleteJob(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req jobIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteJob(req.JobID); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}
