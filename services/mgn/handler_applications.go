package mgn

import (
	"context"
	"net/http"
)

func (h *Handler) handleCreateApplication(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createApplicationRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	app, err := h.Backend.CreateApplication(req.Name, req.Description, req.Tags)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toApplicationWire(app))
}

func (h *Handler) handleUpdateApplication(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req updateApplicationRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	app, err := h.Backend.UpdateApplication(req.ApplicationID, req.Name, req.Description)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toApplicationWire(app))
}

func (h *Handler) handleDeleteApplication(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req applicationIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteApplication(req.ApplicationID); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

//nolint:dupl // structurally parallel to handleDescribeJobs; both decode Filters, list, paginate
func (h *Handler) handleListApplications(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req listApplicationsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	f := ListApplicationsFilters{}
	if req.Filters != nil {
		f = ListApplicationsFilters{
			ApplicationIDs: req.Filters.ApplicationIDs,
			WaveIDs:        req.Filters.WaveIDs,
			IsArchived:     req.Filters.IsArchived,
		}
	}

	pg, err := h.Backend.ListApplications(f, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	items := make([]applicationWire, len(pg.Data))
	for i, a := range pg.Data {
		items[i] = toApplicationWire(a)
	}

	return marshalResponse(listApplicationsResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleArchiveApplication(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req applicationIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	app, err := h.Backend.ArchiveApplication(req.ApplicationID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toApplicationWire(app))
}

func (h *Handler) handleUnarchiveApplication(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req applicationIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	app, err := h.Backend.UnarchiveApplication(req.ApplicationID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toApplicationWire(app))
}

func (h *Handler) handleAssociateSourceServers(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req associateSourceServersRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.AssociateSourceServers(req.ApplicationID, req.SourceServerIDs); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}

func (h *Handler) handleDisassociateSourceServers(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req associateSourceServersRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DisassociateSourceServers(req.ApplicationID, req.SourceServerIDs); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}
